/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package io.confluent.kafkarest;

import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.rest.exceptions.RestException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConsumerSubscriptionReadTask<KafkaK, KafkaV, ClientK, ClientV> {
  private static final Logger log = LoggerFactory.getLogger(ConsumerSubscriptionReadTask.class);

  final private ConsumerState parent;
  private final long maxResponseBytes;
  private ConsumerTopicState topicState;
  final private ConsumerWorkerSubscriptionReadCallback<ClientK, ClientV> callback;
  final private ScheduledExecutorService executor;
  private List<ConsumerRecord<ClientK, ClientV>> failedRecords;
  private List<ConsumerRecord<ClientK, ClientV>> bufferedRecords;
  final private long interval;
  final private long timeout;
  private long bytesConsumed = 0;
  private long nextinterval;
  
  private ReadTask task;

  public ConsumerSubscriptionReadTask(ConsumerState parent, String topic, long maxBytes,
                                      ConsumerWorkerSubscriptionReadCallback callback,
                                      ScheduledExecutorService executor, int interval, long timeout) {
    this.parent = parent;
    this.maxResponseBytes = Math.min(
            maxBytes,
            parent.getConfig().getLong(KafkaRestConfig.CONSUMER_REQUEST_MAX_BYTES_CONFIG));
    this.callback = callback;
    this.executor = executor;
    this.interval = interval;
    this.nextinterval = interval;
    this.timeout = timeout;
    this.bufferedRecords = new ArrayList<ConsumerRecord<ClientK, ClientV>>();
    try {
      topicState = parent.getOrCreateTopicState(topic);
      ConsumerSubscriptionReadTask previousTask = topicState.clearSubscriptionTask();
      if (previousTask != null) {
        previousTask.terminate();
        failedRecords = previousTask.clearFailedRecords();
      }
      topicState.setSubscriptionTask(this);
    } catch (RestException e) {
      // can't subscribe
      log.error("Failed to subscribe topic" + e);
    }
  }

  List<ConsumerRecord<ClientK, ClientV>> clearFailedRecords() {
    List<ConsumerRecord<ClientK, ClientV>> r = failedRecords;
    failedRecords = null;
    return r;
  }

  public void schedule() {
    task = new ReadTask(topicState.getIterator());
    task.schedule(0);
  }
    
  public void terminate() {
    task.terminate();
  }

  private class ReadTask implements Runnable {
    private ConsumerIterator<KafkaK, KafkaV> iter;

    private boolean terminated;
    
    public ReadTask(ConsumerIterator<KafkaK, KafkaV> iter) {
      this.iter = iter;
    }
    @Override
    public void run() {
      try {
        List<ConsumerRecord<ClientK, ClientV>> rs = clearFailedRecords();
        if (rs != null) {
          try {
            callback.onRecords(rs);
          } catch (IOException e) {
            failedRecords = rs;
            log.error("Failed to re-broadcast", e);
          }
        } else {
          while(!terminated && iter.hasNext()) {
            try {
              MessageAndMetadata<KafkaK, KafkaV> msg = iter.next();
              ConsumerRecordAndSize<ClientK, ClientV> recordAndSize = parent.createConsumerRecord(msg);
              bufferedRecords.add(recordAndSize.getRecord());
              bytesConsumed += recordAndSize.getSize();
              // flush the buffered records if the size exceeds the limit
              if (bytesConsumed >= maxResponseBytes) {
                try {
                  callback.onRecords(bufferedRecords);
                  bufferedRecords.clear();
                  bytesConsumed = 0;
                } catch (IOException e) {
                  failedRecords = rs;
                  log.error("Failed to broadcast", e);
                  break;
                }
              }
            } catch (RestException e) {
              callback.onError(e);
            }
          }
        }
      } finally {
        if (!bufferedRecords.isEmpty()) {
          try {
            callback.onRecords(bufferedRecords);
            bufferedRecords.clear();
            bytesConsumed = 0;
          } catch (IOException e) {
            failedRecords = bufferedRecords;
            log.error("Failed to broadcast", e);
          }
        }

        if (!terminated) {
          if (failedRecords == null) {
            nextinterval = interval;
          } else if (nextinterval < Long.MAX_VALUE) {
            nextinterval <<= 1;
            if (nextinterval < 0) {
              nextinterval = Long.MAX_VALUE;
            }
          }
          //TODO only increase the interval up to the consumer timeout value
          callback.onCompletion();
          schedule(nextinterval);
        }
      }
    }

    void schedule(long delay) {
      executor.schedule(this, delay, TimeUnit.MILLISECONDS);
    }
    void terminate() {
      terminated = true;
    }
  }
}
