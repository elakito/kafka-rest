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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerIterator;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConsumerSubscriptionReadTask<KafkaK, KafkaV, ClientK, ClientV> {
  final private ConsumerState parent;
  private ConsumerTopicState topicState;
  final private ConsumerWorkerSubscriptionReadCallback<ClientK, ClientV> callback;
  final private ScheduledExecutorService executor;
  private ConsumerRecord<ClientK, ClientV> failedRecord;
  final private long interval;
  final private long timeout;

  private ReadTask task;

  public ConsumerSubscriptionReadTask(ConsumerState parent, String topic, 
                                      ConsumerWorkerSubscriptionReadCallback callback,
                                      ScheduledExecutorService executor, int interval, long timeout) {
    this.parent = parent;
    this.callback = callback;
    this.executor = executor;
    this.interval = interval;
    this.timeout = timeout;
    try {
      topicState = parent.getOrCreateTopicState(topic);
      ConsumerSubscriptionReadTask previousTask = topicState.clearSubscriptionTask();
      if (previousTask != null) {
        previousTask.terminate();
        failedRecord = previousTask.clearFailedRecord();
      }
      topicState.setSubscriptionTask(this);
    } catch (RestException e) {
      // can't subscribe
    }
  }

  ConsumerRecord<ClientK, ClientV> clearFailedRecord() {
    ConsumerRecord<ClientK, ClientV> r = failedRecord;
    failedRecord = null;
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
        // TODO aggregate records up to some limit as in the normal topic reader
        while(!terminated && hasNextRecord()) {
          try {
            ConsumerRecord<ClientK, ClientV> r = getNextRecord();
            try {
              callback.onRecord(r);
            } catch (IOException e) {
              failedRecord = r;
            }          
          } catch (RestException e) {
            callback.onError(e);
          }
        }
      } finally {
        if (!terminated) {
          schedule(interval);
        }
      }
    }
    private boolean hasNextRecord() {
      return failedRecord != null || iter.hasNext();
    }

    private ConsumerRecord<ClientK, ClientV> getNextRecord() {
      ConsumerRecord<ClientK, ClientV> r = clearFailedRecord();
      if (r == null) {
        MessageAndMetadata<KafkaK, KafkaV> msg = iter.next();
        ConsumerRecordAndSize<ClientK, ClientV> recordAndSize = parent.createConsumerRecord(msg);
        r = recordAndSize.getRecord();
      }
      return r;
    }
    void schedule(long delay) {
      executor.schedule(this, delay, TimeUnit.MILLISECONDS);
    }
    void terminate() {
      terminated = true;
    }
  }
}
