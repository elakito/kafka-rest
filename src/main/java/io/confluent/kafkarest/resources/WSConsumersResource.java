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
package io.confluent.kafkarest.resources;

import java.io.IOException;
import java.util.List;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.confluent.kafkarest.AvroConsumerState;
import io.confluent.kafkarest.BinaryConsumerState;
import io.confluent.kafkarest.ConsumerManager;
import io.confluent.kafkarest.ConsumerState;
import io.confluent.kafkarest.Context;
import io.confluent.kafkarest.JsonConsumerState;
import io.confluent.kafkarest.Versions;
import io.confluent.kafkarest.entities.ConsumerRecord;
import io.confluent.rest.annotations.PerformanceMetric;

@Path("/ws/consumers")
// We include embedded formats here so you can always use these headers when interacting with
// a consumers resource. The few cases where it isn't safe are overridden per-method
@Produces({Versions.KAFKA_V1_JSON_BINARY_WEIGHTED_LOW, Versions.KAFKA_V1_JSON_AVRO_WEIGHTED_LOW,
           Versions.KAFKA_V1_JSON_JSON_WEIGHTED_LOW, Versions.KAFKA_V1_JSON_WEIGHTED,
           Versions.KAFKA_DEFAULT_JSON_WEIGHTED, Versions.JSON_WEIGHTED})
@Consumes({Versions.KAFKA_V1_JSON_BINARY, Versions.KAFKA_V1_JSON_AVRO, Versions.KAFKA_V1_JSON_JSON,
           Versions.KAFKA_V1_JSON, Versions.KAFKA_DEFAULT_JSON, Versions.JSON,
           Versions.GENERIC_REQUEST})
public class WSConsumersResource {

  private final Context ctx;
  private final ObjectMapper mapper = new ObjectMapper();

  public WSConsumersResource(Context ctx) {
    this.ctx = ctx;
  }

  @DELETE
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @PerformanceMetric("ws.consumer.unsubscribe")
  public void unsubscribeTopic(final @PathParam("group") String group,
                                      final @PathParam("instance") String instance,
                                      final @PathParam("topic") String topic) {
    ctx.getConsumerManager().deleteSubscription(group, instance, topic);
  }

  @GET
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @PerformanceMetric("ws.consumer.topic.read-binary")
  @Produces({Versions.KAFKA_V1_JSON_BINARY_WEIGHTED,
             Versions.KAFKA_V1_JSON_WEIGHTED,
             Versions.KAFKA_DEFAULT_JSON_WEIGHTED,
             Versions.JSON_WEIGHTED})
  public void subscribeTopicBinary(final @javax.ws.rs.core.Context HttpServletResponse httpResponse,
                              final @PathParam("group") String group,
                              final @PathParam("instance") String instance,
                              final @PathParam("topic") String topic,
                              @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes) {
    subscribeTopic(httpResponse, group, instance, topic, maxBytes, BinaryConsumerState.class);
    subscribeTopic(httpResponse, group, instance, topic, BinaryConsumerState.class);
  }

  @GET
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @PerformanceMetric("ws.consumer.topic.read-json")
  @Produces({Versions.KAFKA_V1_JSON_JSON_WEIGHTED_LOW}) // Using low weight ensures binary is default
  public void subscribeTopicJson(final @javax.ws.rs.core.Context HttpServletResponse httpResponse,
                            final @PathParam("group") String group,
                            final @PathParam("instance") String instance,
                            final @PathParam("topic") String topic,
                            @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes) {
    subscribeTopic(httpResponse, group, instance, topic, maxBytes, JsonConsumerState.class);
    subscribeTopic(httpResponse, group, instance, topic, JsonConsumerState.class);
  }

  @GET
  @Path("/{group}/instances/{instance}/topics/{topic}")
  @PerformanceMetric("ws.consumer.topic.read-avro")
  @Produces({Versions.KAFKA_V1_JSON_AVRO_WEIGHTED_LOW}) // Using low weight ensures binary is default
  public void subscribeTopicAvro(final @javax.ws.rs.core.Context HttpServletResponse httpResponse,
                            final @PathParam("group") String group,
                            final @PathParam("instance") String instance,
                            final @PathParam("topic") String topic,
                            @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes) {
    subscribeTopic(httpResponse, group, instance, topic, maxBytes, AvroConsumerState.class);
    subscribeTopic(httpResponse, group, instance, topic, AvroConsumerState.class);
  }

  private <KafkaK, KafkaV, ClientK, ClientV> void subscribeTopic(
      final @javax.ws.rs.core.Context HttpServletResponse httpResponse,
      final @PathParam("group") String group,
      final @PathParam("instance") String instance,
      final @PathParam("topic") String topic,
      @QueryParam("max_bytes") @DefaultValue("-1") long maxBytes,
      Class<? extends ConsumerState<KafkaK, KafkaV, ClientK, ClientV>> consumerStateType) {
    maxBytes = (maxBytes <= 0) ? Long.MAX_VALUE : maxBytes;
    ctx.getConsumerManager().readTopic(
        group, instance, topic, consumerStateType, maxBytes,
        new ConsumerManager.ReadCallback<ClientK, ClientV>() {
          @Override
          public void onCompletion(List<? extends ConsumerRecord<ClientK, ClientV>> records,
                                   Exception e) {
            if (e != null) {
              try {
                writeValue(httpResponse, e);
              } catch (IOException e2) {
                // ignore?
              }
            } else {
              try {
                writeValue(httpResponse, records);
              } catch (IOException e2) {
                // TODO should the records to be placed back in the basket to be pushed later?
              }
            }
          }
        });
  }

  private <KafkaK, KafkaV, ClientK, ClientV> void subscribeTopic(
      final HttpServletResponse httpResponse,
      final String group,
      final String instance,
      final String topic, 
      Class<? extends ConsumerState<KafkaK, KafkaV, ClientK, ClientV>> consumerStateType) {
    ctx.getConsumerManager().subscribeTopic(group, instance, topic, consumerStateType,
      new ConsumerManager.SubscriptionReadCallback<ClientK, ClientV>() {

        @Override
        public void onRecord(ConsumerRecord<ClientK, ClientV> record) throws IOException {
          writeValue(httpResponse, record);
        }

        @Override
        public void onError(Exception e) {
          //REVISIT the error message format to be specified
          try {
            writeValue(httpResponse, e.toString());
          } catch (IOException e2) {
            // ignore?
          }
        }
      });
    
  }

  private void writeValue(HttpServletResponse httpResponse, Object value) throws IOException {
    mapper.writeValue(httpResponse.getOutputStream(), value);
  }
}
