package org.dcsa.consumermediator;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.kafka.CloudEventDeserializer;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.Random;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.function.cloudevent.CloudEventMessageBuilder;
import org.springframework.cloud.function.cloudevent.CloudEventMessageUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.util.MimeTypeUtils;

// https://dzone.com/articles/cloud-events-kafka-binding-with-spring
// https://cloudevents.github.io/sdk-java/kafka.html
@SpringBootApplication
public class ConsumerMediatorApplication {

  Logger LOGGER = LoggerFactory.getLogger(ConsumerMediatorApplication.class);

  private final KafkaTemplate<String, EventTO> kafkaTemplate;

  public ConsumerMediatorApplication(KafkaTemplate<String, EventTO> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  record EventTO(
          String id,
          String eventClassifierCode,
          String equipmentEventTypeCode,
          String emptyIndicatorCode) {}

  //	private final RestTemplate restTemplate;

  public static void main(String[] args) {
    SpringApplication.run(ConsumerMediatorApplication.class, args);
  }

  // create map of string with values
  //	Map<String, String> routeByEvent = Collections.unmodifiableMap(new HashMap<String, String>() {
  //		{
  //			put("key1", "value1");
  //			put("key2", "value2");
  //		}
  //	});

  @Bean
  public Consumer<CloudEvent> events() {
    return event -> {
      LOGGER.info("Producer-Mediator : Received event on /events: {}", event);

      //      return CloudEventBuilder.from(event)
      //          .withId(UUID.randomUUID().toString())
      //          .withSource(URI.create("https://spring.io/foos"))
      //          .withType("io.spring.event.Foo")
      //          .withData(event.getData().toBytes())
      //          .build();
      try {
        Message<byte[]> message =
                CloudEventMessageBuilder.withData(new ObjectMapper().writeValueAsBytes(event))
                        .setId(event.getId())
                        .setSource(event.getId())
                        .setType(event.getType())
                        .setTime(event.getTime())
                        .setHeader("callbackurl", event.getAttribute("callbackurl"))
                        .setHeader("subscriptionid", event.getAttribute("subscriptionid"))
                        .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
                        .build(CloudEventMessageUtils.KAFKA_ATTR_PREFIX);
        kafkaTemplate.setDefaultTopic("events");
        kafkaTemplate.send(message);
      } catch (Exception e) {
        LOGGER.error("Error while creating message", e);
      }
    };
  }

  //  @Bean
  //  public Supplier<Message<byte[]>> source() {
  //    return () -> {
  //      var event = new EventTO(UUID.randomUUID().toString(), "ACT", "LOAD", "LADEN");
  //
  ////      Message<EventTO> message = MessageBuilder.withPayload(event).build();
  //
  //      try{
  //      Message<byte[]> message =
  //              CloudEventMessageBuilder.withData(new ObjectMapper().writeValueAsBytes(event))
  //                      .setId(event.id.toString())
  //                      .setSource(URI.create("http://member.dcsa.org"))
  //                      .setType("org.dcsa.v1.event")
  //                      .setTime(OffsetDateTime.now())
  //                      .setHeader("callbackurl", "http://127.0.0.1:8081/events")
  //                      .setHeader("subscriptionid", UUID.randomUUID().toString())
  //                      .setHeader("secret", "secret")
  //                      .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
  //                      .build(CloudEventMessageUtils.KAFKA_ATTR_PREFIX);
  //        return message;
  //      } catch (Exception e) {
  //        LOGGER.error("Error while creating message", e);
  //      }
  //
  //      return null;
  //
  //    };
  //  }

  @Bean
  public Supplier<CloudEvent> source() {
    return () -> {
      var event = new EventTO(UUID.randomUUID().toString(), "ACT", "LOAD", "LADEN");

      //      Message<EventTO> message = MessageBuilder.withPayload(event).build();

      // counter increment and logic for odd or even

      CloudEventDeserializer.class.getClassLoader().getResourceAsStream("application.properties");
      try {

        if (Math.random() < 0.5) {
          return CloudEventBuilder.v1()
                  .withId(event.id.toString())
                  .withSource(URI.create("http://member.dcsa.org"))
                  .withType("org.dcsa.v2.event")
                  .withTime(OffsetDateTime.now())
                  .withData(new ObjectMapper().writeValueAsBytes(event))
                  .withExtension("callbackurl", "http://127.0.0.1:8081/events")
                  .withExtension("subscriptionid", UUID.randomUUID().toString())
                  .withExtension("secret", "secret")
                  .withDataContentType("application/json")
                  .build();
        } else {
          return CloudEventBuilder.v1()
                  .withId(event.id.toString())
                  .withSource(URI.create("http://member.dcsa.org"))
                  .withType("org.dcsa.v1.event")
                  .withTime(OffsetDateTime.now())
                  .withData(new ObjectMapper().writeValueAsBytes(event))
                  .withExtension("callbackurl", "http://127.0.0.1:8081/events")
                  .withExtension("subscriptionid", UUID.randomUUID().toString())
                  .withExtension("secret", "secret")
                  .withDataContentType("application/json")
                  .build();
        }

      } catch (Exception e) {
        LOGGER.error("Error while creating message", e);
      }

      return null;
    };
  }

  @Bean
  Consumer<CloudEvent> consumer() {
    return message -> {
      LOGGER.info("Received Message: " + message.getType());
      LOGGER.info("Received Message: " + message.getExtension("callbackurl"));
      LOGGER.info("Received Message: " + message);
    };
  }
  //  @KafkaListener(topics = "events", groupId = "consumer-mediator")
  //  public void consumer(
  //      @Payload Message<EventTO> message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition)
  // {
  //    System.out.println("Received Message: " + message.getPayload() + "from partition: " +
  // partition);
  //  }
}
