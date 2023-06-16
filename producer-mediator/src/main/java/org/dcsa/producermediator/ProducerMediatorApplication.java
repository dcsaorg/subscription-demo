package org.dcsa.producermediator;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.spring.messaging.CloudEventMessageConverter;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.function.cloudevent.CloudEventMessageBuilder;
import org.springframework.cloud.function.cloudevent.CloudEventMessageUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.client.RestTemplate;

// https://github.com/spring-cloud/spring-cloud-function/blob/main/spring-cloud-function-samples/function-sample-cloudevent-sdk/src/main/java/io/spring/cloudevent/DemoApplication.java
// https://github.com/spring-cloud/spring-cloud-function/blob/main/spring-cloud-function-samples/function-sample-cloudevent-stream/src/main/java/io/spring/cloudevent/DemoApplication.java
// https://github.com/spring-cloud/spring-cloud-function/blob/main/spring-cloud-function-samples/function-sample-cloudevent-stream/src/test/java/io/spring/cloudevent/DemoApplicationTests.java

@SpringBootApplication
public class ProducerMediatorApplication {

  // add logger to this class
  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerMediatorApplication.class);

  public static void main(String[] args) {
    SpringApplication.run(ProducerMediatorApplication.class, args);
  }

  private RestTemplate restTemplate = new RestTemplate();

  @Configuration
  public static class CloudEventMessageConverterConfiguration {
    @Bean
    public CloudEventMessageConverter cloudEventMessageConverter() {
      return new CloudEventMessageConverter();
    }
  }

  record Person(String name) {}

  record Employee(Person person) {}

  /** */
  record EventTO(
          String id,
          String eventClassifierCode,
          String equipmentEventTypeCode,
          String emptyIndicatorCode) {}

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
    };
  }

  @Bean
  public Supplier<Message<byte[]>> source() {
    return () -> {
      var event = new EventTO(UUID.randomUUID().toString(), "ACT", "LOAD", "LADEN");

      try {

        // Message<byte[]
        return CloudEventMessageBuilder.withData(new ObjectMapper().writeValueAsBytes(event))
                .setId(event.id.toString())
                .setSource(URI.create("http://member.dcsa.org"))
                .setType("org.dcsa.v1.event")
                .setTime(OffsetDateTime.now())
                .setHeader("callbackurl", "http://127.0.0.1:8081/events")
                .setHeader("subscriptionid", UUID.randomUUID().toString())
                .setHeader("secret", "secret")
                .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
                .build(CloudEventMessageUtils.AMQP_ATTR_PREFIX);

      } catch (Exception e) {
        return null;
      }
    };
  }

  @Bean
  public Consumer<Message<String>> sink() throws Exception {
    return message -> {
      LOGGER.info("Received message " + message);

      // CloudEvent cloudEvent = CloudEventBuilder.v1()
      // 	.withId(message.getHeaders().get("ce-id").toString())
      // 	.withSource(URI.create(message.getHeaders().get("ce-source").toString()))
      // 	.withType(message.getHeaders().get("ce-type").toString())
      // 	.withDataContentType("application/json")
      // 	.withData(message.getPayload().getBytes())
      // 	.build();

      HttpHeaders headers = new HttpHeaders();
      // Set any necessary headers for the REST API request
      headers.set("Content-Type", "application/json");
      headers.set("ce-specversion", "1.0");
      headers.set("ce-type", message.getHeaders().get("ce-type").toString());
      headers.set("ce-source", message.getHeaders().get("ce-source").toString());
      headers.set("ce-datacontenttype", "application/json");
      headers.set("ce-id", message.getHeaders().get("ce-id").toString());

      // try catch
      try {

        var event = new ObjectMapper().readValue(message.getPayload(), EventTO.class);

        Message<EventTO> msg = MessageBuilder.withPayload(event).build();

        HttpEntity<Message<?>> httpEntity = new HttpEntity<>(msg, headers);

        ResponseEntity<String> ce =
                restTemplate.exchange(
                        message.getHeaders().get("callbackurl").toString(),
                        HttpMethod.POST,
                        httpEntity,
                        String.class);

        LOGGER.info("Received response " + ce.getStatusCode());

      } catch (Exception e) {
        LOGGER.error("Error parsing event", e);
      }
    };
  }
}
