package org.dcsa.producercore;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.cloudevents.spring.mvc.CloudEventHttpMessageConverter;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitMessagingTemplate;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Service;
import org.springframework.util.MimeTypeUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

// https://github.com/spring-cloud/spring-cloud-function/tree/main/spring-cloud-function-context/src/main/java/org/springframework/cloud/function/cloudevent

@SpringBootApplication
public class ProducerCoreApplication {

  // logger
  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerCoreApplication.class);

  public static void main(String[] args) {
    SpringApplication.run(ProducerCoreApplication.class, args);
  }

  @Configuration
  public static class CloudEventHandlerConfiguration implements WebMvcConfigurer {

    // @Bean
    // public CloudEventHttpMessageConverter cloudEventHttpMessageConverter() {
    //   return new CloudEventHttpMessageConverter();
    // }

    @Override
    public void configureMessageConverters(List<HttpMessageConverter<?>> converters) {
      converters.add(0, new CloudEventHttpMessageConverter());
    }

    @Bean
    public RabbitTemplate rabbitTemplate(
        ConnectionFactory connectionFactory, ObjectMapper objectMapper) {
      RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
      rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter(objectMapper));
      return rabbitTemplate;
    }
  }

  // a simple java data structure to store the subscription data via id
  public static final Map<UUID, SubscriptionTO> subscriptions = new HashMap<>();

  public record SubscribeOn(String topic, String value) {}

  record SubscriptionTO(
      // read only json annotiation
      @JsonProperty(access = JsonProperty.Access.READ_ONLY) String subscriptionId,
      String callbackUrl,
      SubscribeOn subscribeOn,
      String secret) {}

  // the callback and secret would be stored in the subscription data linked to a user
  // since there is no persistence in the demo, we will send it across in the event
  record EventTO(
      String id,
      String eventClassifierCode,
      String equipmentEventTypeCode,
      String emptyIndicatorCode) {}

  @RestController
  @RequestMapping("/api/v1")
  public static class CoreController {

    private final CoreService coreService;

    public CoreController(CoreService coreService) {
      this.coreService = coreService;
    }

    // create get events endpoint
    @GetMapping(value = "/events", produces = MimeTypeUtils.APPLICATION_JSON_VALUE)
    public String getEvents() {
      return """
    {
      "eventClassifierCode": "EST",
      "eventDateTime": "2019-11-12T07:41:00+08:30",
      "equipmentEventTypeCode": "LOAD",
      "equipmentReference": "APZU4812090",
      "ISOEquipmentCode": "22GP",
      "emptyIndicatorCode": "LADEN",
      "isTransshipmentMove": true,
      "transportCall": {
        "transportCallReference": "987e4567",
        "modeOfTransport": "VESSEL",
        "location": {
          "locationType": "UNLO",
          "locationName": "Port of Amsterdam",
          "UNLocationCode": "NLRAM"
        },
        "portVisitReference": "NLRTM1234589",
        "carrierServiceCode": "FE1",
        "universalServiceReference": "SR12345A",
        "carrierExportVoyageNumber": "2103S",
        "universalExportVoyageReference": "2103N",
        "carrierImportVoyageNumber": "2103N",
        "universalImportVoyageReference": "2103N",
        "transportCallSequenceNumber": 2,
        "facilityTypeCode": "POTE",
        "vessel": {
          "vesselIMONumber": "9321483",
          "name": "King of the Seas",
          "flag": "NL",
          "callSign": "NCVV",
          "operatorCarrierCode": "MAEU",
          "operatorCarrierCodeListProvider": "NMFTA"
        }
      },
      "relatedDocumentReferences": [
        {
          "type": "BKG",
          "value": "ABC123059"
        },
        {
          "type": "TRD",
          "value": "85943567"
        }
      ],
      "references": [
        {
          "type": "EQ",
          "value": "APZU4812090"
        }
      ],
      "seals": [
        {
          "number": "133534",
          "source": "CUS",
          "type": "WIR"
        }
      ]
    }
""";
    }

    // endpoint to create a subscription
    @PostMapping("/event-subscriptions")
    public SubscriptionTO createSubscription(@RequestBody SubscriptionTO subscription) {
      // create a new subscription id
      UUID subscriptionId = UUID.randomUUID();
      // store the subscription data in the map
      subscriptions.put(subscriptionId, subscription);

      LOGGER.info("Created subscription with id: {}", subscriptionId);

      // return the subscription data with the id
      return new SubscriptionTO(
          subscriptionId.toString(),
          subscription.callbackUrl(),
          subscription.subscribeOn(),
          subscription.secret());
    }

    // curl request to trigger an events endpoint
    // http://localhost:8080/api/v1/events
    // endpoint to trigger an event
    @PostMapping("/trigger-event")
    public void triggerEvent() throws Exception {
      // public void triggerEvent(@RequestBody CloudEvent event) throws Exception{

      LOGGER.info("Event is being trigered.");

      // create a new cloud event
      //      CloudEvent cloudEvent =
      //          CloudEventBuilder.v1()
      //              .withId(UUID.randomUUID().toString())
      //              .withSource(URI.create("https://dcsa.org"))
      //              .withType("org.dcsa.v1.event")
      //              .withTime(OffsetDateTime.now())
      //              .withData("application/json", objectMapper.writeValueAsBytes(event))
      //              .build();

      coreService.triggerEvent();
      // return CloudEventBuilder.v1()
      //         .withId(UUID.randomUUID().toString())
      //         .withSource(URI.create("https://dcsa.org"))
      //         .withType("org.dcsa.v1.event")
      //         .withTime(OffsetDateTime.now())
      //         .withData("application/json", "{\"firstName\":\"John\",
      // \"lastName\":\"Doe\"}".getBytes())
      //         .build();
    }
  }

  @Service
  public static class CoreService {

    private final RabbitMessagingTemplate rabbitMessagingTemplate;
    private final ObjectMapper objectMapper;

    public CoreService(RabbitTemplate rabbitTemplate, ObjectMapper objectMapper) {
      this.rabbitMessagingTemplate = new RabbitMessagingTemplate(rabbitTemplate);
      this.objectMapper = objectMapper;
    }

    @Bean
    public Queue myQueue() {
      return new Queue("equipmentReference.producer-core", true);
    }

    // method to trigger an event
    public void triggerEvent() throws JsonProcessingException {
      // loop through all the subscriptions
      for (SubscriptionTO subscription : subscriptions.values()) {
        // an event is created
        var event = new EventTO(UUID.randomUUID().toString(), "ACT", "LOAD", "LADEN");

        //.withData("{\"firstName\":\"John\", \"lastName\":\"Doe\"}".getBytes())


        Message<byte[]> message =
            CloudEventMessageBuilder.withData(objectMapper.writeValueAsBytes(event))
                .setId(event.id.toString())
                .setSource(URI.create("http://member.dcsa.org"))
                .setType("org.dcsa.v1.event")
                .setTime(OffsetDateTime.now())
                .setHeader("callbackurl", subscription.callbackUrl)
                .setHeader("subscriptionid", subscription.subscriptionId)
                .setHeader("secret", subscription.secret)
                .setHeader("topic", subscription.subscribeOn.topic)
                .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.APPLICATION_JSON)
                .build(CloudEventMessageUtils.AMQP_ATTR_PREFIX);

        LOGGER.info("Sending message to RabbitMQ");

        // send the event to the topic
        String topic = subscription.subscribeOn.topic + ".producer-core";
        rabbitMessagingTemplate.send(topic, message);
      }
    }

    // @RabbitListener(queues = "equipmentReference", group = "producer-core")
    // public void listen(Message<String> event) throws Exception{
    //   System.out.println("Cloud Event 'specversion': " +
    // CloudEventMessageUtils.getSpecVersion(event));
    //   System.out.println("Cloud Event': " + event);
    //   System.out.println("Cloud Event data: " + event.getPayload());
    // }
  }
}
