package com.faza.example.dynamickafkaconsumer.listener;

import com.faza.example.dynamickafkaconsumer.model.Constant;
import com.faza.example.dynamickafkaconsumer.model.ConsumerActionRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.faza.example.dynamickafkaconsumer.model.CustomKafkaListenerProperty;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Slf4j
@Component
public class ConsumerActionListener {

    @Autowired
    private CustomKafkaListenerRegistrar customKafkaListenerRegistrar;

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private ObjectMapper objectMapper;

    @SneakyThrows
    @KafkaListener(topics = Constant.CONSUMER_ACTION_TOPIC,
            id = "my-message-consumer-#{T(java.util.UUID).randomUUID().toString()}")
    public void consumerActionListener(ConsumerRecord<String, String> record) {
        log.info("Consumer action listener got a new record: " + record);
        ConsumerActionRequest consumerActionRequest = objectMapper.readValue(record.value(),
                ConsumerActionRequest.class);
        processAction(consumerActionRequest);
        log.info("Consumer action listener done processing record: " + record);
    }

    private void processAction(ConsumerActionRequest request) {
        String consumerId = request.getConsumerId();
        MessageListenerContainer listenerContainer = Optional.ofNullable(consumerId)
                .map(kafkaListenerEndpointRegistry::getListenerContainer)
                .orElse(null);
        switch (request.getConsumerAction()) {
            case CREATE:
                CustomKafkaListenerProperty consumerProperty = request.getConsumerProperty();
                log.info(String.format("Creating a %s consumer for topic %s",
                        consumerProperty.getListenerClass(), consumerProperty.getTopic()));
                customKafkaListenerRegistrar.registerCustomKafkaListener(null,
                        consumerProperty, request.getStartImmediately());
                break;
            case ACTIVATE:
                log.info("Running a consumer with id " + consumerId);
                listenerContainer.start();
                break;
            case PAUSE:
                log.info("Pausing a consumer with id " + consumerId);
                listenerContainer.pause();
                break;
            case RESUME:
                log.info("Resuming a consumer with id " + consumerId);
                listenerContainer.resume();
                break;
            case DEACTIVATE:
                log.info("Stopping a consumer with id " + consumerId);
                listenerContainer.stop();
                break;
            default:
                log.warn("Consumer action listener do not know action: " +
                        request.getConsumerAction());
        }
    }
}
