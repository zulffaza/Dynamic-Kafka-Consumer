package com.faza.example.dynamickafkaconsumer.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.faza.example.dynamickafkaconsumer.model.Constant;
import com.faza.example.dynamickafkaconsumer.model.ConsumerAction;
import com.faza.example.dynamickafkaconsumer.model.ConsumerActionRequest;
import com.faza.example.dynamickafkaconsumer.model.CustomKafkaListenerProperty;
import com.faza.example.dynamickafkaconsumer.model.KafkaConsumerAssignmentResponse;
import com.faza.example.dynamickafkaconsumer.model.KafkaConsumerResponse;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping(path = "/api/kafka/registry")
public class KafkaConsumerRegistryController {

    @Autowired
    private KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping
    public List<KafkaConsumerResponse> getConsumerIds() {
        return kafkaListenerEndpointRegistry.getListenerContainerIds()
                .stream()
                .map(this::createKafkaConsumerResponse)
                .collect(Collectors.toList());
    }

    @PostMapping(path = "/create")
    @ResponseStatus(HttpStatus.CREATED)
    public void createConsumer(@RequestParam String topic, @RequestParam String listenerClass,
                               @RequestParam(required = false) boolean startImmediately) {
        publishMessage(ConsumerActionRequest.builder()
                .consumerProperty(CustomKafkaListenerProperty.builder()
                        .topic(topic)
                        .listenerClass(listenerClass)
                        .build())
                .startImmediately(startImmediately)
                .consumerAction(ConsumerAction.CREATE)
                .build());
    }

    @PostMapping(path = "/activate")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void activateConsumer(@RequestParam String consumerId) {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumerId);
        if (Objects.isNull(listenerContainer)) {
            throw new RuntimeException(String.format("Consumer with id %s is not found", consumerId));
        } else if (listenerContainer.isRunning()) {
            throw new RuntimeException(String.format("Consumer with id %s is already running", consumerId));
        } else {
            publishMessage(ConsumerActionRequest.builder()
                    .consumerId(consumerId)
                    .consumerAction(ConsumerAction.ACTIVATE)
                    .build());
        }
    }

    @PostMapping(path = "/pause")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void pauseConsumer(@RequestParam String consumerId) {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumerId);
        if (Objects.isNull(listenerContainer)) {
            throw new RuntimeException(String.format("Consumer with id %s is not found", consumerId));
        } else if (!listenerContainer.isRunning()) {
            throw new RuntimeException(String.format("Consumer with id %s is not running", consumerId));
        } else if (listenerContainer.isContainerPaused()) {
            throw new RuntimeException(String.format("Consumer with id %s is already paused", consumerId));
        } else if (listenerContainer.isPauseRequested()) {
            throw new RuntimeException(String.format("Consumer with id %s is already requested to be paused", consumerId));
        } else {
            publishMessage(ConsumerActionRequest.builder()
                    .consumerId(consumerId)
                    .consumerAction(ConsumerAction.PAUSE)
                    .build());
        }
    }

    @PostMapping(path = "/resume")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void resumeConsumer(@RequestParam String consumerId) {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumerId);
        if (Objects.isNull(listenerContainer)) {
            throw new RuntimeException(String.format("Consumer with id %s is not found", consumerId));
        } else if (!listenerContainer.isRunning()) {
            throw new RuntimeException(String.format("Consumer with id %s is not running", consumerId));
        } else if (!listenerContainer.isContainerPaused()) {
            throw new RuntimeException(String.format("Consumer with id %s is not paused", consumerId));
        } else {
            publishMessage(ConsumerActionRequest.builder()
                    .consumerId(consumerId)
                    .consumerAction(ConsumerAction.RESUME)
                    .build());
        }
    }

    @PostMapping(path = "/deactivate")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void deactivateConsumer(@RequestParam String consumerId) {
        MessageListenerContainer listenerContainer = kafkaListenerEndpointRegistry.getListenerContainer(consumerId);
        if (Objects.isNull(listenerContainer)) {
            throw new RuntimeException(String.format("Consumer with id %s is not found", consumerId));
        } else if (!listenerContainer.isRunning()) {
            throw new RuntimeException(String.format("Consumer with id %s is already stop", consumerId));
        } else {
            publishMessage(ConsumerActionRequest.builder()
                    .consumerId(consumerId)
                    .consumerAction(ConsumerAction.DEACTIVATE)
                    .build());
        }
    }

    private KafkaConsumerResponse createKafkaConsumerResponse(String consumerId) {
        MessageListenerContainer listenerContainer =
                kafkaListenerEndpointRegistry.getListenerContainer(consumerId);
        return KafkaConsumerResponse.builder()
                .consumerId(consumerId)
                .groupId(listenerContainer.getGroupId())
                .listenerId(listenerContainer.getListenerId())
                .active(listenerContainer.isRunning())
                .assignments(Optional.ofNullable(listenerContainer.getAssignedPartitions())
                        .map(topicPartitions -> topicPartitions.stream()
                                .map(this::createKafkaConsumerAssignmentResponse)
                                .collect(Collectors.toList()))
                        .orElse(null))
                .build();
    }

    private KafkaConsumerAssignmentResponse createKafkaConsumerAssignmentResponse(
            TopicPartition topicPartition) {
        return KafkaConsumerAssignmentResponse.builder()
                .topic(topicPartition.topic())
                .partition(topicPartition.partition())
                .build();
    }

    @SneakyThrows
    private void publishMessage(Object message) {
        log.info(String.format("Publishing message %s to %s", message, Constant.CONSUMER_ACTION_TOPIC));
        kafkaTemplate.send(Constant.CONSUMER_ACTION_TOPIC, objectMapper.writeValueAsString(message));
    }
}
