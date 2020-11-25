package com.faza.example.dynamickafkaconsumer.listener;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.config.MethodKafkaListenerEndpoint;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

import java.util.Optional;

public abstract class CustomMessageListener {

    private static int NUMBER_OF_LISTENERS = 0;

    @Autowired
    private KafkaProperties kafkaProperties;

    public abstract KafkaListenerEndpoint createKafkaListenerEndpoint(String name, String topic);

    protected MethodKafkaListenerEndpoint<String, String> createDefaultMethodKafkaListenerEndpoint(String name,
                                                                                                   String topic) {
        MethodKafkaListenerEndpoint<String, String> kafkaListenerEndpoint = new MethodKafkaListenerEndpoint<>();
        kafkaListenerEndpoint.setId(getConsumerId(name));
        kafkaListenerEndpoint.setGroupId(kafkaProperties.getConsumer().getGroupId());
        kafkaListenerEndpoint.setAutoStartup(true);
        kafkaListenerEndpoint.setTopics(topic);
        kafkaListenerEndpoint.setMessageHandlerMethodFactory(new DefaultMessageHandlerMethodFactory());
        return kafkaListenerEndpoint;
    }

    private String getConsumerId(String name) {
        if (isBlank(name)) {
            return CustomMessageListener.class.getCanonicalName() + "#" + NUMBER_OF_LISTENERS++;
        } else {
            return name;
        }
    }

    private boolean isBlank(String string) {
        return Optional.ofNullable(string)
                .map(String::isBlank)
                .orElse(true);
    }
}
