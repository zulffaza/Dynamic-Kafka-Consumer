package com.example.faza.dynamickafkaconsumer.configuration;

import com.example.faza.dynamickafkaconsumer.model.CustomKafkaListenerProperty;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

@Data
@ConfigurationProperties(prefix = "custom.kafka")
public class CustomKafkaListenerProperties {

    private Map<String, CustomKafkaListenerProperty> listeners;
}
