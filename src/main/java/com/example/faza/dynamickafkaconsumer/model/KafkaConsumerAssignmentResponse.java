package com.example.faza.dynamickafkaconsumer.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaConsumerAssignmentResponse {

    private String topic;

    private Integer partition;
}
