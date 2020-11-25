package com.faza.example.dynamickafkaconsumer.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
public class KafkaConsumerResponse {

    private String consumerId;
    private String groupId;
    private String listenerId;

    private Boolean active;

    private List<KafkaConsumerAssignmentResponse> assignments;
}
