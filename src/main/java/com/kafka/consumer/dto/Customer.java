package com.kafka.consumer.dto;

import lombok.Data;

@Data
public class Customer {
    private int customerId;
    private String name;
    private String email;
    private String mobile;
}
