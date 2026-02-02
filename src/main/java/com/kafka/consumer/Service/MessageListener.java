package com.kafka.consumer.Service;

import com.kafka.consumer.dto.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * MessageListener acts as a Kafka Consumer.
 *
 * This class contains multiple Kafka listeners,
 * all listening to the SAME topic and SAME consumer group.
 */
@Service
public class MessageListener {

    /**
     * Logger for logging received messages
     */
    private final Logger log = LoggerFactory.getLogger(MessageListener.class);

    /**
     * Listener 1
     *
     * - Listens to topic: Test_topic
     * - Belongs to consumer group: testgroup
     *
     * Kafka assigns ONE partition to this listener
     * (if available during rebalance)
     */
//    @KafkaListener(topics = "Test_topic", groupId = "testgroup")
//    public void listener1(String message) {
//        log.info("Message received by consumer-1 --> {}", message);
//    }

    /**
     * Listener 2
     *
     * Same topic + same groupId means:
     * - Kafka will NOT send duplicate messages
     * - Messages are load-balanced across listeners
     */
//    @KafkaListener(topics = "Test_topic", groupId = "testgroup")
//    public void listener2(String message) {
//        log.info("Message received by consumer-2 --> {}", message);
//    }

    /**
     * Listener 3
     *
     * If topic has 3 partitions,
     * this listener may get one partition assigned.
     */
//    @KafkaListener(topics = "Test_topic", groupId = "testgroup")
//    public void listener3(String message) {
//        log.info("Message received by consumer-3 --> {}", message);
//    }

    /**
     * Listener 4
     *
     * IMPORTANT:
     * If partitions < consumers,
     * this listener will stay IDLE (no messages).
     */
//    @KafkaListener(topics = "Test_topic", groupId = "testgroup")
//    public void listener4(String message) {
//        log.info("Message received by consumer-4 --> {}", message);
//    }


    @KafkaListener(topics = "Test_event",groupId = "testevent")
    public void eventListner(Customer customer){
        log.info(" Event reciver by consumer 1 ==> {} ",customer);
    }
}
