package com.example.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author: gjwu
 * @create: 2021-03-16 16:24
 * @description:
 **/
@Service
public final class ConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerService.class);

    @KafkaListener(topics = "kafkaTopic", groupId = "group_id")
    public void consume(String message) {
        logger.info(String.format("$$$$ => Consumed message: %s", message));
    }

    @KafkaListener(topics = "kafkaBatchTopic", groupId = "group_id", containerFactory = "batchFactory")
    public void batchConsumer(List<String> messages) {
        messages.forEach(msg -> logger.info(String.format("$$$$ => Consumed message: %s", msg)));
    }
}
