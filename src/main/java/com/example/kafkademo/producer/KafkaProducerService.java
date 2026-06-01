package com.example.kafkademo.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaProducerService {

    @Value("${app.kafka.topic}")
    private String topic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String key, String message) {
        log.info("Sending message to topic [{}] | key: {} | message: {}", topic, key, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, message);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send message: {}", ex.getMessage());
            } else {
                log.info("Message sent successfully | topic: {} | partition: {} | offset: {}",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    //send message to test-topic1
    public void sendMessageToTopic1(String key, String message) {
        log.info("Sending message to topic [{}] | key: {} | message: {}", topic, key, message);

        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send("test-topic1", key, message);
        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send message: {}", ex.getMessage());
            } else {
                log.info("Message sent successfully");
            }
        });
    }
}
