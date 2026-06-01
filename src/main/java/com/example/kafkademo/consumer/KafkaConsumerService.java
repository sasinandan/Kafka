package com.example.kafkademo.consumer;

import com.example.kafkademo.producer.KafkaProducerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class KafkaConsumerService {

    @Autowired
    KafkaProducerService kafkaProducerService;

    @KafkaListener(
            topics = "${app.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        log.info("============================================");
        log.info("Message received!");
        log.info("Topic     : {}", record.topic());
        log.info("Partition : {}", record.partition());
        log.info("Offset    : {}", record.offset());
        log.info("Key       : {}", record.key());
        log.info("Value     : {}", record.value());
        log.info("Timestamp : {}", record.timestamp());
        log.info("============================================");

        if (record.key() != null || record.value() != null) {
            kafkaProducerService.sendMessageToTopic1(record.key(), record.value());
        }
        // Manually acknowledge — tells Kafka this message was processed successfully
        acknowledgment.acknowledge();
    }
}
