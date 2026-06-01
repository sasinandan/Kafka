package com.example.kafkademo;

import com.example.kafkademo.consumer.KafkaConsumerService;
import com.example.kafkademo.producer.KafkaProducerService;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(
        partitions = 1,
        topics = {"test-topic", "test-topic1"},
        bootstrapServersProperty = "spring.kafka.bootstrap-servers"
)
class KafkaDemoApplicationTests {

    @Autowired
    private KafkaProducerService producerService;

    @MockitoSpyBean
    private KafkaConsumerService consumerService;

    @Test
    void contextLoads() {
        // Verifies the Spring context starts up correctly with an embedded Kafka broker
    }

    @Test
    void testProducerConsumerFlow() {
        producerService.sendMessage("test-key", "hello-embedded-kafka");

        // Verify that the consumer received the message
        Mockito.verify(consumerService, Mockito.timeout(10000).times(1))
                .consume(
                        Mockito.argThat(record -> "test-key".equals(record.key()) && "hello-embedded-kafka".equals(record.value())),
                        Mockito.any()
                );
    }
}
