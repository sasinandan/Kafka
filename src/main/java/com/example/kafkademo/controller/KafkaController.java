package com.example.kafkademo.controller;

import com.example.kafkademo.model.MessageRequest;
import com.example.kafkademo.producer.KafkaProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class KafkaController {

    private final KafkaProducerService producerService;

    /**
     * POST /api/kafka/publish
     * Body: { "key": "myKey", "message": "Hello Kafka!" }
     */
    @PostMapping("/publish")
    public ResponseEntity<Map<String, String>> publishMessage(@RequestBody MessageRequest request) {
        log.info("REST request to publish message: {}", request);
        producerService.sendMessage(request.getKey(), request.getMessage());
        return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Message sent to Kafka topic",
                "key", request.getKey()
        ));
    }

    /**
     * GET /api/kafka/publish?key=myKey&message=HelloKafka
     * Convenient quick-test via browser or curl
     */
    @GetMapping("/publish")
    public ResponseEntity<Map<String, String>> publishMessageGet(
            @RequestParam String key,
            @RequestParam String message) {
        producerService.sendMessage(key, message);
        return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Message sent to Kafka topic",
                "key", key
        ));
    }
}
