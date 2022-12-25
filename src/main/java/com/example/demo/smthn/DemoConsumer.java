package com.example.demo.smthn;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class DemoConsumer {

    private CounterService counterService;

    @KafkaListener(topics = {"spring-kafka-demo"}, concurrency = "10")
    void listenTopic (ConsumerRecord<String, String> record) {
        counterService.increment();
    }
}
