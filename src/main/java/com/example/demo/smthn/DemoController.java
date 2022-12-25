package com.example.demo.smthn;

import lombok.AllArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
public class DemoController {

    private KafkaTemplate<String, String> template;
    private CounterService counterService;

    DemoController (KafkaTemplate<String, String> template, CounterService counterService) {
        this.template = template;
        this.counterService = counterService;
    }

    private String topic = "spring-kafka-demo";

    @GetMapping("/send-random-message")
    String sendRandomMessage() {
        template.send( topic, UUID.randomUUID().toString(), UUID.randomUUID().toString() );
        return "OK";
    }

    @GetMapping("/counter-result")
    void getCounterResult() {
        counterService.getResult();
    }



}
