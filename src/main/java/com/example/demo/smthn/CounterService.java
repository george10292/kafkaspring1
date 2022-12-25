package com.example.demo.smthn;

import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class CounterService {

    private AtomicInteger counter = new AtomicInteger(0);

    void increment() {
        counter.incrementAndGet();
    }

    Integer getResult () {
        return counter.get();
    }
}
