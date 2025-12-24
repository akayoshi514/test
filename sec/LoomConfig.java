package com.example.batch.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class LoomConfig {

    @Bean(destroyMethod = "shutdown")
    public ExecutorService virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    @Bean
    public Scheduler virtualThreadScheduler(ExecutorService executor) {
        return Schedulers.fromExecutor(executor);
    }
}
