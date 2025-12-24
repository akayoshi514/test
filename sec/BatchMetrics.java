package com.example.batch.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

@Component
public class BatchMetrics {

    private final Counter success;
    private final Counter failure;

    public BatchMetrics(MeterRegistry registry) {
        this.success = Counter.builder("batch_items_success_total")
                .description("Items traités avec succès")
                .register(registry);

        this.failure = Counter.builder("batch_items_failure_total")
                .description("Items en erreur")
                .register(registry);
    }

    public void markSuccess() {
        success.increment();
    }

    public void markFailure() {
        failure.increment();
    }
}
