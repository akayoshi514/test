package com.example.batch.service;

import com.example.batch.domain.JobItem;
import com.example.batch.metrics.BatchMetrics;
import com.example.batch.repository.JobItemRepository;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
public class JobProcessorService {

    private final JobItemRepository repo;
    private final WebClient webClient;
    private final Scheduler scheduler;
    private final BatchMetrics metrics;

    public JobProcessorService(JobItemRepository repo,
                               WebClient webClient,
                               Scheduler scheduler,
                               BatchMetrics metrics) {
        this.repo = repo;
        this.webClient = webClient;
        this.scheduler = scheduler;
        this.metrics = metrics;
    }

    public Mono<Void> runBatchOnce(int batchSize, int concurrency) {
        return repo.lockNextPendingBatch(batchSize)
                .flatMap(item ->
                                Mono.defer(() -> processOneItem(item))
                                    .subscribeOn(scheduler)
                                    .onErrorResume(ex -> handleError(item, ex)),
                         concurrency
                )
                .then();
    }

    private Mono<Void> processOneItem(JobItem item) {
        return repo.markAsProcessing(item.id())
                .then(callExternalApi(item.payload()))
                .then(repo.markAsDone(item.id()))
                .doOnSuccess(v -> metrics.markSuccess())
                .then();
    }

    private Mono<Void> handleError(JobItem item, Throwable ex) {
        metrics.markFailure();
        return repo.markAsError(item.id(), ex.getMessage()).then();
    }

    private Mono<Void> callExternalApi(String payload) {
        return webClient.post()
                .uri("/process")
                .bodyValue(payload)
                .retrieve()
                .bodyToMono(Void.class)
                .retryWhen(
                        Retry.backoff(3, Duration.ofSeconds(1))
                )
                .then();
    }
}
