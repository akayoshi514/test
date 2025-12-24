package com.example.batch.launcher;

import com.example.batch.service.JobProcessorService;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class BatchLauncher implements ApplicationRunner {

    private final JobProcessorService service;

    public BatchLauncher(JobProcessorService service) {
        this.service = service;
    }

    @Override
    public void run(ApplicationArguments args) {
        int batchSize = 150;
        int concurrency = 150;

        Mono.defer(() -> service.runBatchOnce(batchSize, concurrency))
                .repeat()
                .subscribe();
    }
}
