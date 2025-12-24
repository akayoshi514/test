package com.example.batch.repository;

import com.example.batch.domain.JobItem;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public class JobItemRepository {

    private final DatabaseClient client;

    public JobItemRepository(DatabaseClient client) {
        this.client = client;
    }

    public Flux<JobItem> lockNextPendingBatch(int batchSize) {
        String sql = """
            SELECT id, payload
            FROM job_item
            WHERE status = 'PENDING'
            FOR UPDATE SKIP LOCKED
            LIMIT :limit
        """;

        return client.sql(sql)
                .bind("limit", batchSize)
                .map((row, meta) -> new JobItem(
                        row.get("id", Long.class),
                        row.get("payload", String.class)
                ))
                .all();
    }

    public Mono<Integer> markAsProcessing(Long id) {
        return client.sql("""
                UPDATE job_item
                SET status='PROCESSING'
                WHERE id=:id
                """)
                .bind("id", id)
                .fetch()
                .rowsUpdated();
    }

    public Mono<Integer> markAsDone(Long id) {
        return client.sql("""
                UPDATE job_item
                SET status='DONE'
                WHERE id=:id
                """)
                .bind("id", id)
                .fetch()
                .rowsUpdated();
    }

    public Mono<Integer> markAsError(Long id, String reason) {
        return client.sql("""
                UPDATE job_item
                SET status='ERROR'
                WHERE id=:id
                """)
                .bind("id", id)
                .fetch()
                .rowsUpdated();
    }
}
