package pl.sagademo.kafka;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.support.TransactionTemplate;

public class OutboxRelay implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private static final long CLEANUP_INTERVAL_NANOS = Duration.ofMinutes(1).toNanos();
    private static final Duration CLAIM_TIMEOUT = Duration.ofSeconds(30);

    private static final String SELECT_SQL =
            "SELECT seq, event_id, event_type, topic, message_key, payload, correlation_id "
                    + "FROM outbox WHERE published_at IS NULL "
                    + "AND (claimed_at IS NULL OR claimed_at < now() - make_interval(secs => :claimTimeout)) "
                    + "ORDER BY seq LIMIT :limit FOR UPDATE SKIP LOCKED";
    private static final String CLAIM_SQL =
            "UPDATE outbox SET claimed_by = :relayId, claimed_at = now() WHERE seq IN (:seqs)";
    private static final String MARK_SQL =
            "UPDATE outbox SET published_at = now(), claimed_by = NULL, claimed_at = NULL "
                    + "WHERE claimed_by = :relayId AND seq IN (:seqs)";
    private static final String RELEASE_SQL =
            "UPDATE outbox SET claimed_by = NULL, claimed_at = NULL "
                    + "WHERE claimed_by = :relayId AND seq IN (:seqs)";
    private static final String CLEANUP_SQL =
            "DELETE FROM outbox WHERE published_at IS NOT NULL "
                    + "AND published_at < now() - make_interval(secs => :seconds)";
    private static final String PROCESSED_EVENTS_CLEANUP_SQL =
            "DELETE FROM processed_events WHERE processed_at < now() - make_interval(secs => :seconds)";

    private record Row(
            long seq, UUID eventId, String eventType, String topic, String key, String payload, String correlationId) {
    }

    private final JdbcClient jdbcClient;
    private final TransactionTemplate transactionTemplate;
    private final EventPublisher publisher;
    private final OutboxProperties properties;
    private final Semaphore signal = new Semaphore(0);
    private final UUID relayId = UUID.randomUUID();

    private volatile boolean running;
    private Thread thread;
    private long lastCleanupNanos = System.nanoTime();

    public OutboxRelay(
            JdbcClient jdbcClient,
            TransactionTemplate transactionTemplate,
            EventPublisher publisher,
            OutboxProperties properties) {
        this.jdbcClient = jdbcClient;
        this.transactionTemplate = transactionTemplate;
        this.publisher = publisher;
        this.properties = properties;
    }

    public void wake() {
        signal.release();
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        thread = Thread.ofPlatform().name("outbox-relay").daemon(true).start(this::loop);
    }

    @Override
    public synchronized void stop() {
        running = false;
        signal.release();
        if (thread == null) {
            return;
        }
        try {
            thread.join(Duration.ofSeconds(15));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        thread = null;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void loop() {
        while (running) {
            try {
                int published;
                do {
                    published = publishBatch();
                } while (running && published == properties.batchSize());
                cleanupIfDue();
            } catch (RuntimeException e) {
                log.warn("Outbox relay iteration failed: {}", e.toString());
            }
            awaitSignal();
        }
    }

    private void awaitSignal() {
        try {
            signal.tryAcquire(properties.pollInterval().toNanos(), TimeUnit.NANOSECONDS);
            signal.drainPermits();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }

    private int publishBatch() {
        List<Row> rows = transactionTemplate.execute(status -> {
            List<Row> claimed = jdbcClient.sql(SELECT_SQL)
                    .param("limit", properties.batchSize())
                    .param("claimTimeout", (double) CLAIM_TIMEOUT.toSeconds())
                    .query(OutboxRelay::mapRow)
                    .list();
            if (!claimed.isEmpty()) {
                jdbcClient.sql(CLAIM_SQL)
                        .param("relayId", relayId)
                        .param("seqs", claimed.stream().map(Row::seq).toList())
                        .update();
            }
            return claimed;
        });
        if (rows == null || rows.isEmpty()) {
            return 0;
        }

        List<CompletableFuture<SendResult<String, String>>> sends = rows.stream().map(this::send).toList();
        List<Long> delivered = new ArrayList<>();
        List<Long> failed = new ArrayList<>();
        for (int i = 0; i < rows.size(); i++) {
            Row row = rows.get(i);
            try {
                SendResult<String, String> result = sends.get(i).join();
                delivered.add(row.seq());
                log.info("Published eventType={} eventId={} orderId={} topic={} partition={} offset={}",
                        row.eventType(),
                        row.eventId(),
                        row.key(),
                        row.topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } catch (RuntimeException e) {
                failed.add(row.seq());
                log.warn("Outbox publish failed eventType={} eventId={} topic={}: {}",
                        row.eventType(), row.eventId(), row.topic(), e.toString());
            }
        }
        transactionTemplate.executeWithoutResult(status -> {
            if (!delivered.isEmpty()) {
                jdbcClient.sql(MARK_SQL)
                        .param("relayId", relayId)
                        .param("seqs", delivered)
                        .update();
            }
            if (!failed.isEmpty()) {
                jdbcClient.sql(RELEASE_SQL)
                        .param("relayId", relayId)
                        .param("seqs", failed)
                        .update();
            }
        });
        return delivered.size();
    }

    private CompletableFuture<SendResult<String, String>> send(Row row) {
        try {
            return publisher.sendAsync(row.topic(), row.key(), row.payload(), row.correlationId());
        } catch (RuntimeException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private void cleanupIfDue() {
        long now = System.nanoTime();
        if (now - lastCleanupNanos < CLEANUP_INTERVAL_NANOS) {
            return;
        }
        lastCleanupNanos = now;
        int removed = jdbcClient.sql(CLEANUP_SQL)
                .param("seconds", (double) properties.retention().toSeconds())
                .update();
        if (removed > 0) {
            log.debug("Outbox cleanup removed={}", removed);
        }
        int processedRemoved = jdbcClient.sql(PROCESSED_EVENTS_CLEANUP_SQL)
                .param("seconds", (double) properties.processedEventsRetention().toSeconds())
                .update();
        if (processedRemoved > 0) {
            log.debug("Processed events cleanup removed={}", processedRemoved);
        }
    }

    private static Row mapRow(ResultSet resultSet, int rowNumber) throws SQLException {
        return new Row(
                resultSet.getLong("seq"),
                resultSet.getObject("event_id", UUID.class),
                resultSet.getString("event_type"),
                resultSet.getString("topic"),
                resultSet.getString("message_key"),
                resultSet.getString("payload"),
                resultSet.getString("correlation_id"));
    }
}
