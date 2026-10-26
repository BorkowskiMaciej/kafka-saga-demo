package pl.sagademo.kafka;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import pl.sagademo.events.EventEnvelope;

public class OutboxWriter {

    private static final String INSERT_SQL =
            "INSERT INTO outbox (event_id, event_type, topic, message_key, payload, correlation_id, created_at) "
                    + "VALUES (:eventId, :eventType, :topic, :messageKey, :payload, :correlationId, now())";

    private final JdbcClient jdbcClient;
    private final EventCodec codec;
    private final Runnable afterCommit;

    public OutboxWriter(JdbcClient jdbcClient, EventCodec codec, Runnable afterCommit) {
        this.jdbcClient = jdbcClient;
        this.codec = codec;
        this.afterCommit = afterCommit;
    }

    public void enqueue(String topic, EventEnvelope<?> envelope, String correlationId) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new IllegalStateException("Outbox requires an active transaction");
        }
        jdbcClient.sql(INSERT_SQL)
                .param("eventId", envelope.eventId())
                .param("eventType", envelope.eventType())
                .param("topic", topic)
                .param("messageKey", envelope.orderId().toString())
                .param("payload", codec.write(envelope))
                .param("correlationId", correlationId)
                .update();
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                afterCommit.run();
            }
        });
    }
}
