package pl.sagademo.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.transaction.support.TransactionTemplate;
import pl.sagademo.events.EventEnvelope;

public class IdempotentExecutor {

    private static final Logger log = LoggerFactory.getLogger(IdempotentExecutor.class);

    private static final String MARK_PROCESSED_SQL =
            "INSERT INTO processed_events (event_id, event_type, processed_at) "
                    + "VALUES (:eventId, :eventType, now()) "
                    + "ON CONFLICT (event_id) DO NOTHING";

    private final JdbcClient jdbcClient;
    private final TransactionTemplate transactionTemplate;

    public IdempotentExecutor(JdbcClient jdbcClient, TransactionTemplate transactionTemplate) {
        this.jdbcClient = jdbcClient;
        this.transactionTemplate = transactionTemplate;
    }

    public boolean runOnce(EventEnvelope<?> event, Runnable action) {
        Boolean executed = transactionTemplate.execute(status -> {
            int inserted = jdbcClient.sql(MARK_PROCESSED_SQL)
                    .param("eventId", event.eventId())
                    .param("eventType", event.eventType())
                    .update();
            if (inserted == 0) {
                return false;
            }
            action.run();
            return true;
        });

        if (!Boolean.TRUE.equals(executed)) {
            log.warn("Duplicate event ignored eventId={} eventType={}", event.eventId(), event.eventType());
            return false;
        }
        return true;
    }
}
