package pl.sagademo.inventory.domain;

import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class OrderProcessingLockRepository {

    private static final String LOCK_SQL =
            "SELECT 1 AS acquired FROM pg_advisory_xact_lock(:key)";

    private final JdbcClient jdbcClient;

    public OrderProcessingLockRepository(JdbcClient jdbcClient) {
        this.jdbcClient = jdbcClient;
    }

    public void lock(UUID orderId) {
        jdbcClient.sql(LOCK_SQL)
                .param("key", lockKey(orderId))
                .query(Integer.class)
                .single();
    }

    private static long lockKey(UUID orderId) {
        return orderId.getMostSignificantBits() ^ orderId.getLeastSignificantBits();
    }
}
