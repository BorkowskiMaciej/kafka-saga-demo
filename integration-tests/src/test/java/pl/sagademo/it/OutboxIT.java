package pl.sagademo.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static pl.sagademo.it.SagaAssertions.paymentStatus;
import static pl.sagademo.it.SagaAssertions.stock;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class OutboxIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final List<String> DATABASES = List.of("order_db", "payment_db", "inventory_db");

    private static SagaEnvironment env;

    @BeforeAll
    static void startEnvironment() {
        env = SagaEnvironment.get();
    }

    @Test
    void everyServiceStoresExactlyOneEventPerOrderAndDrainsItsOutbox() {
        UUID id = env.orders().create("c-outbox", "p-1", 1, "10.00");
        env.orders().awaitStatus(id, "CONFIRMED", TIMEOUT);

        for (String database : DATABASES) {
            await("outbox of " + database + " drained")
                    .atMost(TIMEOUT)
                    .untilAsserted(() -> assertThat(unpublished(database, id)).isZero());
            assertThat(total(database, id)).as(database).isEqualTo(1);
        }
    }

    @Test
    void ordersAreAcceptedWhileKafkaIsDownAndDeliveredAfterRecovery() {
        long stockBefore = stock(env, "p-1");
        UUID id;
        env.pauseKafka();
        try {
            long started = System.nanoTime();
            id = env.orders().create("c-kafka-down", "p-1", 1, "10.00");
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(5));
            UUID orderId = id;

            await("order stays CREATED and its event waits in the outbox")
                    .during(Duration.ofSeconds(6))
                    .atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> {
                        assertThat(env.orders().view(orderId).status()).isEqualTo("CREATED");
                        assertThat(unpublished("order_db", orderId)).isEqualTo(1);
                    });
        } finally {
            env.resumeKafka();
        }

        env.orders().awaitStatus(id, "CONFIRMED", TIMEOUT);

        assertThat(paymentStatus(env, id)).isEqualTo("COMPLETED");
        assertThat(env.database().count("payment_db", "SELECT count(*) FROM payments WHERE order_id = ?", id))
                .isEqualTo(1);
        assertThat(stock(env, "p-1")).isEqualTo(stockBefore - 1);
        UUID confirmed = id;
        for (String database : DATABASES) {
            await("outbox of " + database + " drained")
                    .atMost(TIMEOUT)
                    .untilAsserted(() -> assertThat(unpublished(database, confirmed)).isZero());
        }
    }

    private static long unpublished(String database, UUID orderId) {
        return env.database().count(
                database,
                "SELECT count(*) FROM outbox WHERE message_key = ? AND published_at IS NULL",
                orderId.toString());
    }

    private static long total(String database, UUID orderId) {
        return env.database().count(
                database, "SELECT count(*) FROM outbox WHERE message_key = ?", orderId.toString());
    }
}
