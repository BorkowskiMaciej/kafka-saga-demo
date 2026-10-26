package pl.sagademo.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static pl.sagademo.it.KafkaProbe.keyIs;
import static pl.sagademo.it.SagaAssertions.assertNoDeadLetters;
import static pl.sagademo.it.SagaAssertions.paymentFailureReason;
import static pl.sagademo.it.SagaAssertions.paymentStatus;
import static pl.sagademo.it.SagaAssertions.stock;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pl.sagademo.events.EventHeaders;
import pl.sagademo.events.Topics;
import pl.sagademo.it.OrderApi.OrderView;

class SagaFlowIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(45);

    private static SagaEnvironment env;

    @BeforeAll
    static void startEnvironment() {
        env = SagaEnvironment.get();
    }

    @Test
    void approvedOrderIsPaidReservedAndConfirmed() {
        long stockBefore = stock(env, "p-1");

        UUID id = env.orders().create("c-happy", "p-1", 2, "100.00");
        OrderView order = env.orders().awaitStatus(id, "CONFIRMED", TIMEOUT);

        String key = id.toString();
        assertThat(order.cancellationReason()).isNull();
        assertThat(stock(env, "p-1")).isEqualTo(stockBefore - 2);
        assertThat(paymentStatus(env, id)).isEqualTo("COMPLETED");
        assertThat(env.kafka().snapshot(Topics.ORDER_CREATED, keyIs(key))).hasSize(1);
        assertThat(env.kafka().snapshot(Topics.PAYMENT_COMPLETED, keyIs(key))).hasSize(1);
        assertThat(env.kafka().snapshot(Topics.STOCK_RESERVED, keyIs(key))).hasSize(1);
        assertThat(env.kafka().snapshot(Topics.PAYMENT_FAILED, keyIs(key))).isEmpty();
        assertThat(env.kafka().snapshot(Topics.STOCK_REJECTED, keyIs(key))).isEmpty();
        assertThat(env.kafka().snapshot(Topics.PAYMENT_REFUNDED, keyIs(key))).isEmpty();
        assertNoDeadLetters(env, List.of(id));
    }

    @Test
    void orderAtExactlyThePaymentLimitIsApproved() {
        UUID id = env.orders().create("c-limit", "p-1", 1, "5000.00");

        OrderView order = env.orders().awaitStatus(id, "CONFIRMED", TIMEOUT);

        assertThat(order.status()).isEqualTo("CONFIRMED");
        assertThat(paymentStatus(env, id)).isEqualTo("COMPLETED");
    }

    @Test
    void orderAbovePaymentLimitIsCancelledWithoutTouchingStock() {
        long stockBefore = stock(env, "p-1");

        UUID id = env.orders().create("c-limit-exceeded", "p-1", 1, "5000.01");
        OrderView order = env.orders().awaitStatus(id, "CANCELLED", TIMEOUT);

        String key = id.toString();
        assertThat(order.cancellationReason()).isEqualTo("PAYMENT_FAILED");
        assertThat(paymentStatus(env, id)).isEqualTo("FAILED");
        assertThat(paymentFailureReason(env, id)).isEqualTo("LIMIT_EXCEEDED");
        assertThat(env.kafka().snapshot(Topics.PAYMENT_FAILED, keyIs(key))).hasSize(1);
        assertThat(env.kafka().snapshot(Topics.PAYMENT_COMPLETED, keyIs(key))).isEmpty();
        assertThat(env.kafka().snapshot(Topics.STOCK_RESERVED, keyIs(key))).isEmpty();
        assertThat(stock(env, "p-1")).isEqualTo(stockBefore);
    }

    @Test
    void orderWithDeclinedAmountIsCancelled() {
        UUID id = env.orders().create("c-declined", "p-1", 1, "13.13");

        OrderView order = env.orders().awaitStatus(id, "CANCELLED", TIMEOUT);

        assertThat(order.cancellationReason()).isEqualTo("PAYMENT_FAILED");
        assertThat(paymentFailureReason(env, id)).isEqualTo("CARD_DECLINED");
    }

    @Test
    void outOfStockOrderIsCancelledAndPaymentRefunded() {
        UUID id = env.orders().create("c-no-stock", "p-3", 1, "100.00");

        OrderView order = env.orders().awaitStatus(id, "CANCELLED", TIMEOUT);

        String key = id.toString();
        assertThat(order.cancellationReason()).isEqualTo("OUT_OF_STOCK");
        await("payment refunded")
                .atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(paymentStatus(env, id)).isEqualTo("REFUNDED"));
        assertThat(env.kafka().awaitRecords(Topics.PAYMENT_REFUNDED, keyIs(key), 1, TIMEOUT)).hasSize(1);
        assertThat(env.kafka().snapshot(Topics.STOCK_REJECTED, keyIs(key))).hasSize(1);
        assertThat(env.kafka().snapshot(Topics.STOCK_RESERVED, keyIs(key))).isEmpty();
    }

    @Test
    void limitedStockIsReservedUntilExhausted() {
        UUID first = env.orders().create("c-last-units", "p-2", 5, "10.00");
        env.orders().awaitStatus(first, "CONFIRMED", TIMEOUT);
        assertThat(stock(env, "p-2")).isZero();

        UUID second = env.orders().create("c-too-late", "p-2", 1, "10.00");
        OrderView order = env.orders().awaitStatus(second, "CANCELLED", TIMEOUT);

        assertThat(order.cancellationReason()).isEqualTo("OUT_OF_STOCK");
        assertThat(stock(env, "p-2")).isZero();
    }

    @Test
    void correlationIdIsPropagatedAcrossTheWholeChain() {
        UUID id = env.orders().create("c-correlation", "p-1", 1, "10.00");
        env.orders().awaitStatus(id, "CONFIRMED", TIMEOUT);

        String key = id.toString();
        String created = correlationId(Topics.ORDER_CREATED, key);
        String paid = correlationId(Topics.PAYMENT_COMPLETED, key);
        String reserved = correlationId(Topics.STOCK_RESERVED, key);

        assertThat(created).isNotBlank().isNotEqualTo(key);
        assertThat(paid).isEqualTo(created);
        assertThat(reserved).isEqualTo(created);
    }

    @Test
    void duplicateDeliveryOfOrderCreatedHasNoSecondEffect() {
        UUID id = env.orders().create("c-duplicate", "p-1", 1, "10.00");
        env.orders().awaitStatus(id, "CONFIRMED", TIMEOUT);
        String key = id.toString();
        long stockAfterFirst = stock(env, "p-1");
        ConsumerRecord<String, String> original =
                env.kafka().awaitRecords(Topics.ORDER_CREATED, keyIs(key), 1, TIMEOUT).getFirst();

        env.kafka().send(Topics.ORDER_CREATED, key, original.value());

        await("duplicate has no effect")
                .during(Duration.ofSeconds(4))
                .atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> {
                    assertThat(env.kafka().snapshot(Topics.PAYMENT_COMPLETED, keyIs(key))).hasSize(1);
                    assertThat(env.kafka().snapshot(Topics.STOCK_RESERVED, keyIs(key))).hasSize(1);
                    assertThat(env.database().count(
                                    "payment_db", "SELECT count(*) FROM payments WHERE order_id = ?", id))
                            .isEqualTo(1);
                    assertThat(stock(env, "p-1")).isEqualTo(stockAfterFirst);
                });
        assertThat(env.orders().view(id).status()).isEqualTo("CONFIRMED");
    }

    private static String correlationId(String topic, String key) {
        List<ConsumerRecord<String, String>> records = env.kafka().awaitRecords(topic, keyIs(key), 1, TIMEOUT);
        assertThat(records).as("records on %s", topic).hasSize(1);
        Header header = records.getFirst().headers().lastHeader(EventHeaders.CORRELATION_ID);
        assertThat(header).as("correlation header on %s", topic).isNotNull();
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
