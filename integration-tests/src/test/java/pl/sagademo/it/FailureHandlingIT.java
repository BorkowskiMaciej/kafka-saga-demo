package pl.sagademo.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static pl.sagademo.it.KafkaProbe.keyIs;
import static pl.sagademo.it.SagaAssertions.assertNoDeadLetters;
import static pl.sagademo.it.SagaAssertions.stock;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pl.sagademo.events.Topics;

class FailureHandlingIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);

    private static SagaEnvironment env;

    @BeforeAll
    static void startEnvironment() {
        env = SagaEnvironment.get();
    }

    @Test
    void malformedMessageGoesStraightToDeadLetterTopicsWithoutRetries() {
        String key = UUID.randomUUID().toString();

        env.kafka().send(Topics.ORDER_CREATED, key, "to nie jest json");

        assertThat(env.kafka().awaitRecords("orders.created-payment-dlt", keyIs(key), 1, TIMEOUT)).hasSize(1);
        assertThat(env.kafka().awaitRecords("orders.created-inventory-dlt", keyIs(key), 1, TIMEOUT)).hasSize(1);
        await("DLT handlers log the failure with correlation context")
                .atMost(TIMEOUT)
                .until(() -> env.paymentService().logContains("Message sent to DLT", key)
                        && env.inventoryService().logContains("Message sent to DLT", key));
        for (String service : List.of("payment", "inventory")) {
            for (int index = 0; index < 3; index++) {
                String retryTopic = "orders.created-" + service + "-retry-" + index;
                assertThat(env.kafka().snapshot(retryTopic, keyIs(key))).as(retryTopic).isEmpty();
            }
        }
    }

    @Test
    void paymentCompletedBeforeOrderCreatedIsDeferredByInventory() {
        UUID unknownOrder = UUID.randomUUID();
        String key = unknownOrder.toString();

        env.kafka().send(Topics.PAYMENT_COMPLETED, key, paymentCompleted(unknownOrder));
        env.kafka().send(Topics.ORDER_CREATED, key, orderCreated(unknownOrder));

        assertThat(env.kafka().awaitRecords(Topics.STOCK_RESERVED, keyIs(key), 1, TIMEOUT)).hasSize(1);
        assertThat(env.kafka().snapshot(Topics.PAYMENT_COMPLETED + "-inventory-dlt", keyIs(key))).isEmpty();
        for (int index = 0; index < 3; index++) {
            String retryTopic = Topics.PAYMENT_COMPLETED + "-inventory-retry-" + index;
            assertThat(env.kafka().snapshot(retryTopic, keyIs(key))).as(retryTopic).isEmpty();
        }
    }

    @Test
    void ordersAreNotLostWhileInventoryServiceIsDown() {
        long stockBefore = stock(env, "p-1");
        List<UUID> ids = new ArrayList<>();
        env.inventoryService().stop();
        try {
            for (int i = 0; i < 10; i++) {
                ids.add(env.orders().create("c-inventory-down", "p-1", 1, "10.00"));
            }
            env.orders().awaitAll(ids, "PAID", Duration.ofSeconds(60));
            await("orders stay PAID while inventory is down")
                    .during(Duration.ofSeconds(3))
                    .atMost(Duration.ofSeconds(15))
                    .untilAsserted(() -> {
                        for (UUID id : ids) {
                            assertThat(env.orders().view(id).status()).isEqualTo("PAID");
                        }
                    });
        } finally {
            env.inventoryService().start();
            env.inventoryService().awaitReady(Duration.ofSeconds(120));
        }

        env.orders().awaitAll(ids, "CONFIRMED", Duration.ofSeconds(90));

        assertThat(stock(env, "p-1")).isEqualTo(stockBefore - ids.size());
        for (UUID id : ids) {
            assertThat(env.kafka().snapshot(Topics.STOCK_RESERVED, keyIs(id.toString()))).hasSize(1);
        }
        assertNoDeadLetters(env, ids);
    }

    private static String paymentCompleted(UUID orderId) {
        return """
                {"eventId":"%s","eventType":"PaymentCompleted","occurredAt":"%s","version":1,"orderId":"%s","payload":{"orderId":"%s","paymentId":"%s","amount":100.00}}"""
                .formatted(UUID.randomUUID(), Instant.now(), orderId, orderId, UUID.randomUUID());
    }

    private static String orderCreated(UUID orderId) {
        return """
                {"eventId":"%s","eventType":"OrderCreated","occurredAt":"%s","version":1,"orderId":"%s","payload":{"orderId":"%s","customerId":"out-of-order","items":[{"productId":"p-1","quantity":1,"unitPrice":6000.00}],"totalAmount":6000.00}}"""
                .formatted(UUID.randomUUID(), Instant.now(), orderId, orderId);
    }
}
