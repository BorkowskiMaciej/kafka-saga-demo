package pl.sagademo.it;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public final class SagaAssertions {

    private SagaAssertions() {
    }

    public static void assertNoDeadLetters(SagaEnvironment env, Collection<UUID> orderIds) {
        Set<String> keys = orderIds.stream().map(UUID::toString).collect(Collectors.toSet());
        for (String topic : env.kafka().deadLetterTopics()) {
            List<ConsumerRecord<String, String>> leaked =
                    env.kafka().snapshot(topic, record -> keys.contains(record.key()));
            assertThat(leaked).as("dead letters in %s", topic).isEmpty();
        }
    }

    public static long stock(SagaEnvironment env, String productId) {
        return env.database().count("inventory_db", "SELECT stock FROM products WHERE product_id = ?", productId);
    }

    public static String paymentStatus(SagaEnvironment env, UUID orderId) {
        return env.database().string("payment_db", "SELECT status FROM payments WHERE order_id = ?", orderId);
    }

    public static String paymentFailureReason(SagaEnvironment env, UUID orderId) {
        return env.database().string("payment_db", "SELECT failure_reason FROM payments WHERE order_id = ?", orderId);
    }
}
