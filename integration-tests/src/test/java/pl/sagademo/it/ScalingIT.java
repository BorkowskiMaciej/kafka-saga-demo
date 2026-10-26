package pl.sagademo.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static pl.sagademo.it.KafkaProbe.keyIs;
import static pl.sagademo.it.SagaAssertions.assertNoDeadLetters;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pl.sagademo.events.Topics;

class ScalingIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    private static SagaEnvironment env;

    @BeforeAll
    static void startEnvironment() {
        env = SagaEnvironment.get();
    }

    @Test
    void secondPaymentInstanceSharesPartitionsAndNothingIsProcessedTwice() {
        ServiceProcess second = env.startPaymentInstance();
        try {
            await("both payment instances own partitions of orders.created")
                    .atMost(TIMEOUT)
                    .pollInterval(Duration.ofSeconds(1))
                    .ignoreExceptions()
                    .until(() -> membersOwningOrderCreated() == 2);

            List<UUID> ids = new ArrayList<>();
            for (int i = 0; i < 60; i++) {
                ids.add(env.orders().create("c-scaling", "p-1", 1, "10.00"));
            }
            env.orders().awaitAll(ids, "CONFIRMED", TIMEOUT);

            for (UUID id : ids) {
                String key = id.toString();
                assertThat(env.kafka().snapshot(Topics.PAYMENT_COMPLETED, keyIs(key))).as(key).hasSize(1);
                assertThat(env.database().count(
                                "payment_db", "SELECT count(*) FROM payments WHERE order_id = ?", id))
                        .as(key)
                        .isEqualTo(1);
            }
            assertNoDeadLetters(env, ids);
        } finally {
            second.stop();
        }
    }

    private static long membersOwningOrderCreated() throws ExecutionException, InterruptedException, TimeoutException {
        try (Admin admin = env.kafka().admin()) {
            ConsumerGroupDescription group = admin.describeConsumerGroups(List.of("payment-service"))
                    .describedGroups()
                    .get("payment-service")
                    .get(15, TimeUnit.SECONDS);
            return group.members().stream().filter(ScalingIT::ownsOrderCreated).count();
        }
    }

    private static boolean ownsOrderCreated(MemberDescription member) {
        return member.assignment().topicPartitions().stream()
                .anyMatch(partition -> partition.topic().equals(Topics.ORDER_CREATED));
    }
}
