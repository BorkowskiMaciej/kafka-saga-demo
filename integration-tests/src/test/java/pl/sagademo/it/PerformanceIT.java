package pl.sagademo.it;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static pl.sagademo.it.SagaAssertions.assertNoDeadLetters;
import static pl.sagademo.it.SagaAssertions.stock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import pl.sagademo.events.Topics;

class PerformanceIT {

    private static final int ORDERS = Integer.getInteger("perf.orders", 300);
    private static final int CONCURRENCY = Integer.getInteger("perf.concurrency", 32);
    private static final long TIMEOUT_SECONDS = Long.getLong("perf.timeoutSeconds", 180L);
    private static final long MAX_P95_MILLIS = Long.getLong("perf.maxP95Millis", 15_000L);
    private static final double MIN_THROUGHPUT = Double.parseDouble(System.getProperty("perf.minThroughput", "5"));
    private static final long STOCK_RESERVE = 1_000L;

    private static SagaEnvironment env;

    private record Submission(UUID id, long nanos) {
    }

    @BeforeAll
    static void startEnvironment() {
        env = SagaEnvironment.get();
    }

    @Test
    void sagaSustainsLoadWithoutLossOrDuplicates() throws Exception {
        env.database().execute(
                "inventory_db", "UPDATE products SET stock = ? WHERE product_id = ?", ORDERS + STOCK_RESERVE, "p-1");
        String customer = "perf-" + UUID.randomUUID();

        long start = System.nanoTime();
        List<Submission> submissions = submitOrders(customer);
        long submitted = System.nanoTime();

        await("all orders confirmed")
                .atMost(Duration.ofSeconds(TIMEOUT_SECONDS))
                .pollInterval(Duration.ofMillis(250))
                .until(() -> env.database().count(
                                "order_db",
                                "SELECT count(*) FROM orders WHERE customer_id = ? AND status = 'CONFIRMED'",
                                customer)
                        == ORDERS);
        long confirmed = System.nanoTime();

        Set<String> keys = new HashSet<>();
        submissions.forEach(submission -> keys.add(submission.id().toString()));
        Map<String, Long> createdAt = timestamps(Topics.ORDER_CREATED, keys);
        List<ConsumerRecord<String, String>> reserved = env.kafka()
                .snapshot(Topics.STOCK_RESERVED, record -> keys.contains(record.key()));
        Map<String, Long> reservedAt = new HashMap<>();
        reserved.forEach(record -> reservedAt.put(record.key(), record.timestamp()));

        long[] pipeline = keys.stream().mapToLong(key -> reservedAt.get(key) - createdAt.get(key)).sorted().toArray();
        long[] submit = submissions.stream().mapToLong(s -> s.nanos() / 1_000_000L).sorted().toArray();
        double endToEndSeconds = (confirmed - start) / 1e9;
        double throughput = ORDERS / endToEndSeconds;
        long firstCreated = createdAt.values().stream().mapToLong(Long::longValue).min().orElseThrow();
        long lastReserved = reservedAt.values().stream().mapToLong(Long::longValue).max().orElseThrow();
        double pipelineThroughput = ORDERS / Math.max((lastReserved - firstCreated) / 1000.0, 0.001);

        String report = """
                Saga performance report
                  orders                      : %d
                  submit concurrency          : %d
                  submit phase                : %.2f s
                  submit latency p50/p95/max  : %d / %d / %d ms
                  end-to-end (POST -> all CONFIRMED) : %.2f s
                  end-to-end throughput       : %.1f orders/s
                  pipeline throughput (Kafka timestamps) : %.1f orders/s
                  pipeline latency p50/p95/p99/max (OrderCreated -> StockReserved) : %d / %d / %d / %d ms
                """.formatted(
                ORDERS,
                CONCURRENCY,
                (submitted - start) / 1e9,
                percentile(submit, 0.50),
                percentile(submit, 0.95),
                submit[submit.length - 1],
                endToEndSeconds,
                throughput,
                pipelineThroughput,
                percentile(pipeline, 0.50),
                percentile(pipeline, 0.95),
                percentile(pipeline, 0.99),
                pipeline[pipeline.length - 1]);
        System.out.println(report);
        writeReport(report);

        assertThat(reserved).as("StockReserved events, exactly one per order").hasSize(ORDERS);
        assertThat(stock(env, "p-1")).isEqualTo(STOCK_RESERVE);
        assertNoDeadLetters(env, submissions.stream().map(Submission::id).toList());
        assertThat(percentile(pipeline, 0.95)).as("pipeline p95 latency [ms]").isLessThanOrEqualTo(MAX_P95_MILLIS);
        assertThat(throughput).as("end-to-end throughput [orders/s]").isGreaterThanOrEqualTo(MIN_THROUGHPUT);
    }

    private static List<Submission> submitOrders(String customer) throws InterruptedException, ExecutionException {
        Semaphore gate = new Semaphore(CONCURRENCY);
        List<Future<Submission>> futures = new ArrayList<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < ORDERS; i++) {
                futures.add(executor.submit(() -> {
                    gate.acquire();
                    try {
                        long begin = System.nanoTime();
                        UUID id = env.orders().create(customer, "p-1", 1, "10.00");
                        return new Submission(id, System.nanoTime() - begin);
                    } finally {
                        gate.release();
                    }
                }));
            }
        }
        List<Submission> submissions = new ArrayList<>();
        for (Future<Submission> future : futures) {
            submissions.add(future.get());
        }
        return submissions;
    }

    private static Map<String, Long> timestamps(String topic, Set<String> keys) {
        Map<String, Long> result = new HashMap<>();
        env.kafka().snapshot(topic, record -> keys.contains(record.key()))
                .forEach(record -> result.put(record.key(), record.timestamp()));
        assertThat(result).as("records on %s", topic).hasSize(keys.size());
        return result;
    }

    private static long percentile(long[] sorted, double percentile) {
        int index = (int) Math.ceil(percentile * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(index, sorted.length - 1))];
    }

    private static void writeReport(String report) throws IOException {
        Path target = Path.of("target");
        Files.createDirectories(target);
        Files.writeString(target.resolve("perf-report.txt"), report);
    }
}
