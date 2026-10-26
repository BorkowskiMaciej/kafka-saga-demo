package pl.sagademo.it;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class KafkaProbe {

    private final String bootstrapServers;

    public KafkaProbe(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String bootstrapServers() {
        return bootstrapServers;
    }

    public void send(String topic, String key, String value) {
        Map<String, Object> config = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.ACKS_CONFIG, "all");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(config)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get(15, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Cannot send to " + topic, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted", e);
        }
    }

    public List<ConsumerRecord<String, String>> snapshot(String topic, Predicate<ConsumerRecord<String, String>> filter) {
        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            List<TopicPartition> partitions = partitionsOf(consumer, topic);
            if (partitions.isEmpty()) {
                return matched;
            }
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (System.nanoTime() < deadline && !caughtUp(consumer, partitions, ends)) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (filter.test(record)) {
                        matched.add(record);
                    }
                }
            }
        }
        return matched;
    }

    public List<ConsumerRecord<String, String>> awaitRecords(
            String topic, Predicate<ConsumerRecord<String, String>> filter, int expected, Duration timeout) {
        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = newConsumer()) {
            long deadline = System.nanoTime() + timeout.toNanos();
            List<TopicPartition> partitions = List.of();
            while (System.nanoTime() < deadline && partitions.isEmpty()) {
                partitions = partitionsOf(consumer, topic);
                if (partitions.isEmpty()) {
                    sleep(300);
                }
            }
            if (partitions.isEmpty()) {
                return matched;
            }
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            while (System.nanoTime() < deadline && matched.size() < expected) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(200))) {
                    if (filter.test(record)) {
                        matched.add(record);
                    }
                }
            }
        }
        return matched;
    }

    public List<String> deadLetterTopics() {
        try (Admin admin = Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            return admin.listTopics().names().get(15, TimeUnit.SECONDS).stream()
                    .filter(name -> name.endsWith("-dlt"))
                    .sorted()
                    .toList();
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Cannot list topics", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted", e);
        }
    }

    public Admin admin() {
        return Admin.create(Map.<String, Object>of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers));
    }

    public static Predicate<ConsumerRecord<String, String>> keyIs(String key) {
        return record -> key.equals(record.key());
    }

    private KafkaConsumer<String, String> newConsumer() {
        return new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "probe-" + UUID.randomUUID(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"));
    }

    private static List<TopicPartition> partitionsOf(KafkaConsumer<String, String> consumer, String topic) {
        List<PartitionInfo> infos = consumer.listTopics(Duration.ofSeconds(10)).get(topic);
        if (infos == null) {
            return List.of();
        }
        return infos.stream().map(info -> new TopicPartition(info.topic(), info.partition())).toList();
    }

    private static boolean caughtUp(
            KafkaConsumer<String, String> consumer, List<TopicPartition> partitions, Map<TopicPartition, Long> ends) {
        return partitions.stream().allMatch(partition -> consumer.position(partition) >= ends.get(partition));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted", e);
        }
    }
}
