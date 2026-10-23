package pl.sagademo.kafka;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.EventHeaders;

public final class KafkaRecords {

    private KafkaRecords() {
    }

    public static String correlationId(ConsumerRecord<?, ?> record) {
        Header header = record.headers().lastHeader(EventHeaders.CORRELATION_ID);
        if (header == null) {
            return String.valueOf(record.key());
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    public static Map<String, String> logContext(ConsumerRecord<?, ?> record, EventEnvelope<?> event) {
        return Map.of(
                "eventId", event.eventId().toString(),
                "orderId", event.orderId().toString(),
                "topic", record.topic(),
                "partition", String.valueOf(record.partition()),
                "offset", String.valueOf(record.offset()));
    }
}
