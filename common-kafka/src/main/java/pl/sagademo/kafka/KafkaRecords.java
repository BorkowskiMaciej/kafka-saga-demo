package pl.sagademo.kafka;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.MDC;
import org.springframework.kafka.support.KafkaHeaders;
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
                "correlationId", correlationId(record),
                "topic", record.topic(),
                "partition", String.valueOf(record.partition()),
                "offset", String.valueOf(record.offset()));
    }

    public static void logDeadLetter(Logger log, ConsumerRecord<?, ?> record) {
        Map<String, String> context = new HashMap<>();
        context.put("correlationId", correlationId(record));
        context.put("topic", record.topic());
        context.put("partition", String.valueOf(record.partition()));
        context.put("offset", String.valueOf(record.offset()));

        MDC.setContextMap(context);
        try {
            log.error(
                    "Message sent to DLT exception={} reason={}",
                    headerValue(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                    headerValue(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE));
        } finally {
            MDC.clear();
        }
    }

    private static String headerValue(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "unknown" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
