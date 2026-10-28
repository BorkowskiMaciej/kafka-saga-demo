package pl.sagademo.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
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

    public static LogContext openLogContext(ConsumerRecord<?, ?> record) {
        Map<String, String> previous = MDC.getCopyOfContextMap();
        MDC.put("correlationId", correlationId(record));
        MDC.put("topic", record.topic());
        MDC.put("partition", String.valueOf(record.partition()));
        MDC.put("offset", String.valueOf(record.offset()));
        return new LogContext(previous);
    }

    public static void addEventContext(EventEnvelope<?> event) {
        MDC.put("eventId", event.eventId().toString());
        MDC.put("orderId", event.orderId().toString());
    }

    public static void logDeadLetter(Logger log, ConsumerRecord<?, ?> record) {
        try (LogContext ignored = openLogContext(record)) {
            MDC.put("dltTopic", record.topic());
            MDC.put("originalTopic", headerValue(record, KafkaHeaders.DLT_ORIGINAL_TOPIC));
            MDC.put("topic", headerValue(record, KafkaHeaders.DLT_ORIGINAL_TOPIC));
            log.error(
                    "Message sent to DLT originalPartition={} originalOffset={} exception={} reason={}",
                    headerNumber(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                    headerNumber(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                    headerValue(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                    headerValue(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE));
        }
    }

    public static void logProcessingFailure(Logger log, ConsumerRecord<?, ?> record, String message, Throwable error) {
        try (LogContext ignored = openLogContext(record)) {
            log.error(message, error);
        }
    }

    private static String headerValue(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "unknown" : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static String headerNumber(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            return "unknown";
        }
        ByteBuffer value = ByteBuffer.wrap(header.value());
        return switch (header.value().length) {
            case Integer.BYTES -> String.valueOf(value.getInt());
            case Long.BYTES -> String.valueOf(value.getLong());
            default -> "unknown";
        };
    }

    public static final class LogContext implements AutoCloseable {

        private final Map<String, String> previous;

        private LogContext(Map<String, String> previous) {
            this.previous = previous == null ? Collections.emptyMap() : previous;
        }

        @Override
        public void close() {
            if (previous.isEmpty()) {
                MDC.clear();
            } else {
                MDC.setContextMap(previous);
            }
        }
    }
}
