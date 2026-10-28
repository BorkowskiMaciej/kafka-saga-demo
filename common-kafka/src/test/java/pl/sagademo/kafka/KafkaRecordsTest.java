package pl.sagademo.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.EventHeaders;
import pl.sagademo.events.EventTypes;
import pl.sagademo.events.payload.StockReserved;

class KafkaRecordsTest {

    @Test
    void readsCorrelationIdFromHeader() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders.created", 1, 5L, "key", "value");
        record.headers().add(EventHeaders.CORRELATION_ID, "corr-1".getBytes(StandardCharsets.UTF_8));

        assertThat(KafkaRecords.correlationId(record)).isEqualTo("corr-1");
    }

    @Test
    void fallsBackToRecordKeyWhenHeaderIsMissing() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders.created", 1, 5L, "order-1", "value");

        assertThat(KafkaRecords.correlationId(record)).isEqualTo("order-1");
    }

    @Test
    void buildsLogContextFromRecordAndEnvelope() {
        UUID orderId = UUID.randomUUID();
        EventEnvelope<StockReserved> event =
                EventEnvelope.of(EventTypes.STOCK_RESERVED, orderId, new StockReserved(orderId));
        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("inventory.reserved", 2, 7L, orderId.toString(), "{}");

        assertThat(KafkaRecords.logContext(record, event))
                .containsEntry("eventId", event.eventId().toString())
                .containsEntry("orderId", orderId.toString())
                .containsEntry("correlationId", orderId.toString())
                .containsEntry("topic", "inventory.reserved")
                .containsEntry("partition", "2")
                .containsEntry("offset", "7");
    }
}
