package pl.sagademo.inventory.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.Topics;
import pl.sagademo.events.payload.PaymentCompleted;
import pl.sagademo.inventory.service.InventoryService;
import pl.sagademo.kafka.EventCodec;
import pl.sagademo.kafka.IdempotentExecutor;
import pl.sagademo.kafka.KafkaRecords;
import pl.sagademo.kafka.ResilientListener;

@Component
public class PaymentCompletedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentCompletedListener.class);

    private final EventCodec codec;
    private final IdempotentExecutor idempotentExecutor;
    private final InventoryService inventoryService;

    public PaymentCompletedListener(
            EventCodec codec, IdempotentExecutor idempotentExecutor, InventoryService inventoryService) {
        this.codec = codec;
        this.idempotentExecutor = idempotentExecutor;
        this.inventoryService = inventoryService;
    }

    @ResilientListener(retryTopicSuffix = "-inventory-retry", dltTopicSuffix = "-inventory-dlt")
    @KafkaListener(topics = Topics.PAYMENT_COMPLETED)
    public void onPaymentCompleted(ConsumerRecord<String, String> record) {
        try (KafkaRecords.LogContext ignored = KafkaRecords.openLogContext(record)) {
            EventEnvelope<PaymentCompleted> event = codec.read(record.value(), PaymentCompleted.class);
            KafkaRecords.addEventContext(event);
            log.info("Received PaymentCompleted");
            idempotentExecutor.runOnce(
                    event, () -> inventoryService.reserveStock(event.orderId(), KafkaRecords.correlationId(record)));
        } catch (RuntimeException e) {
            KafkaRecords.logProcessingFailure(log, record, "PaymentCompleted processing failed", e);
            throw e;
        }
    }

    @DltHandler
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        KafkaRecords.logDeadLetter(log, record);
    }
}
