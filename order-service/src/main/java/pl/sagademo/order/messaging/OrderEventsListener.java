package pl.sagademo.order.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.Topics;
import pl.sagademo.events.payload.CancellationReason;
import pl.sagademo.events.payload.PaymentCompleted;
import pl.sagademo.events.payload.PaymentFailed;
import pl.sagademo.events.payload.StockRejected;
import pl.sagademo.events.payload.StockReserved;
import pl.sagademo.kafka.EventCodec;
import pl.sagademo.kafka.IdempotentExecutor;
import pl.sagademo.kafka.KafkaRecords;
import pl.sagademo.kafka.ResilientListener;
import pl.sagademo.order.service.OrderStatusService;

@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final EventCodec codec;
    private final IdempotentExecutor idempotentExecutor;
    private final OrderStatusService orderStatusService;

    public OrderEventsListener(
            EventCodec codec, IdempotentExecutor idempotentExecutor, OrderStatusService orderStatusService) {
        this.codec = codec;
        this.idempotentExecutor = idempotentExecutor;
        this.orderStatusService = orderStatusService;
    }

    @ResilientListener(retryTopicSuffix = "-order-retry", dltTopicSuffix = "-order-dlt")
    @KafkaListener(topics = Topics.PAYMENT_COMPLETED)
    public void onPaymentCompleted(ConsumerRecord<String, String> record) {
        EventEnvelope<PaymentCompleted> event = read(record, PaymentCompleted.class);
        handle(record, event, () -> orderStatusService.markPaid(event.orderId()));
    }

    @ResilientListener(retryTopicSuffix = "-order-retry", dltTopicSuffix = "-order-dlt")
    @KafkaListener(topics = Topics.PAYMENT_FAILED)
    public void onPaymentFailed(ConsumerRecord<String, String> record) {
        EventEnvelope<PaymentFailed> event = read(record, PaymentFailed.class);
        handle(record, event, () -> orderStatusService.cancel(event.orderId(), CancellationReason.PAYMENT_FAILED));
    }

    @ResilientListener(retryTopicSuffix = "-order-retry", dltTopicSuffix = "-order-dlt")
    @KafkaListener(topics = Topics.STOCK_RESERVED)
    public void onStockReserved(ConsumerRecord<String, String> record) {
        EventEnvelope<StockReserved> event = read(record, StockReserved.class);
        handle(record, event, () -> orderStatusService.confirm(event.orderId()));
    }

    @ResilientListener(retryTopicSuffix = "-order-retry", dltTopicSuffix = "-order-dlt")
    @KafkaListener(topics = Topics.STOCK_REJECTED)
    public void onStockRejected(ConsumerRecord<String, String> record) {
        EventEnvelope<StockRejected> event = read(record, StockRejected.class);
        handle(record, event, () -> orderStatusService.cancel(event.orderId(), CancellationReason.OUT_OF_STOCK));
    }

    private void handle(ConsumerRecord<String, String> record, EventEnvelope<?> event, Runnable action) {
        try (KafkaRecords.LogContext ignored = KafkaRecords.openLogContext(record)) {
            KafkaRecords.addEventContext(event);
            log.info("Received {}", event.eventType());
            idempotentExecutor.runOnce(event, action);
        } catch (RuntimeException e) {
            KafkaRecords.logProcessingFailure(log, record, event.eventType() + " processing failed", e);
            throw e;
        }
    }

    private <T> EventEnvelope<T> read(ConsumerRecord<String, String> record, Class<T> payloadType) {
        try (KafkaRecords.LogContext ignored = KafkaRecords.openLogContext(record)) {
            return codec.read(record.value(), payloadType);
        } catch (RuntimeException e) {
            KafkaRecords.logProcessingFailure(log, record, "Kafka event deserialization failed", e);
            throw e;
        }
    }

    @DltHandler
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        KafkaRecords.logDeadLetter(log, record);
    }
}
