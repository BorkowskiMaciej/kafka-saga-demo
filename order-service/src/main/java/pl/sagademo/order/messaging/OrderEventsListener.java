package pl.sagademo.order.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
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
import pl.sagademo.kafka.KafkaRecords;
import pl.sagademo.order.service.OrderStatusService;

@Component
public class OrderEventsListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventsListener.class);

    private final EventCodec codec;
    private final OrderStatusService orderStatusService;

    public OrderEventsListener(EventCodec codec, OrderStatusService orderStatusService) {
        this.codec = codec;
        this.orderStatusService = orderStatusService;
    }

    @KafkaListener(topics = Topics.PAYMENT_COMPLETED)
    public void onPaymentCompleted(ConsumerRecord<String, String> record) {
        EventEnvelope<PaymentCompleted> event = codec.read(record.value(), PaymentCompleted.class);
        handle(record, event, () -> orderStatusService.markPaid(event.orderId()));
    }

    @KafkaListener(topics = Topics.PAYMENT_FAILED)
    public void onPaymentFailed(ConsumerRecord<String, String> record) {
        EventEnvelope<PaymentFailed> event = codec.read(record.value(), PaymentFailed.class);
        handle(record, event, () -> orderStatusService.cancel(event.orderId(), CancellationReason.PAYMENT_FAILED));
    }

    @KafkaListener(topics = Topics.STOCK_RESERVED)
    public void onStockReserved(ConsumerRecord<String, String> record) {
        EventEnvelope<StockReserved> event = codec.read(record.value(), StockReserved.class);
        handle(record, event, () -> orderStatusService.confirm(event.orderId()));
    }

    @KafkaListener(topics = Topics.STOCK_REJECTED)
    public void onStockRejected(ConsumerRecord<String, String> record) {
        EventEnvelope<StockRejected> event = codec.read(record.value(), StockRejected.class);
        handle(record, event, () -> orderStatusService.cancel(event.orderId(), CancellationReason.OUT_OF_STOCK));
    }

    private void handle(ConsumerRecord<String, String> record, EventEnvelope<?> event, Runnable action) {
        MDC.setContextMap(KafkaRecords.logContext(record, event));
        try {
            log.info("Received {}", event.eventType());
            action.run();
        } finally {
            MDC.clear();
        }
    }
}
