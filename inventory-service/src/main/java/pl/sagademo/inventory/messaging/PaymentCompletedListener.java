package pl.sagademo.inventory.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.Topics;
import pl.sagademo.events.payload.PaymentCompleted;
import pl.sagademo.inventory.service.InventoryService;
import pl.sagademo.kafka.EventCodec;
import pl.sagademo.kafka.KafkaRecords;

@Component
public class PaymentCompletedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentCompletedListener.class);

    private final EventCodec codec;
    private final InventoryService inventoryService;

    public PaymentCompletedListener(EventCodec codec, InventoryService inventoryService) {
        this.codec = codec;
        this.inventoryService = inventoryService;
    }

    @KafkaListener(topics = Topics.PAYMENT_COMPLETED)
    public void onPaymentCompleted(ConsumerRecord<String, String> record) {
        EventEnvelope<PaymentCompleted> event = codec.read(record.value(), PaymentCompleted.class);
        MDC.setContextMap(KafkaRecords.logContext(record, event));
        try {
            log.info("Received PaymentCompleted");
            inventoryService.reserveStock(event.orderId(), KafkaRecords.correlationId(record));
        } finally {
            MDC.clear();
        }
    }
}
