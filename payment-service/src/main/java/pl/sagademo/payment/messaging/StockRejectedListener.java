package pl.sagademo.payment.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.Topics;
import pl.sagademo.events.payload.StockRejected;
import pl.sagademo.kafka.EventCodec;
import pl.sagademo.kafka.KafkaRecords;
import pl.sagademo.payment.service.PaymentService;

@Component
public class StockRejectedListener {

    private static final Logger log = LoggerFactory.getLogger(StockRejectedListener.class);

    private final EventCodec codec;
    private final PaymentService paymentService;

    public StockRejectedListener(EventCodec codec, PaymentService paymentService) {
        this.codec = codec;
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = Topics.STOCK_REJECTED)
    public void onStockRejected(ConsumerRecord<String, String> record) {
        EventEnvelope<StockRejected> event = codec.read(record.value(), StockRejected.class);
        MDC.setContextMap(KafkaRecords.logContext(record, event));
        try {
            log.info("Received StockRejected missing={}", event.payload().missingProductIds());
            paymentService.refund(event.orderId(), KafkaRecords.correlationId(record));
        } finally {
            MDC.clear();
        }
    }
}
