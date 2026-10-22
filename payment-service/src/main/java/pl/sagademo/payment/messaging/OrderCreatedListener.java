package pl.sagademo.payment.messaging;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.Topics;
import pl.sagademo.events.payload.OrderCreated;
import pl.sagademo.kafka.EventCodec;
import pl.sagademo.kafka.KafkaRecords;
import pl.sagademo.payment.service.PaymentService;

@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final EventCodec codec;
    private final PaymentService paymentService;

    public OrderCreatedListener(EventCodec codec, PaymentService paymentService) {
        this.codec = codec;
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = Topics.ORDER_CREATED)
    public void onOrderCreated(ConsumerRecord<String, String> record) {
        EventEnvelope<OrderCreated> event = codec.read(record.value(), OrderCreated.class);
        MDC.setContextMap(Map.of(
                "eventId", event.eventId().toString(),
                "orderId", event.orderId().toString(),
                "topic", record.topic(),
                "partition", String.valueOf(record.partition()),
                "offset", String.valueOf(record.offset())));
        try {
            log.info("Received OrderCreated");
            paymentService.process(event.payload(), KafkaRecords.correlationId(record));
        } finally {
            MDC.clear();
        }
    }
}
