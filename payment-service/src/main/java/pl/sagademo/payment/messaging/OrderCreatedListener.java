package pl.sagademo.payment.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.Topics;
import pl.sagademo.events.payload.OrderCreated;
import pl.sagademo.kafka.EventCodec;
import pl.sagademo.kafka.IdempotentExecutor;
import pl.sagademo.kafka.KafkaRecords;
import pl.sagademo.kafka.ResilientListener;
import pl.sagademo.payment.service.PaymentService;

@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final EventCodec codec;
    private final IdempotentExecutor idempotentExecutor;
    private final PaymentService paymentService;

    public OrderCreatedListener(
            EventCodec codec, IdempotentExecutor idempotentExecutor, PaymentService paymentService) {
        this.codec = codec;
        this.idempotentExecutor = idempotentExecutor;
        this.paymentService = paymentService;
    }

    @ResilientListener(retryTopicSuffix = "-payment-retry", dltTopicSuffix = "-payment-dlt")
    @KafkaListener(topics = Topics.ORDER_CREATED)
    public void onOrderCreated(ConsumerRecord<String, String> record) {
        EventEnvelope<OrderCreated> event = codec.read(record.value(), OrderCreated.class);
        MDC.setContextMap(KafkaRecords.logContext(record, event));
        try {
            log.info("Received OrderCreated");
            idempotentExecutor.runOnce(
                    event, () -> paymentService.process(event.payload(), KafkaRecords.correlationId(record)));
        } finally {
            MDC.clear();
        }
    }

    @DltHandler
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        KafkaRecords.logDeadLetter(log, record);
    }
}
