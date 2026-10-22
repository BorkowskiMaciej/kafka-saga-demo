package pl.sagademo.payment.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.EventTypes;
import pl.sagademo.events.Topics;
import pl.sagademo.events.payload.OrderCreated;
import pl.sagademo.events.payload.PaymentCompleted;
import pl.sagademo.events.payload.PaymentFailed;
import pl.sagademo.kafka.EventPublisher;
import pl.sagademo.payment.domain.Payment;
import pl.sagademo.payment.domain.PaymentRepository;

@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRules rules;
    private final PaymentRepository paymentRepository;
    private final EventPublisher eventPublisher;

    public PaymentService(PaymentRules rules, PaymentRepository paymentRepository, EventPublisher eventPublisher) {
        this.rules = rules;
        this.paymentRepository = paymentRepository;
        this.eventPublisher = eventPublisher;
    }

    @Transactional
    public void process(OrderCreated order, String correlationId) {
        PaymentDecision decision = rules.evaluate(order.totalAmount());

        if (decision.approved()) {
            Payment payment = paymentRepository.save(Payment.completed(order.orderId(), order.totalAmount()));
            eventPublisher.publish(
                    Topics.PAYMENT_COMPLETED,
                    EventEnvelope.of(
                            EventTypes.PAYMENT_COMPLETED,
                            order.orderId(),
                            new PaymentCompleted(order.orderId(), payment.getId(), payment.getAmount())),
                    correlationId);
            log.info("Payment completed paymentId={} amount={}", payment.getId(), payment.getAmount());
            return;
        }

        Payment payment = paymentRepository.save(
                Payment.failed(order.orderId(), order.totalAmount(), decision.reason()));
        eventPublisher.publish(
                Topics.PAYMENT_FAILED,
                EventEnvelope.of(
                        EventTypes.PAYMENT_FAILED,
                        order.orderId(),
                        new PaymentFailed(order.orderId(), decision.reason())),
                correlationId);
        log.info("Payment failed paymentId={} reason={}", payment.getId(), decision.reason());
    }
}
