package pl.sagademo.payment.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.EventTypes;
import pl.sagademo.events.Topics;
import pl.sagademo.events.payload.OrderCreated;
import pl.sagademo.events.payload.OrderItem;
import pl.sagademo.events.payload.PaymentCompleted;
import pl.sagademo.events.payload.PaymentFailed;
import pl.sagademo.events.payload.PaymentFailureReason;
import pl.sagademo.events.payload.PaymentRefunded;
import pl.sagademo.kafka.OutboxWriter;
import pl.sagademo.payment.domain.Payment;
import pl.sagademo.payment.domain.PaymentNotFoundException;
import pl.sagademo.payment.domain.PaymentRepository;
import pl.sagademo.payment.domain.PaymentStatus;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private OutboxWriter outboxWriter;

    @Captor
    private ArgumentCaptor<EventEnvelope<?>> envelopeCaptor;

    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        PaymentRules rules = new PaymentRules(new PaymentProperties(new BigDecimal("5000.00"), new BigDecimal("13.13")));
        paymentService = new PaymentService(rules, paymentRepository, outboxWriter);
        lenient().when(paymentRepository.save(any(Payment.class))).then(returnsFirstArg());
    }

    @Test
    void enqueuesPaymentCompletedForApprovedOrder() {
        OrderCreated order = orderWithUnitPrice("100.00");

        paymentService.process(order, "corr-1");

        verify(outboxWriter).enqueue(eq(Topics.PAYMENT_COMPLETED), envelopeCaptor.capture(), eq("corr-1"));
        EventEnvelope<?> envelope = envelopeCaptor.getValue();
        assertThat(envelope.eventType()).isEqualTo(EventTypes.PAYMENT_COMPLETED);
        assertThat(envelope.orderId()).isEqualTo(order.orderId());
        PaymentCompleted payload = (PaymentCompleted) envelope.payload();
        assertThat(payload.amount()).isEqualByComparingTo("100.00");
    }

    @Test
    void enqueuesPaymentFailedWhenLimitExceeded() {
        OrderCreated order = orderWithUnitPrice("6000.00");

        paymentService.process(order, "corr-2");

        verify(outboxWriter).enqueue(eq(Topics.PAYMENT_FAILED), envelopeCaptor.capture(), eq("corr-2"));
        PaymentFailed payload = (PaymentFailed) envelopeCaptor.getValue().payload();
        assertThat(payload.reason()).isEqualTo(PaymentFailureReason.LIMIT_EXCEEDED);
    }

    @Test
    void enqueuesPaymentFailedWhenCardDeclined() {
        OrderCreated order = orderWithUnitPrice("13.13");

        paymentService.process(order, "corr-3");

        verify(outboxWriter).enqueue(eq(Topics.PAYMENT_FAILED), envelopeCaptor.capture(), eq("corr-3"));
        PaymentFailed payload = (PaymentFailed) envelopeCaptor.getValue().payload();
        assertThat(payload.reason()).isEqualTo(PaymentFailureReason.CARD_DECLINED);
    }

    @Test
    void refundsCompletedPaymentAndEnqueuesPaymentRefunded() {
        UUID orderId = UUID.randomUUID();
        Payment payment = Payment.completed(orderId, new BigDecimal("100.00"));
        when(paymentRepository.findByOrderId(orderId)).thenReturn(Optional.of(payment));

        paymentService.refund(orderId, "corr-4");

        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.REFUNDED);
        verify(outboxWriter).enqueue(eq(Topics.PAYMENT_REFUNDED), envelopeCaptor.capture(), eq("corr-4"));
        EventEnvelope<?> envelope = envelopeCaptor.getValue();
        assertThat(envelope.eventType()).isEqualTo(EventTypes.PAYMENT_REFUNDED);
        PaymentRefunded payload = (PaymentRefunded) envelope.payload();
        assertThat(payload.paymentId()).isEqualTo(payment.getId());
        assertThat(payload.amount()).isEqualByComparingTo("100.00");
    }

    @Test
    void failsWhenPaymentToRefundDoesNotExist() {
        UUID orderId = UUID.randomUUID();
        when(paymentRepository.findByOrderId(orderId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> paymentService.refund(orderId, "corr-5"))
                .isInstanceOf(PaymentNotFoundException.class);
        verifyNoInteractions(outboxWriter);
    }

    private OrderCreated orderWithUnitPrice(String unitPrice) {
        return OrderCreated.of(
                UUID.randomUUID(),
                "customer-1",
                List.of(new OrderItem("p-1", 1, new BigDecimal(unitPrice))));
    }
}
