package pl.sagademo.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.EventTypes;
import pl.sagademo.events.payload.OrderCreated;
import pl.sagademo.events.payload.OrderItem;
import pl.sagademo.events.payload.PaymentFailed;
import pl.sagademo.events.payload.PaymentFailureReason;
import tools.jackson.databind.json.JsonMapper;

class EventCodecTest {

    private final EventCodec codec = new EventCodec(JsonMapper.builder().build());

    @Test
    void roundTripsEnvelopeWithTypedPayload() {
        UUID orderId = UUID.randomUUID();
        OrderCreated payload = OrderCreated.of(
                orderId, "customer-1", List.of(new OrderItem("p-1", 2, new BigDecimal("50.00"))));
        EventEnvelope<OrderCreated> original = EventEnvelope.of(EventTypes.ORDER_CREATED, orderId, payload);

        EventEnvelope<OrderCreated> restored = codec.read(codec.write(original), OrderCreated.class);

        assertThat(restored.eventId()).isEqualTo(original.eventId());
        assertThat(restored.eventType()).isEqualTo(EventTypes.ORDER_CREATED);
        assertThat(restored.version()).isEqualTo(EventEnvelope.CURRENT_VERSION);
        assertThat(restored.orderId()).isEqualTo(orderId);
        assertThat(restored.payload()).isInstanceOf(OrderCreated.class);
        assertThat(restored.payload().totalAmount()).isEqualByComparingTo("100.00");
        assertThat(restored.payload().items()).containsExactly(payload.items().get(0));
    }

    @Test
    void roundTripsEnumPayload() {
        UUID orderId = UUID.randomUUID();
        PaymentFailed payload = new PaymentFailed(orderId, PaymentFailureReason.LIMIT_EXCEEDED);
        EventEnvelope<PaymentFailed> original = EventEnvelope.of(EventTypes.PAYMENT_FAILED, orderId, payload);

        EventEnvelope<PaymentFailed> restored = codec.read(codec.write(original), PaymentFailed.class);

        assertThat(restored.payload().reason()).isEqualTo(PaymentFailureReason.LIMIT_EXCEEDED);
    }
}
