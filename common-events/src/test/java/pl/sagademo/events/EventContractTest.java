package pl.sagademo.events;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import pl.sagademo.events.payload.OrderCreated;
import pl.sagademo.events.payload.OrderItem;
import pl.sagademo.events.payload.StockReserved;

class EventContractTest {

    @Test
    void envelopeCarriesIdsVersionAndType() {
        UUID orderId = UUID.randomUUID();

        EventEnvelope<StockReserved> event =
                EventEnvelope.of(EventTypes.STOCK_RESERVED, orderId, new StockReserved(orderId));

        assertThat(event.eventId()).isNotNull();
        assertThat(event.orderId()).isEqualTo(orderId);
        assertThat(event.eventType()).isEqualTo("StockReserved");
        assertThat(event.version()).isEqualTo(EventEnvelope.CURRENT_VERSION);
        assertThat(event.occurredAt()).isNotNull();
    }

    @Test
    void orderCreatedComputesTotalFromItems() {
        OrderCreated order = OrderCreated.of(
                UUID.randomUUID(),
                "customer-1",
                List.of(
                        new OrderItem("p-1", 2, new BigDecimal("50.00")),
                        new OrderItem("p-2", 1, new BigDecimal("0.50"))));

        assertThat(order.totalAmount()).isEqualByComparingTo("100.50");
    }

    @Test
    void orderItemRejectsInvalidValues() {
        assertThatThrownBy(() -> new OrderItem("p-1", 0, BigDecimal.ONE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OrderItem("p-1", 1, BigDecimal.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new OrderItem(" ", 1, BigDecimal.ONE))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void orderCreatedRejectsEmptyItems() {
        assertThatThrownBy(() -> OrderCreated.of(UUID.randomUUID(), "customer-1", List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void topicListMatchesConstants() {
        assertThat(Topics.ALL).containsExactly(
                "orders.created",
                "payments.completed",
                "payments.failed",
                "payments.refunded",
                "inventory.reserved",
                "inventory.rejected");
    }
}
