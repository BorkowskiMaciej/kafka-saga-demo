package pl.sagademo.order.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import pl.sagademo.events.payload.OrderItem;
import pl.sagademo.events.payload.OrderStatus;

class OrderTest {

    @Test
    void acceptsStockConfirmationBeforeLocalPaymentProjection() {
        Order order = Order.create(
                UUID.randomUUID(),
                "customer-1",
                List.of(new OrderItem("p-1", 1, new BigDecimal("10.00"))),
                new BigDecimal("10.00"));

        order.confirm();
        order.markPaid();

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
    }
}
