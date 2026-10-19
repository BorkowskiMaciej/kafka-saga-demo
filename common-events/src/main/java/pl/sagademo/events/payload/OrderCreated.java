package pl.sagademo.events.payload;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record OrderCreated(
        UUID orderId,
        String customerId,
        List<OrderItem> items,
        BigDecimal totalAmount
) {

    public OrderCreated {
        items = List.copyOf(items);
        if (items.isEmpty()) {
            throw new IllegalArgumentException("items must not be empty");
        }
    }

    public static OrderCreated of(UUID orderId, String customerId, List<OrderItem> items) {
        BigDecimal total = items.stream()
                .map(OrderItem::lineTotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        return new OrderCreated(orderId, customerId, items, total);
    }
}
