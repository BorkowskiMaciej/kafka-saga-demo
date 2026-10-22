package pl.sagademo.order.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import pl.sagademo.events.payload.CancellationReason;
import pl.sagademo.events.payload.OrderStatus;
import pl.sagademo.order.domain.Order;

public record OrderResponse(
        UUID id,
        String customerId,
        OrderStatus status,
        CancellationReason cancellationReason,
        BigDecimal totalAmount,
        Instant createdAt,
        List<Line> items) {

    public record Line(String productId, int quantity, BigDecimal unitPrice) {
    }

    public static OrderResponse from(Order order) {
        List<Line> lines = order.getItems().stream()
                .map(item -> new Line(item.getProductId(), item.getQuantity(), item.getUnitPrice()))
                .toList();
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getStatus(),
                order.getCancellationReason(),
                order.getTotalAmount(),
                order.getCreatedAt(),
                lines);
    }
}
