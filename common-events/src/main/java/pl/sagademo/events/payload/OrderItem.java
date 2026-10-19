package pl.sagademo.events.payload;

import java.math.BigDecimal;

public record OrderItem(
        String productId,
        int quantity,
        BigDecimal unitPrice
) {

    public OrderItem {
        if (productId == null || productId.isBlank()) {
            throw new IllegalArgumentException("productId must not be blank");
        }
        if (quantity <= 0) {
            throw new IllegalArgumentException("quantity must be > 0");
        }
        if (unitPrice == null || unitPrice.signum() <= 0) {
            throw new IllegalArgumentException("unitPrice must be > 0");
        }
    }

    public BigDecimal lineTotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}
