package pl.sagademo.events.payload;

import java.math.BigDecimal;
import java.util.UUID;

public record PaymentRefunded(UUID orderId, UUID paymentId, BigDecimal amount) {
}
