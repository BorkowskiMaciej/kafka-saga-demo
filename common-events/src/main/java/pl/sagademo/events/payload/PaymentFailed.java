package pl.sagademo.events.payload;

import java.util.UUID;

public record PaymentFailed(UUID orderId, PaymentFailureReason reason) {
}
