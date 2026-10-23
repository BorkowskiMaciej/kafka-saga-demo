package pl.sagademo.payment.domain;

import java.util.UUID;

public class PaymentNotFoundException extends RuntimeException {

    public PaymentNotFoundException(UUID orderId) {
        super("No payment found for order " + orderId);
    }
}
