package pl.sagademo.payment.service;

import pl.sagademo.events.payload.PaymentFailureReason;

public record PaymentDecision(boolean approved, PaymentFailureReason reason) {

    public static PaymentDecision approve() {
        return new PaymentDecision(true, null);
    }

    public static PaymentDecision reject(PaymentFailureReason reason) {
        return new PaymentDecision(false, reason);
    }
}
