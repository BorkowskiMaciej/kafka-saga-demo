package pl.sagademo.payment.service;

import java.math.BigDecimal;
import org.springframework.stereotype.Component;
import pl.sagademo.events.payload.PaymentFailureReason;

@Component
public class PaymentRules {

    private final PaymentProperties properties;

    public PaymentRules(PaymentProperties properties) {
        this.properties = properties;
    }

    public PaymentDecision evaluate(BigDecimal amount) {
        if (amount.compareTo(properties.limit()) > 0) {
            return PaymentDecision.reject(PaymentFailureReason.LIMIT_EXCEEDED);
        }
        if (amount.compareTo(properties.declinedAmount()) == 0) {
            return PaymentDecision.reject(PaymentFailureReason.CARD_DECLINED);
        }
        return PaymentDecision.approve();
    }
}
