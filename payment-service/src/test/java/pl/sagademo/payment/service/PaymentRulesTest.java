package pl.sagademo.payment.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import pl.sagademo.events.payload.PaymentFailureReason;

class PaymentRulesTest {

    private final PaymentRules rules =
            new PaymentRules(new PaymentProperties(new BigDecimal("5000.00"), new BigDecimal("13.13")));

    @ParameterizedTest
    @CsvSource({
            "100.00",
            "5000.00",
            "13.12",
            "13.14",
            "0.01"
    })
    void approvesAmountsWithinLimit(String amount) {
        PaymentDecision decision = rules.evaluate(new BigDecimal(amount));

        assertThat(decision.approved()).isTrue();
        assertThat(decision.reason()).isNull();
    }

    @ParameterizedTest
    @CsvSource({
            "5000.01, LIMIT_EXCEEDED",
            "6000.00, LIMIT_EXCEEDED",
            "13.13, CARD_DECLINED",
            "13.130, CARD_DECLINED"
    })
    void rejectsAmountsBreakingRules(String amount, PaymentFailureReason expectedReason) {
        PaymentDecision decision = rules.evaluate(new BigDecimal(amount));

        assertThat(decision.approved()).isFalse();
        assertThat(decision.reason()).isEqualTo(expectedReason);
    }
}
