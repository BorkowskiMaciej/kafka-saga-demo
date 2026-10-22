package pl.sagademo.payment.service;

import java.math.BigDecimal;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "payment")
public record PaymentProperties(
        @DefaultValue("5000.00") BigDecimal limit,
        @DefaultValue("13.13") BigDecimal declinedAmount) {
}
