package pl.sagademo.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;
import pl.sagademo.kafka.EventKafkaConfiguration;
import pl.sagademo.payment.service.PaymentProperties;

@SpringBootApplication
@Import(EventKafkaConfiguration.class)
@EnableConfigurationProperties(PaymentProperties.class)
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
