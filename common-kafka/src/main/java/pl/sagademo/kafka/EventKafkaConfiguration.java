package pl.sagademo.kafka;

import java.time.Duration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
public class EventKafkaConfiguration {

    @Bean
    EventCodec eventCodec(JsonMapper jsonMapper) {
        return new EventCodec(jsonMapper);
    }

    @Bean
    EventPublisher eventPublisher(KafkaTemplate<String, String> kafkaTemplate, EventCodec codec) {
        return new EventPublisher(kafkaTemplate, codec, Duration.ofSeconds(5));
    }

    @Bean
    IdempotentExecutor idempotentExecutor(JdbcClient jdbcClient, PlatformTransactionManager transactionManager) {
        return new IdempotentExecutor(jdbcClient, new TransactionTemplate(transactionManager));
    }
}
