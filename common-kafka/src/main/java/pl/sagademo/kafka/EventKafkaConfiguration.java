package pl.sagademo.kafka;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(OutboxProperties.class)
public class EventKafkaConfiguration {

    @Bean
    EventCodec eventCodec(JsonMapper jsonMapper) {
        return new EventCodec(jsonMapper);
    }

    @Bean
    EventPublisher eventPublisher(
            KafkaTemplate<String, String> kafkaTemplate, EventCodec codec, OutboxProperties properties) {
        return new EventPublisher(kafkaTemplate, codec, properties.sendTimeout());
    }

    @Bean
    OutboxRelay outboxRelay(
            JdbcClient jdbcClient,
            PlatformTransactionManager transactionManager,
            EventPublisher eventPublisher,
            OutboxProperties properties) {
        return new OutboxRelay(jdbcClient, new TransactionTemplate(transactionManager), eventPublisher, properties);
    }

    @Bean
    OutboxWriter outboxWriter(JdbcClient jdbcClient, EventCodec codec, OutboxRelay outboxRelay) {
        return new OutboxWriter(jdbcClient, codec, outboxRelay::wake);
    }

    @Bean
    IdempotentExecutor idempotentExecutor(JdbcClient jdbcClient, PlatformTransactionManager transactionManager) {
        return new IdempotentExecutor(jdbcClient, new TransactionTemplate(transactionManager));
    }
}
