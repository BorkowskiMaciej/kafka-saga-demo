package pl.sagademo.kafka;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "outbox")
public record OutboxProperties(
        @DefaultValue("200ms") Duration pollInterval,
        @DefaultValue("100") int batchSize,
        @DefaultValue("1h") Duration retention,
        @DefaultValue("7d") Duration processedEventsRetention,
        @DefaultValue("5s") Duration sendTimeout) {
}
