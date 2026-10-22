package pl.sagademo.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.EventHeaders;

public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EventCodec codec;
    private final Duration sendTimeout;

    public EventPublisher(KafkaTemplate<String, String> kafkaTemplate, EventCodec codec, Duration sendTimeout) {
        this.kafkaTemplate = kafkaTemplate;
        this.codec = codec;
        this.sendTimeout = sendTimeout;
    }

    public void publish(String topic, EventEnvelope<?> envelope, String correlationId) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, envelope.orderId().toString(), codec.write(envelope));
        record.headers().add(EventHeaders.CORRELATION_ID, correlationId.getBytes(StandardCharsets.UTF_8));

        try {
            SendResult<String, String> result = kafkaTemplate.send(record)
                    .orTimeout(sendTimeout.toMillis(), TimeUnit.MILLISECONDS)
                    .join();
            log.info("Published eventType={} eventId={} orderId={} topic={} partition={} offset={}",
                    envelope.eventType(),
                    envelope.eventId(),
                    envelope.orderId(),
                    topic,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
        } catch (CompletionException e) {
            throw new EventPublishException(topic, envelope.eventId(), e.getCause());
        }
    }
}
