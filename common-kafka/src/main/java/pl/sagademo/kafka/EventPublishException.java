package pl.sagademo.kafka;

import java.util.UUID;

public class EventPublishException extends RuntimeException {

    public EventPublishException(String topic, UUID eventId, Throwable cause) {
        super("Failed to publish event " + eventId + " to topic " + topic, cause);
    }
}
