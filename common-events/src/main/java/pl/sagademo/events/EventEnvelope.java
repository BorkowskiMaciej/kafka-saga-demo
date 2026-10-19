package pl.sagademo.events;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record EventEnvelope<T>(
        UUID eventId,
        String eventType,
        Instant occurredAt,
        int version,
        UUID orderId,
        T payload) {

    public static final int CURRENT_VERSION = 1;

    public EventEnvelope {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(eventType, "eventType");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(orderId, "orderId");
        Objects.requireNonNull(payload, "payload");
        if (version < 1) {
            throw new IllegalArgumentException("version must be >= 1");
        }
    }

    public static <T> EventEnvelope<T> of(String eventType, UUID orderId, T payload) {
        return new EventEnvelope<>(UUID.randomUUID(), eventType, Instant.now(), CURRENT_VERSION, orderId, payload);
    }
}
