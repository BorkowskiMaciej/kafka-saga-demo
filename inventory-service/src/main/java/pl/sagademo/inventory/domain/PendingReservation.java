package pl.sagademo.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "pending_reservations")
public class PendingReservation {

    @Id
    @Column(name = "order_id")
    private UUID orderId;

    @Column(name = "correlation_id", nullable = false)
    private String correlationId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected PendingReservation() {
    }

    public static PendingReservation of(UUID orderId, String correlationId) {
        PendingReservation pending = new PendingReservation();
        pending.orderId = orderId;
        pending.correlationId = correlationId;
        pending.createdAt = Instant.now();
        return pending;
    }

    public String getCorrelationId() {
        return correlationId;
    }
}
