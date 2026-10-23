package pl.sagademo.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "reservations")
public class Reservation {

    @Id
    @Column(name = "order_id")
    private UUID orderId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private ReservationStatus status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Reservation() {
    }

    public static Reservation reserved(UUID orderId) {
        return create(orderId, ReservationStatus.RESERVED);
    }

    public static Reservation rejected(UUID orderId) {
        return create(orderId, ReservationStatus.REJECTED);
    }

    private static Reservation create(UUID orderId, ReservationStatus status) {
        Reservation reservation = new Reservation();
        reservation.orderId = orderId;
        reservation.status = status;
        reservation.createdAt = Instant.now();
        return reservation;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public ReservationStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
