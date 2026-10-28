package pl.sagademo.inventory.domain;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PendingReservationRepository extends JpaRepository<PendingReservation, UUID> {
}
