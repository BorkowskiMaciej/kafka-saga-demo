package pl.sagademo.inventory.domain;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderLineSnapshotRepository extends JpaRepository<OrderLineSnapshot, UUID> {

    List<OrderLineSnapshot> findByOrderId(UUID orderId);
}
