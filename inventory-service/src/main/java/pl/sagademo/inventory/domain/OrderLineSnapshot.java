package pl.sagademo.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;

@Entity
@Table(name = "order_lines")
public class OrderLineSnapshot {

    @Id
    private UUID id;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "product_id", nullable = false)
    private String productId;

    @Column(nullable = false)
    private int quantity;

    protected OrderLineSnapshot() {
    }

    public static OrderLineSnapshot of(UUID orderId, String productId, int quantity) {
        OrderLineSnapshot line = new OrderLineSnapshot();
        line.id = UUID.randomUUID();
        line.orderId = orderId;
        line.productId = productId;
        line.quantity = quantity;
        return line;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getProductId() {
        return productId;
    }

    public int getQuantity() {
        return quantity;
    }
}
