package pl.sagademo.order.domain;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import pl.sagademo.events.payload.CancellationReason;
import pl.sagademo.events.payload.OrderItem;
import pl.sagademo.events.payload.OrderStatus;

@Entity
@Table(name = "orders")
public class Order {

    @Id
    private UUID id;

    @Column(name = "customer_id", nullable = false)
    private String customerId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OrderStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "cancellation_reason")
    private CancellationReason cancellationReason;

    @Column(name = "total_amount", nullable = false, precision = 19, scale = 2)
    private BigDecimal totalAmount;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true)
    @JoinColumn(name = "order_id", nullable = false)
    private List<OrderLine> items = new ArrayList<>();

    protected Order() {
    }

    public static Order create(UUID id, String customerId, List<OrderItem> items, BigDecimal totalAmount) {
        Order order = new Order();
        order.id = id;
        order.customerId = customerId;
        order.status = OrderStatus.CREATED;
        order.totalAmount = totalAmount;
        order.createdAt = Instant.now();
        items.forEach(item -> order.items.add(new OrderLine(item.productId(), item.quantity(), item.unitPrice())));
        return order;
    }

    public UUID getId() {
        return id;
    }

    public String getCustomerId() {
        return customerId;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public CancellationReason getCancellationReason() {
        return cancellationReason;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public List<OrderLine> getItems() {
        return Collections.unmodifiableList(items);
    }
}
