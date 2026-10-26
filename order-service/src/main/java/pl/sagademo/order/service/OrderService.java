package pl.sagademo.order.service;

import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.EventTypes;
import pl.sagademo.events.Topics;
import pl.sagademo.events.payload.OrderCreated;
import pl.sagademo.events.payload.OrderItem;
import pl.sagademo.kafka.OutboxWriter;
import pl.sagademo.order.api.CreateOrderRequest;
import pl.sagademo.order.api.OrderResponse;
import pl.sagademo.order.domain.Order;
import pl.sagademo.order.domain.OrderNotFoundException;
import pl.sagademo.order.domain.OrderRepository;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OutboxWriter outboxWriter;

    public OrderService(OrderRepository orderRepository, OutboxWriter outboxWriter) {
        this.orderRepository = orderRepository;
        this.outboxWriter = outboxWriter;
    }

    @Transactional
    public OrderResponse create(CreateOrderRequest request) {
        UUID orderId = UUID.randomUUID();
        List<OrderItem> items = request.items().stream()
                .map(item -> new OrderItem(item.productId(), item.quantity(), item.unitPrice()))
                .toList();
        OrderCreated payload = OrderCreated.of(orderId, request.customerId(), items);

        Order order = orderRepository.save(Order.create(orderId, request.customerId(), items, payload.totalAmount()));

        String correlationId = UUID.randomUUID().toString();
        outboxWriter.enqueue(
                Topics.ORDER_CREATED,
                EventEnvelope.of(EventTypes.ORDER_CREATED, orderId, payload),
                correlationId);

        try (MDC.MDCCloseable ignored = MDC.putCloseable("orderId", orderId.toString())) {
            log.info("Order created totalAmount={} correlationId={}", order.getTotalAmount(), correlationId);
        }
        return OrderResponse.from(order);
    }

    @Transactional(readOnly = true)
    public OrderResponse get(UUID id) {
        return orderRepository.findWithItemsById(id)
                .map(OrderResponse::from)
                .orElseThrow(() -> new OrderNotFoundException(id));
    }
}
