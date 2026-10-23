package pl.sagademo.order.service;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pl.sagademo.events.payload.CancellationReason;
import pl.sagademo.events.payload.OrderStatus;
import pl.sagademo.order.domain.Order;
import pl.sagademo.order.domain.OrderNotFoundException;
import pl.sagademo.order.domain.OrderRepository;

@Service
public class OrderStatusService {

    private static final Logger log = LoggerFactory.getLogger(OrderStatusService.class);

    private final OrderRepository orderRepository;

    public OrderStatusService(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Transactional
    public void markPaid(UUID orderId) {
        Order order = find(orderId);
        OrderStatus before = order.getStatus();
        order.markPaid();
        logTransition(before, order);
    }

    @Transactional
    public void confirm(UUID orderId) {
        Order order = find(orderId);
        OrderStatus before = order.getStatus();
        order.confirm();
        logTransition(before, order);
    }

    @Transactional
    public void cancel(UUID orderId, CancellationReason reason) {
        Order order = find(orderId);
        OrderStatus before = order.getStatus();
        order.cancel(reason);
        logTransition(before, order);
    }

    private Order find(UUID orderId) {
        return orderRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
    }

    private void logTransition(OrderStatus before, Order order) {
        log.info("Order status changed from={} to={} reason={}",
                before, order.getStatus(), order.getCancellationReason());
    }
}
