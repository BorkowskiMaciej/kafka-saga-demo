package pl.sagademo.inventory.service;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.EventTypes;
import pl.sagademo.events.Topics;
import pl.sagademo.events.payload.OrderCreated;
import pl.sagademo.events.payload.StockRejected;
import pl.sagademo.events.payload.StockReserved;
import pl.sagademo.inventory.domain.OrderLineSnapshot;
import pl.sagademo.inventory.domain.OrderLineSnapshotRepository;
import pl.sagademo.inventory.domain.Product;
import pl.sagademo.inventory.domain.ProductRepository;
import pl.sagademo.inventory.domain.Reservation;
import pl.sagademo.inventory.domain.ReservationRepository;
import pl.sagademo.kafka.EventPublisher;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final ProductRepository productRepository;
    private final OrderLineSnapshotRepository snapshotRepository;
    private final ReservationRepository reservationRepository;
    private final EventPublisher eventPublisher;

    public InventoryService(
            ProductRepository productRepository,
            OrderLineSnapshotRepository snapshotRepository,
            ReservationRepository reservationRepository,
            EventPublisher eventPublisher) {
        this.productRepository = productRepository;
        this.snapshotRepository = snapshotRepository;
        this.reservationRepository = reservationRepository;
        this.eventPublisher = eventPublisher;
    }

    @Transactional
    public void recordOrder(OrderCreated order) {
        List<OrderLineSnapshot> lines = order.items().stream()
                .map(item -> OrderLineSnapshot.of(order.orderId(), item.productId(), item.quantity()))
                .toList();
        snapshotRepository.saveAll(lines);
        log.info("Order lines stored count={}", lines.size());
    }

    @Transactional
    public void reserveStock(UUID orderId, String correlationId) {
        List<OrderLineSnapshot> lines = snapshotRepository.findByOrderId(orderId);
        if (lines.isEmpty()) {
            throw new OrderNotKnownException(orderId);
        }

        Map<String, Integer> required = lines.stream().collect(Collectors.toMap(
                OrderLineSnapshot::getProductId,
                OrderLineSnapshot::getQuantity,
                Integer::sum,
                TreeMap::new));
        Map<String, Product> products = productRepository.findAllByProductIdInOrderByProductId(required.keySet())
                .stream()
                .collect(Collectors.toMap(Product::getProductId, Function.identity()));

        List<String> missing = required.entrySet().stream()
                .filter(entry -> !canFulfill(products.get(entry.getKey()), entry.getValue()))
                .map(Map.Entry::getKey)
                .toList();

        if (missing.isEmpty()) {
            required.forEach((productId, quantity) -> products.get(productId).decrease(quantity));
            reservationRepository.save(Reservation.reserved(orderId));
            eventPublisher.publish(
                    Topics.STOCK_RESERVED,
                    EventEnvelope.of(EventTypes.STOCK_RESERVED, orderId, new StockReserved(orderId)),
                    correlationId);
            log.info("Stock reserved");
            return;
        }

        reservationRepository.save(Reservation.rejected(orderId));
        eventPublisher.publish(
                Topics.STOCK_REJECTED,
                EventEnvelope.of(EventTypes.STOCK_REJECTED, orderId, new StockRejected(orderId, missing)),
                correlationId);
        log.info("Stock rejected missing={}", missing);
    }

    private static boolean canFulfill(Product product, int quantity) {
        return product != null && product.canFulfill(quantity);
    }
}
