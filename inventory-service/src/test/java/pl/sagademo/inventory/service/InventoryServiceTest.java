package pl.sagademo.inventory.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import pl.sagademo.events.EventEnvelope;
import pl.sagademo.events.EventTypes;
import pl.sagademo.events.Topics;
import pl.sagademo.events.payload.OrderCreated;
import pl.sagademo.events.payload.OrderItem;
import pl.sagademo.events.payload.StockRejected;
import pl.sagademo.inventory.domain.OrderLineSnapshot;
import pl.sagademo.inventory.domain.OrderLineSnapshotRepository;
import pl.sagademo.inventory.domain.OrderProcessingLockRepository;
import pl.sagademo.inventory.domain.PendingReservation;
import pl.sagademo.inventory.domain.PendingReservationRepository;
import pl.sagademo.inventory.domain.Product;
import pl.sagademo.inventory.domain.ProductRepository;
import pl.sagademo.inventory.domain.Reservation;
import pl.sagademo.inventory.domain.ReservationRepository;
import pl.sagademo.kafka.OutboxWriter;

@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    @Mock
    private ProductRepository productRepository;

    @Mock
    private OrderLineSnapshotRepository snapshotRepository;

    @Mock
    private ReservationRepository reservationRepository;

    @Mock
    private PendingReservationRepository pendingReservationRepository;

    @Mock
    private OrderProcessingLockRepository orderProcessingLockRepository;

    @Mock
    private OutboxWriter outboxWriter;

    @Captor
    private ArgumentCaptor<EventEnvelope<?>> envelopeCaptor;

    @InjectMocks
    private InventoryService inventoryService;

    @Test
    void reservesStockWhenAllItemsAreAvailable() {
        UUID orderId = UUID.randomUUID();
        Product product = new Product("p-1", 10);
        when(snapshotRepository.findByOrderId(orderId))
                .thenReturn(List.of(OrderLineSnapshot.of(orderId, "p-1", 2)));
        when(productRepository.findAllByProductIdInOrderByProductId(any())).thenReturn(List.of(product));

        inventoryService.reserveStock(orderId, "corr-1");

        assertThat(product.getStock()).isEqualTo(8);
        verify(reservationRepository).save(any(Reservation.class));
        verify(outboxWriter).enqueue(eq(Topics.STOCK_RESERVED), envelopeCaptor.capture(), eq("corr-1"));
        assertThat(envelopeCaptor.getValue().eventType()).isEqualTo(EventTypes.STOCK_RESERVED);
        assertThat(envelopeCaptor.getValue().orderId()).isEqualTo(orderId);
    }

    @Test
    void sumsQuantitiesOfRepeatedProducts() {
        UUID orderId = UUID.randomUUID();
        Product product = new Product("p-1", 10);
        when(snapshotRepository.findByOrderId(orderId)).thenReturn(List.of(
                OrderLineSnapshot.of(orderId, "p-1", 4),
                OrderLineSnapshot.of(orderId, "p-1", 3)));
        when(productRepository.findAllByProductIdInOrderByProductId(any())).thenReturn(List.of(product));

        inventoryService.reserveStock(orderId, "corr-2");

        assertThat(product.getStock()).isEqualTo(3);
    }

    @Test
    void rejectsWhenStockIsInsufficient() {
        UUID orderId = UUID.randomUUID();
        Product product = new Product("p-2", 5);
        when(snapshotRepository.findByOrderId(orderId))
                .thenReturn(List.of(OrderLineSnapshot.of(orderId, "p-2", 10)));
        when(productRepository.findAllByProductIdInOrderByProductId(any())).thenReturn(List.of(product));

        inventoryService.reserveStock(orderId, "corr-3");

        assertThat(product.getStock()).isEqualTo(5);
        verify(outboxWriter).enqueue(eq(Topics.STOCK_REJECTED), envelopeCaptor.capture(), eq("corr-3"));
        StockRejected payload = (StockRejected) envelopeCaptor.getValue().payload();
        assertThat(payload.missingProductIds()).containsExactly("p-2");
    }

    @Test
    void reservesNothingWhenAnyItemIsMissing() {
        UUID orderId = UUID.randomUUID();
        Product available = new Product("p-1", 10);
        Product soldOut = new Product("p-3", 0);
        when(snapshotRepository.findByOrderId(orderId)).thenReturn(List.of(
                OrderLineSnapshot.of(orderId, "p-1", 2),
                OrderLineSnapshot.of(orderId, "p-3", 1)));
        when(productRepository.findAllByProductIdInOrderByProductId(any()))
                .thenReturn(List.of(available, soldOut));

        inventoryService.reserveStock(orderId, "corr-4");

        assertThat(available.getStock()).isEqualTo(10);
        assertThat(soldOut.getStock()).isZero();
        verify(outboxWriter).enqueue(eq(Topics.STOCK_REJECTED), envelopeCaptor.capture(), eq("corr-4"));
        StockRejected payload = (StockRejected) envelopeCaptor.getValue().payload();
        assertThat(payload.missingProductIds()).containsExactly("p-3");
    }

    @Test
    void rejectsUnknownProduct() {
        UUID orderId = UUID.randomUUID();
        when(snapshotRepository.findByOrderId(orderId))
                .thenReturn(List.of(OrderLineSnapshot.of(orderId, "p-9", 1)));
        when(productRepository.findAllByProductIdInOrderByProductId(any())).thenReturn(List.of());

        inventoryService.reserveStock(orderId, "corr-5");

        verify(outboxWriter).enqueue(eq(Topics.STOCK_REJECTED), envelopeCaptor.capture(), eq("corr-5"));
        StockRejected payload = (StockRejected) envelopeCaptor.getValue().payload();
        assertThat(payload.missingProductIds()).containsExactly("p-9");
    }

    @Test
    void defersReservationWhenOrderLinesAreNotStoredYet() {
        UUID orderId = UUID.randomUUID();
        when(snapshotRepository.findByOrderId(orderId)).thenReturn(List.of());

        inventoryService.reserveStock(orderId, "corr-6");

        verify(pendingReservationRepository).save(any(PendingReservation.class));
    }

    @Test
    void completesDeferredReservationWhenOrderCreatedArrives() {
        UUID orderId = UUID.randomUUID();
        PendingReservation pending = PendingReservation.of(orderId, "corr-deferred");
        Product product = new Product("p-1", 10);
        when(pendingReservationRepository.findById(orderId)).thenReturn(Optional.of(pending));
        when(snapshotRepository.findByOrderId(orderId))
                .thenReturn(List.of(OrderLineSnapshot.of(orderId, "p-1", 2)));
        when(productRepository.findAllByProductIdInOrderByProductId(any())).thenReturn(List.of(product));
        OrderCreated order = OrderCreated.of(
                orderId,
                "customer-1",
                List.of(new OrderItem("p-1", 2, new java.math.BigDecimal("10.00"))));

        inventoryService.recordOrder(order, "corr-order-created");

        assertThat(product.getStock()).isEqualTo(8);
        verify(outboxWriter).enqueue(eq(Topics.STOCK_RESERVED), any(), eq("corr-deferred"));
        verify(pendingReservationRepository).delete(pending);
    }
}
