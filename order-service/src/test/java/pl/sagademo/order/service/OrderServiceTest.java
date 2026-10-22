package pl.sagademo.order.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
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
import pl.sagademo.events.payload.OrderStatus;
import pl.sagademo.kafka.EventPublisher;
import pl.sagademo.order.api.CreateOrderRequest;
import pl.sagademo.order.api.OrderResponse;
import pl.sagademo.order.domain.Order;
import pl.sagademo.order.domain.OrderNotFoundException;
import pl.sagademo.order.domain.OrderRepository;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private EventPublisher eventPublisher;

    @Captor
    private ArgumentCaptor<EventEnvelope<?>> envelopeCaptor;

    @InjectMocks
    private OrderService orderService;

    @Test
    void savesOrderAsCreatedAndPublishesOrderCreated() {
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> invocation.getArgument(0));
        CreateOrderRequest request = new CreateOrderRequest(
                "customer-1",
                List.of(
                        new CreateOrderRequest.Item("p-1", 2, new BigDecimal("50.00")),
                        new CreateOrderRequest.Item("p-2", 1, new BigDecimal("0.50"))));

        OrderResponse response = orderService.create(request);

        assertThat(response.status()).isEqualTo(OrderStatus.CREATED);
        assertThat(response.totalAmount()).isEqualByComparingTo("100.50");
        assertThat(response.items()).hasSize(2);

        verify(eventPublisher).publish(eq(Topics.ORDER_CREATED), envelopeCaptor.capture(), anyString());
        EventEnvelope<?> envelope = envelopeCaptor.getValue();
        assertThat(envelope.eventType()).isEqualTo(EventTypes.ORDER_CREATED);
        assertThat(envelope.orderId()).isEqualTo(response.id());
        OrderCreated payload = (OrderCreated) envelope.payload();
        assertThat(payload.customerId()).isEqualTo("customer-1");
        assertThat(payload.totalAmount()).isEqualByComparingTo("100.50");
    }

    @Test
    void throwsWhenOrderDoesNotExist() {
        UUID id = UUID.randomUUID();
        when(orderRepository.findWithItemsById(id)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orderService.get(id)).isInstanceOf(OrderNotFoundException.class);
    }
}
