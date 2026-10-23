package pl.sagademo.order.domain;

import java.util.UUID;
import pl.sagademo.events.payload.OrderStatus;

public class InvalidOrderTransitionException extends RuntimeException {

    public InvalidOrderTransitionException(UUID orderId, OrderStatus current, String action) {
        super("Cannot " + action + " order " + orderId + " in status " + current);
    }
}
