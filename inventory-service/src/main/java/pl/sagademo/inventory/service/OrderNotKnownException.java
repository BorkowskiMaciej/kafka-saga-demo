package pl.sagademo.inventory.service;

import java.util.UUID;

public class OrderNotKnownException extends RuntimeException {

    public OrderNotKnownException(UUID orderId) {
        super("No order lines stored yet for order " + orderId);
    }
}
