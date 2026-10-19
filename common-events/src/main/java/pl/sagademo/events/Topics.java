package pl.sagademo.events;

import java.util.List;

public final class Topics {

    public static final int PARTITIONS = 3;

    public static final String ORDER_CREATED = "orders.created";
    public static final String PAYMENT_COMPLETED = "payments.completed";
    public static final String PAYMENT_FAILED = "payments.failed";
    public static final String PAYMENT_REFUNDED = "payments.refunded";
    public static final String STOCK_RESERVED = "inventory.reserved";
    public static final String STOCK_REJECTED = "inventory.rejected";

    public static final List<String> ALL = List.of(
            ORDER_CREATED,
            PAYMENT_COMPLETED,
            PAYMENT_FAILED,
            PAYMENT_REFUNDED,
            STOCK_RESERVED,
            STOCK_REJECTED);

    private Topics() {
    }
}
