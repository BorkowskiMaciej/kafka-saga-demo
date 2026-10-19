package pl.sagademo.events;

public final class EventTypes {

    public static final String ORDER_CREATED = "OrderCreated";
    public static final String PAYMENT_COMPLETED = "PaymentCompleted";
    public static final String PAYMENT_FAILED = "PaymentFailed";
    public static final String PAYMENT_REFUNDED = "PaymentRefunded";
    public static final String STOCK_RESERVED = "StockReserved";
    public static final String STOCK_REJECTED = "StockRejected";

    private EventTypes() {
    }
}
