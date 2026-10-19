package pl.sagademo.events.payload;

import java.util.List;
import java.util.UUID;

public record StockRejected(UUID orderId, List<String> missingProductIds) {

    public StockRejected {
        missingProductIds = List.copyOf(missingProductIds);
    }
}
