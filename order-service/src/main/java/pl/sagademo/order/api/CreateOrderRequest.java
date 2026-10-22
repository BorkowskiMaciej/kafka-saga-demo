package pl.sagademo.order.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.math.BigDecimal;
import java.util.List;

public record CreateOrderRequest(
        @NotBlank String customerId,
        @NotEmpty @Valid List<Item> items) {

    public record Item(
            @NotBlank String productId,
            @Positive int quantity,
            @NotNull @Positive @Digits(integer = 17, fraction = 2) BigDecimal unitPrice) {
    }
}
