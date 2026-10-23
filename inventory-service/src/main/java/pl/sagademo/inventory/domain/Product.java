package pl.sagademo.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "products")
public class Product {

    @Id
    @Column(name = "product_id")
    private String productId;

    @Column(nullable = false)
    private int stock;

    protected Product() {
    }

    public Product(String productId, int stock) {
        this.productId = productId;
        this.stock = stock;
    }

    public boolean canFulfill(int quantity) {
        return stock >= quantity;
    }

    public void decrease(int quantity) {
        if (!canFulfill(quantity)) {
            throw new IllegalStateException("Insufficient stock for " + productId);
        }
        stock -= quantity;
    }

    public String getProductId() {
        return productId;
    }

    public int getStock() {
        return stock;
    }
}
