CREATE TABLE products (
    product_id VARCHAR(100) PRIMARY KEY,
    stock INTEGER NOT NULL CHECK (stock >= 0)
);

CREATE TABLE order_lines (
    id UUID PRIMARY KEY,
    order_id UUID NOT NULL,
    product_id VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0)
);

CREATE INDEX idx_order_lines_order_id ON order_lines (order_id);

CREATE TABLE reservations (
    order_id UUID PRIMARY KEY,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

INSERT INTO products (product_id, stock) VALUES
    ('p-1', 100),
    ('p-2', 5),
    ('p-3', 0);
