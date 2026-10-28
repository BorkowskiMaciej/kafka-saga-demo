CREATE TABLE pending_reservations (
    order_id UUID PRIMARY KEY,
    correlation_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
