CREATE TABLE outbox (
    seq BIGSERIAL PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    event_type VARCHAR(50) NOT NULL,
    topic VARCHAR(200) NOT NULL,
    message_key VARCHAR(100) NOT NULL,
    payload TEXT NOT NULL,
    correlation_id VARCHAR(100) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_unpublished ON outbox (seq) WHERE published_at IS NULL;

CREATE INDEX idx_outbox_published_at ON outbox (published_at) WHERE published_at IS NOT NULL;
