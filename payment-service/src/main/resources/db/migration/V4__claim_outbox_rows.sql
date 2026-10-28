ALTER TABLE outbox ADD COLUMN claimed_by UUID;
ALTER TABLE outbox ADD COLUMN claimed_at TIMESTAMPTZ;

CREATE INDEX idx_outbox_claimable ON outbox (seq, claimed_at) WHERE published_at IS NULL;
