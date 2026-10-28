# ADR 004: Partition by order ID and handle cross-topic ordering explicitly

- Status: Accepted
- Date: 2026-10-22

## Context

Events belonging to one order should be processed consistently while consumers scale across partitions. Kafka preserves order only inside one partition of one topic. It does not order `OrderCreated`, `PaymentCompleted` and `StockReserved` across their different topics.

## Decision

Use `orderId` as the Kafka record key for every saga event. This keeps events for one order in the same partition within each topic. Do not rely on ordering across topics:

- Inventory stores a pending reservation when `PaymentCompleted` arrives before its `OrderCreated` snapshot.
- Order state transitions accept causally valid `StockReserved` before the local `PaymentCompleted` projection.
- Database locks protect concurrent updates for the same order and product rows.

## Consequences

- Consumer groups can scale while preserving per-order ordering within a topic.
- Different orders are distributed across available partitions.
- A hot order key cannot be processed in parallel within one topic partition.
- Cross-topic dependencies require explicit pending state or order-independent transitions.
- Increasing the partition count changes key-to-partition mapping for future records.

## Alternatives considered

- **No message key:** improves random distribution but loses per-order ordering.
- **One topic for all saga events:** makes ordering easier, but couples unrelated consumers and retention policies.
- **Rely on retries for cross-topic races:** simpler initially, but correctness would depend on timing and retry duration.
