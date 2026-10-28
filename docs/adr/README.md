# Architecture Decision Records

ADRs document the important design choices made in this learning project. They describe why a decision was made, its consequences and the alternatives that were considered.

| ADR | Decision |
|---|---|
| [001](001-choreography-instead-of-orchestration.md) | Use saga choreography instead of a central orchestrator |
| [002](002-transactional-outbox.md) | Publish events through a transactional outbox |
| [003](003-at-least-once-and-idempotent-consumers.md) | Accept at-least-once delivery and make consumers idempotent |
| [004](004-event-key-and-partition-ordering.md) | Partition by `orderId` and handle cross-topic reordering explicitly |
| [005](005-retry-and-dlt-policy.md) | Separate transient retries from dead-letter handling |
