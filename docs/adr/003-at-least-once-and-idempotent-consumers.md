# ADR 003: Use at-least-once delivery with idempotent consumers

- Status: Accepted
- Date: 2026-10-26

## Context

Kafka records and outbox rows can be delivered more than once after failures, retries or an uncertain acknowledgement. Business operations such as charging, refunding or decreasing stock must not execute twice for the same event.

## Decision

Accept at-least-once delivery. Every event has a unique `eventId`. Before applying its effect, a consumer inserts that ID into `processed_events`. The marker, business state change and optional next outbox event are committed in one local transaction.

## Consequences

- Re-delivery does not repeat the business effect for the same `eventId`.
- A failed business transaction also rolls back the processed marker, allowing a safe retry.
- `processed_events` requires retention and cleanup.
- Retention must be longer than the supported replay window; replaying an older event after its marker expires may execute it again.
- Idempotence is defined by event identity, not only by order identity.

## Alternatives considered

- **Assume exactly-once delivery:** unsafe across Kafka and an external database.
- **Use only business-table uniqueness constraints:** useful as an additional safeguard, but insufficient for all state transitions and side effects.
- **Kafka Streams exactly-once processing:** valuable for Kafka-to-Kafka stateful processing, but not an end-to-end guarantee for these PostgreSQL writes.
