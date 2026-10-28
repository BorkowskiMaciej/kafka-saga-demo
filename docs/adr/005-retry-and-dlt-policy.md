# ADR 005: Separate retryable failures from dead-letter handling

- Status: Accepted
- Date: 2026-10-26

## Context

Some failures are temporary, while others cannot succeed without changing the message or application. Retrying malformed JSON wastes resources, but immediately discarding a temporary database or processing failure reduces resilience.

## Decision

Use Spring Kafka retry topics with four total delivery attempts and exponential backoff. `EventDeserializationException` is excluded from retry and routed directly to a service-specific DLT. Messages that exhaust retry attempts also go to DLT.

Every listener has a DLT handler that logs:

- correlation ID,
- DLT and original topic,
- original partition and offset,
- exception type and reason.

## Consequences

- Transient failures do not block the original topic partition during backoff.
- Invalid messages are isolated quickly.
- Retry and DLT topics increase topic count and operational overhead.
- Moving a message to DLT prevents endless retries but requires monitoring and a controlled replay process.
- Side effects must remain idempotent because every retry is another delivery.

## Alternatives considered

- **Blocking retries in the listener thread:** fewer topics, but delays later records from the same partition.
- **Retry every exception:** wastes time on malformed or incompatible messages.
- **Drop failed messages:** operationally simple but loses evidence and prevents recovery.
- **Infinite retries:** avoids DLT but can create poison-message loops and unbounded backlog.
