# ADR 002: Publish events through a transactional outbox

- Status: Accepted
- Date: 2026-10-26

## Context

A service must update PostgreSQL and publish a Kafka event as one logical operation. A direct database write followed by `KafkaTemplate.send` creates a dual-write window where either side can succeed alone.

## Decision

Store the domain change and an outbox row in the same database transaction. A background relay claims pending rows, publishes them outside the database transaction and then marks successful rows as published. Expired claims can be recovered by another relay instance.

## Consequences

- A committed domain change always has a durable event waiting for publication.
- Kafka outages do not block accepting new business operations.
- Publishing is asynchronous and requires outbox monitoring and cleanup.
- A crash after the Kafka acknowledgement but before marking the row can produce a duplicate.
- Claiming adds database state and operational complexity but avoids holding a transaction while waiting for Kafka.

## Alternatives considered

- **Direct publish after commit:** less code, but can permanently lose an event.
- **Kafka transaction plus database transaction:** does not create one atomic transaction across PostgreSQL and Kafka.
- **Change Data Capture with Debezium:** a strong production alternative, but introduces additional infrastructure beyond the scope of this project.
