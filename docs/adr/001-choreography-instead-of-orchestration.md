# ADR 001: Use saga choreography instead of orchestration

- Status: Accepted
- Date: 2026-10-22

## Context

Order processing spans Order, Payment and Inventory services. A failure in a later step may require cancellation or a payment refund. The project needs to demonstrate asynchronous service collaboration through Kafka.

## Decision

Use a choreography-based saga. Each service reacts to domain events and publishes the next event without a central coordinator. Compensation is also event-driven: `StockRejected` cancels the order and triggers `PaymentRefunded`.

## Consequences

- Services remain independently deployable and do not depend on a central saga runtime.
- The event flow is easy to observe directly in Kafka.
- Workflow state is distributed, so tracing and reasoning about all possible paths is harder.
- Consumers must handle duplicates, delayed messages and events arriving in a different cross-topic order.

## Alternatives considered

- **Central saga orchestrator:** clearer workflow state and transitions, but adds a coordinator and hides some Kafka collaboration concerns this project is intended to explore.
- **Synchronous REST calls:** simpler control flow, but tighter runtime coupling and weaker outage tolerance.
