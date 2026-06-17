# SEDS 520 — Microservice Project Report
## Queue Component (Microservice 0)

> Draft report mapped to the official report format. Review the **TODO** notes,
> then export to PDF for submission.

---

## 1. Service Information

- **Component name:** Queue — Messaging Backbone (Apache Kafka)
- **Team members:** Yasin, Toprak  <!-- TODO: confirm full names -->
- **Main responsibility:** Provide the asynchronous messaging infrastructure that
  connects all microservices of the Fish Auction System and routes auction-related
  events to the relevant services.
- **GitHub repository:** https://github.com/yasntrk/kafka-queue

The repository contains the source code, this README, and running instructions.

---

## 2. Service Description

The Queue component is the **central nervous system** of the Fish Auction System.
It does not contain auction business logic; instead it provides an Apache Kafka
event bus through which the other five microservices communicate **asynchronously**
and **without being directly coupled** to one another.

- **What it does:** runs a Kafka broker, defines the system's topics, owns the
  event schemas (the inter-service contract), validates event structure, and
  provides reusable producer/consumer libraries plus a runnable end-to-end demo.
- **Main role:** decouple producers from consumers. A service publishes an event
  once; any number of other services consume it independently, can be offline and
  catch up later, and can be scaled out without code changes elsewhere.
- **Interacts with:** all services (1 User, 2 Catalog, 3 Auction, 4 Fulfillment,
  5 Notification) — each is a producer, a consumer, or both.

Why messaging matters here: fish is perishable, so the auction must be fast and
resilient. Event-driven messaging lets slow steps (notifications, payment
reports, delivery checks) run in the background without blocking the live auction.

---

## 3. Requirements / User Stories

| ID | Requirement |
|----|-------------|
| R-01 | The queue component shall enable asynchronous communication between microservices. |
| R-02 | The queue component shall route auction-related events to the relevant services. |
| R-03 | The component shall define a topic for every event type in the system. |
| R-04 | The component shall publish events with a documented JSON schema and reject malformed events at publish time. |
| R-05 | The component shall preserve per-session event ordering (via the message key). |
| R-06 | The component shall let each service consume independently using its own consumer group. |
| R-07 | The component shall be deployable with a single command (Docker Compose) locally or on AWS EC2. |
| R-08 | The component shall provide a runnable producer/consumer demo of the full auction flow. |

---

## 4. API / Interface Design

This is infrastructure, so instead of REST endpoints it exposes **messaging
interfaces**:

| Interface | Purpose |
|-----------|---------|
| **Topic** | One per event type; routes that event to all subscribing services. |
| **Message key** | `sessionId` (or entity id); determines the partition, guaranteeing per-session ordering. |
| **Consumer group** | One per microservice; isolates each service's read position (offsets). |
| **Headers** | `content-type=application/json`, `eventType=<topic>`. |

Producer/consumer helper API (Node.js, `kafkajs`):

| Function | Purpose |
|----------|---------|
| `publishEvent(topic, payload)` | Validate against the topic schema, then publish. |
| `createConsumer({ groupId, topics, onEvent })` | Subscribe and receive validated events. |

Bootstrap endpoints: `localhost:29092` (local) or `<EC2_PUBLIC_IP>:29092` (remote).
Kafka UI dashboard: `http://<host>:8080`.

---

## 5. Data Model / Message Model

All 17 event types and their fields are defined as JSON Schemas in
[`/schemas`](../schemas). Examples:

| Event Name | Main Fields |
|------------|-------------|
| `bid.basket.sold` | eventId, sessionId, basketId, buyerId, winningBidId, salePrice, occurredAt |
| `bid.basket.unsold` | eventId, sessionId, basketId, reason, occurredAt |
| `bid.rebid.round.opened` | eventId, sessionId, roundNumber, basketIds[], occurredAt |
| `bid.payment.confirmed` | eventId, sessionId, basketId, buyerId, amount, paymentStatus, occurredAt |
| `bid.basket.sale.completed` | eventId, sessionId, basketId, buyerId, salePrice, paymentConfirmed, occurredAt |
| `bid.all.baskets.finalized` | eventId, sessionId, totalBaskets, soldBasketCount, unsoldBasketCount, occurredAt |

Every event additionally shares `eventId` (UUID) and `occurredAt` (ISO-8601).
The full catalog (user, catalog, auction, fulfillment, notification events) is in
[EVENT-CATALOG.md](EVENT-CATALOG.md).

---

## 6. Events and Communication

- **Message broker:** Apache Kafka 3.9 in KRaft mode (no ZooKeeper), single
  broker for the project, plus Kafka UI for inspection.
- **Topic naming:** one topic per event type, dotted names (e.g.
  `bid.basket.sold`). 17 topics total.
- **Routing:** see the routing table in [EVENT-CATALOG.md](EVENT-CATALOG.md) —
  which service produces and which services consume each topic.
- **Message flow:** registration → catalog published → live auction
  (sold/unsold/re-auction) → finalized → payment & fulfillment → captain
  payments → auction closed; notifications fan out throughout.

| Event | Produced / Consumed | Related Service | Purpose |
|-------|---------------------|-----------------|---------|
| `bid.basket.sold` | Produced by Auction(3) | Fulfillment(4), Notification(5) | Record sale, notify winner |
| `bid.basket.unsold` | Produced by Auction(3) | Auction(3) re-auction queue | Trigger re-auction round |
| `bid.payment.confirmed` | Produced by Fulfillment(4) | Notification(5) | Confirm payment, notify |

---

## 7. Implementation Summary and Team Contribution

- **Programming language / framework:** Node.js + `kafkajs`; schema validation
  with `ajv`.
- **Database:** none (the queue is stateless; Kafka persists the event log).
- **Message broker:** Apache Kafka 3.9 (KRaft), Kafka UI, orchestrated via Docker
  Compose.
- **Main implemented features:** 17 topics with JSON-Schema contracts; idempotent
  schema-validating producer; consumer helper with per-service consumer groups;
  admin scripts (`topics:create`, `topics:list`, `validate:schemas`); full
  end-to-end auction demo; deployable locally or on AWS EC2.
- **Repository structure:** `docker-compose.yml`, `config/topics.json`,
  `schemas/`, `src/` (config, kafka, producer, consumer, admin, demo), `docs/`.

| Team Member | Contribution |
|-------------|--------------|
| Yasin | <!-- TODO --> Kafka/Docker setup, producer & schema registry |
| Toprak | <!-- TODO --> Topic design, consumer helper & demo, README |

**Current status:** The messaging backbone is complete and verified — schemas
compile and the assignment's example payloads validate. The broker and demo run
with `docker compose up -d` + `npm run topics:create` + `npm run demo:produce`.
Remaining work is integration by each downstream team against the published
topic/schema contract.
