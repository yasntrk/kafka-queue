# Event Catalog & Routing

This is the contract every microservice in the Fish Auction System codes against.
**Design choice:** one Kafka topic per event type. The topic name *is* the event
type, so routing is unambiguous — a service simply subscribes to the topics it
cares about.

- **Serialization:** JSON (UTF-8), one event per message.
- **Message key:** `sessionId` for auction events (guarantees per-session
  ordering, since one key always maps to one partition), or the entity id for
  registration events.
- **Headers:** `content-type: application/json`, `eventType: <topic>`.
- **Common fields:** every event carries `eventId` (UUID) and `occurredAt`
  (ISO-8601). Each schema lives in [`/schemas`](../schemas).

## Services

| # | Service | Role |
|---|---------|------|
| 0 | **Queue** (this repo) | Kafka messaging backbone connecting all services |
| 1 | User & Membership | Member/buyer registration, login, buyer verification |
| 2 | Pre-Auction Catalog | Product classification, baskets/lots, base price, catalog |
| 3 | Auction & Bidding | Live session, bidding, sold/unsold, re-auction |
| 4 | Post-Auction & Fulfillment | Sales records, pickup/delivery, captain payments, closing |
| 5 | Notification | Registration / auction / winner / pickup / payment messages |

## Routing table

| Topic (event type) | Producer | Consumers | Key |
|--------------------|----------|-----------|-----|
| `user.member.registered` | 1 | 5 | `memberId` |
| `user.buyer.registered` | 1 | 3, 5 | `buyerId` |
| `catalog.basket.created` | 2 | 3 | `basketId` |
| `catalog.published` | 2 | 3, 5 | `sessionId` |
| `auction.session.started` | 3 | 4, 5 | `sessionId` |
| `auction.basket.opened` | 3 | 5 (UI/buyers) | `sessionId` |
| `bid.placed` | 3 | 3 | `sessionId` |
| `bid.basket.sold` | 3 | 4, 5 | `sessionId` |
| `bid.basket.unsold` | 3 | 3 (re-auction), 4 | `sessionId` |
| `bid.rebid.round.opened` | 3 | 5 (UI/buyers) | `sessionId` |
| `bid.all.baskets.finalized` | 3 | 4 | `sessionId` |
| `bid.payment.confirmed` | 4 | 5 | `sessionId` |
| `bid.basket.sale.completed` | 4 | 4, 5 | `sessionId` |
| `fulfillment.pickup.scheduled` | 4 | 5 | `sessionId` |
| `fulfillment.captain.payment.calculated` | 4 | 5 | `sessionId` |
| `auction.closed` | 4 | 5 | `sessionId` |
| `notification.sent` | 5 | (audit) | `eventId` |

## Message flow (happy path)

```
User(1)      user.member.registered ─────────────────────────► Notification(5)
             user.buyer.registered ──────────────► Auction(3), Notification(5)

Catalog(2)   catalog.basket.created ─────────────────────────► Auction(3)
             catalog.published ─────────────────► Auction(3), Notification(5)

Auction(3)   auction.session.started ────────────► Fulfillment(4), Notification(5)
             auction.basket.opened
             bid.placed
             bid.basket.sold ──────────────────► Fulfillment(4), Notification(5)
             bid.basket.unsold ────────────────► Auction(3) re-auction queue
             bid.rebid.round.opened ───────────► Notification(5)
             bid.all.baskets.finalized ────────► Fulfillment(4)

Fulfillment(4) bid.payment.confirmed ───────────► Notification(5)
               bid.basket.sale.completed ───────► Notification(5)
               fulfillment.pickup.scheduled ────► Notification(5)
               fulfillment.captain.payment.calculated ─► Notification(5)
               auction.closed ──────────────────► Notification(5)

Notification(5) notification.sent (audit log)
```

## Consumer groups

Each microservice uses **one consumer group** (e.g. `notification-service`,
`fulfillment-service`). Because every group keeps its own offsets, every service
receives its own independent copy of the events it subscribes to. Scaling a
service to N instances under the same group id automatically spreads its topic
partitions across those instances.
