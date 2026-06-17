# Fish Auction System — Event Bus (Kafka)

This is the shared **messaging backbone** for the Online Fish Auction System of
the Balıklıova Seafood Cooperative. Every microservice communicates **only**
through Kafka events defined here — no service calls another directly.

If you are on one of the other teams, **this README is your integration guide**:
how to connect, how to publish an event, how to consume events, and the full
catalog of event types with example payloads.

| # | Service | Role |
|---|---------|------|
| 0 | **Queue** (this repo) | Kafka event bus + event contract (topics & schemas) |
| 1 | User & Membership | Member/buyer registration, login, buyer verification |
| 2 | Pre-Auction Catalog | Product classification, baskets/lots, base price, catalog |
| 3 | Auction & Bidding | Live session, bidding, sold/unsold, re-auction |
| 4 | Post-Auction & Fulfillment | Sales records, pickup/delivery, captain payments, closing |
| 5 | Notification | Registration / auction / winner / pickup / payment messages |

---

## Table of contents

- [How it works in 30 seconds](#how-it-works-in-30-seconds)
- [Part 1 — Connect & integrate (all teams)](#part-1--connect--integrate-all-teams)
  - [1. Get access](#1-get-access)
  - [2. Connect](#2-connect)
  - [3. Publish an event](#3-publish-an-event)
  - [4. Consume events](#4-consume-events)
  - [Rules of the contract](#rules-of-the-contract)
  - [Other languages](#other-languages)
- [Event catalog](#event-catalog)
- [Example payloads](#example-payloads)
- [Part 2 — Run the bus yourself (local dev / self-host)](#part-2--run-the-bus-yourself-local-dev--self-host)
- [Troubleshooting](#troubleshooting)

---

## How it works in 30 seconds

- The bus is **Apache Kafka**. Each **event type is its own topic**, e.g.
  `bid.basket.sold`. The topic name *is* the event type.
- A service **publishes** (produces) an event to a topic; any number of services
  **subscribe** (consume) to the topics they care about. Producers and consumers
  never block or call each other.
- Messages are **JSON**, validated against a **JSON Schema** (see
  [`schemas/`](schemas)) — that schema is the contract between teams.
- The message **key** is the `sessionId`, so all events of one auction session
  stay in order.

```
  Auction(3) ── bid.basket.sold ──►  topic ──►  Fulfillment(4)  (records sale)
                                            └─►  Notification(5) (tells the winner)
```

---

## Part 1 — Connect & integrate (all teams)

### 1. Get access

Ask the **Queue team** for:

1. **Bootstrap server** — e.g. `pkc-xxxxx.us-east-2.aws.confluent.cloud:9092`
2. **API key** and **API secret** (each team gets its own key)

The topics already exist on the shared cluster — you only need to produce and
consume.

> The shared cluster is on **Confluent Cloud** (managed Kafka), reachable from
> anywhere. It uses **SASL/SSL** auth. Keep your API secret out of git — put it
> in environment variables, not in code.

Put your credentials in a `.env` file (never commit it):

```bash
KAFKA_BROKERS=pkc-xxxxx.us-east-2.aws.confluent.cloud:9092
KAFKA_SASL_USERNAME=<your-api-key>
KAFKA_SASL_PASSWORD=<your-api-secret>
```

### 2. Connect

Examples below use **Node.js** with [`kafkajs`](https://kafka.js.org)
(`npm install kafkajs`). The same connection settings apply to every client:

```js
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'auction-service',                 // your service name
  brokers: [process.env.KAFKA_BROKERS],
  ssl: true,                                   // required by the managed cluster
  sasl: {
    mechanism: 'plain',
    username: process.env.KAFKA_SASL_USERNAME,
    password: process.env.KAFKA_SASL_PASSWORD,
  },
});
```

### 3. Publish an event

Publish to the topic that matches your event type. Use the `sessionId` as the
message key to keep a session's events ordered.

```js
const { randomUUID } = require('crypto');

const producer = kafka.producer();
await producer.connect();

await producer.send({
  topic: 'bid.basket.sold',
  messages: [
    {
      key: 'session-2026-06-18',                       // = sessionId
      value: JSON.stringify({
        eventId: randomUUID(),
        sessionId: 'session-2026-06-18',
        basketId: 'basket-1',
        buyerId: 'buyer-7',
        winningBidId: 'bid-42',
        salePrice: 110,
        occurredAt: new Date().toISOString(),
      }),
    },
  ],
});

await producer.disconnect();
```

### 4. Consume events

Subscribe to the topics your service cares about. **Use your own `groupId`** (one
per service) so you get your own independent copy of every event.

```js
const consumer = kafka.consumer({ groupId: 'notification-service' });
await consumer.connect();

await consumer.subscribe({
  topics: ['bid.basket.sold', 'bid.payment.confirmed'],
  fromBeginning: true,
});

await consumer.run({
  eachMessage: async ({ topic, message }) => {
    const event = JSON.parse(message.value.toString());
    switch (topic) {
      case 'bid.basket.sold':
        notifyWinner(event.buyerId, event.basketId, event.salePrice);
        break;
      case 'bid.payment.confirmed':
        sendReceipt(event.buyerId, event.amount);
        break;
    }
  },
});
```

> **Tip:** while developing, set `fromBeginning: true` to replay all past events.
> Once your offsets are committed, the consumer resumes where it left off even
> after a restart.

### Rules of the contract

Follow these so every team interoperates cleanly:

1. **Topic = event type.** Publish each event to the topic named after it.
2. **Always include** `eventId` (a UUID) and `occurredAt` (ISO-8601 timestamp).
3. **Key = `sessionId`** for auction events (entity id for registration events).
4. **One `groupId` per service** (e.g. `auction-service`, `fulfillment-service`).
   Run multiple instances under the same group to scale out automatically.
5. **Validate before you publish.** Match the JSON Schema in [`schemas/`](schemas)
   for that topic. Unknown extra fields are rejected (`additionalProperties: false`).

### Other languages

Any Kafka client works — Java (`kafka-clients`), Python
(`confluent-kafka` / `kafka-python`), Go, .NET, etc. Use the same bootstrap
server and **SASL/PLAIN over SSL** credentials. The JSON schemas in
[`schemas/`](schemas) are the source of truth regardless of language.

If you use Node.js, you can also reuse this repo's helpers
([`src/producer.js`](src/producer.js), [`src/consumer.js`](src/consumer.js)),
which validate every event against its schema automatically.

---

## Event catalog

One topic per event type. Full message flow and details:
**[docs/EVENT-CATALOG.md](docs/EVENT-CATALOG.md)**.

| Topic (event type) | Producer | Consumers | Key |
|--------------------|:--------:|:---------:|-----|
| `user.member.registered` | 1 | 5 | `memberId` |
| `user.buyer.registered` | 1 | 3, 5 | `buyerId` |
| `catalog.basket.created` | 2 | 3 | `basketId` |
| `catalog.published` | 2 | 3, 5 | `sessionId` |
| `auction.session.started` | 3 | 4, 5 | `sessionId` |
| `auction.basket.opened` | 3 | 5 (UI) | `sessionId` |
| `bid.placed` | 3 | 3 | `sessionId` |
| `bid.basket.sold` | 3 | 4, 5 | `sessionId` |
| `bid.basket.unsold` | 3 | 3 (re-auction), 4 | `sessionId` |
| `bid.rebid.round.opened` | 3 | 5 (UI) | `sessionId` |
| `bid.all.baskets.finalized` | 3 | 4 | `sessionId` |
| `bid.payment.confirmed` | 4 | 5 | `sessionId` |
| `bid.basket.sale.completed` | 4 | 4, 5 | `sessionId` |
| `fulfillment.pickup.scheduled` | 4 | 5 | `sessionId` |
| `fulfillment.captain.payment.calculated` | 4 | 5 | `sessionId` |
| `auction.closed` | 4 | 5 | `sessionId` |
| `notification.sent` | 5 | (audit) | `eventId` |

Numbers map to the service table at the top. Every event also carries `eventId`
and `occurredAt`.

---

## Example payloads

The core bidding events. The full set of 17 schemas is in [`schemas/`](schemas).

**`bid.basket.sold`** — a basket was sold to the highest bidder
```json
{
  "eventId": "11111111-1111-1111-1111-111111111111",
  "sessionId": "session-2026-06-18",
  "basketId": "basket-1",
  "buyerId": "buyer-7",
  "winningBidId": "bid-42",
  "salePrice": 110,
  "occurredAt": "2026-06-18T07:35:00.000Z"
}
```

**`bid.basket.unsold`** — no acceptable bid this round (`reason`: `NO_BIDS` | `BID_BELOW_BASE` | `WITHDRAWN`)
```json
{
  "eventId": "22222222-2222-2222-2222-222222222222",
  "sessionId": "session-2026-06-18",
  "basketId": "basket-3",
  "reason": "NO_BIDS",
  "occurredAt": "2026-06-18T07:36:00.000Z"
}
```

**`bid.rebid.round.opened`** — a re-auction round for unsold baskets
```json
{
  "eventId": "33333333-3333-3333-3333-333333333333",
  "sessionId": "session-2026-06-18",
  "roundNumber": 2,
  "basketIds": ["basket-3"],
  "occurredAt": "2026-06-18T07:40:00.000Z"
}
```

**`bid.payment.confirmed`** — payment received for a sold basket
```json
{
  "eventId": "44444444-4444-4444-4444-444444444444",
  "sessionId": "session-2026-06-18",
  "basketId": "basket-1",
  "buyerId": "buyer-7",
  "amount": 110,
  "paymentStatus": "CONFIRMED",
  "occurredAt": "2026-06-18T07:50:00.000Z"
}
```

**`bid.basket.sale.completed`** — sold **and** paid (sale fully closed)
```json
{
  "eventId": "55555555-5555-5555-5555-555555555555",
  "sessionId": "session-2026-06-18",
  "basketId": "basket-1",
  "buyerId": "buyer-7",
  "salePrice": 110,
  "paymentConfirmed": true,
  "occurredAt": "2026-06-18T07:51:00.000Z"
}
```

**`bid.all.baskets.finalized`** — every basket reached a terminal state
```json
{
  "eventId": "66666666-6666-6666-6666-666666666666",
  "sessionId": "session-2026-06-18",
  "totalBaskets": 3,
  "soldBasketCount": 3,
  "unsoldBasketCount": 0,
  "occurredAt": "2026-06-18T07:55:00.000Z"
}
```

---

## Part 2 — Run the bus yourself (local dev / self-host)

You do **not** need this to integrate — use the shared cluster above. This part
is for running your own broker locally (e.g. to develop offline) or for the
Queue team operating the cluster.

**Prerequisites:** Docker + Docker Compose, and Node.js ≥ 18.

### Local broker with Docker

```bash
git clone https://github.com/yasntrk/kafka-queue.git
cd kafka-queue
cp .env.example .env          # KAFKA_ADVERTISED_HOST=localhost for local dev
docker compose up -d          # starts Kafka (KRaft) + Kafka UI

npm install
npm run topics:create         # create all 17 topics
open http://localhost:8080    # Kafka UI dashboard
```

Run the built-in end-to-end demo (a full simulated morning auction):

```bash
npm run demo:consume          # terminal A — prints every event with OK/BAD validation
npm run demo:produce          # terminal B — publishes the full auction flow
```

### Commands

| Command | What it does |
|---------|--------------|
| `npm run topics:create` | Create all topics from `config/topics.json` (idempotent) |
| `npm run topics:list` | List topics and partition counts |
| `npm run validate:schemas` | **Offline** — compile schemas + validate example payloads (no broker needed) |
| `npm run demo:produce` | Publish a full simulated auction session |
| `npm run demo:consume` | Subscribe to all topics and print events |

All scripts read `.env`, so they work against **either** a local broker or the
shared Confluent Cloud cluster — only the env values change.

### Use the shared managed cluster (Confluent Cloud)

Point `.env` at the managed cluster instead of localhost (SSL turns on
automatically when a SASL username is set):

```bash
KAFKA_BROKERS=pkc-xxxxx.us-east-2.aws.confluent.cloud:9092
KAFKA_SASL_MECHANISM=plain
KAFKA_SASL_USERNAME=<API_KEY>
KAFKA_SASL_PASSWORD=<API_SECRET>
KAFKA_REPLICATION_FACTOR=3       # Confluent Cloud requires RF=3 (used by topics:create)
```

**Queue-team setup (one time):** create a Basic cluster at
[confluent.cloud](https://confluent.cloud), make an API key, copy the bootstrap
server into `.env` as above, then run `npm run topics:create`. Issue a separate
API key per team so keys can be revoked individually.

### Self-host on a VM (AWS EC2 etc.)

1. Install Docker + Compose on the instance.
2. Open inbound TCP **29092** (Kafka) and **8080** (Kafka UI) in the firewall /
   security group — ideally only to known IPs.
3. Set `KAFKA_ADVERTISED_HOST` to the instance's **public IP/DNS** in `.env`
   (otherwise remote clients connect and then get redirected to an unreachable
   address), then `docker compose up -d` and `npm run topics:create`.

> The Docker setup is **PLAINTEXT (no auth)** — fine for local dev or a locked-down
> security group. For an internet-facing shared cluster, prefer Confluent Cloud
> (SASL/SSL) above.

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `SASL` / authentication error | Check `KAFKA_SASL_USERNAME` / `KAFKA_SASL_PASSWORD`; the username is the API **key**, password is the **secret**. SSL must be on (it is automatic when SASL is set). |
| `There is no leader for this topic-partition` | Topic doesn't exist yet — ask the Queue team, or run `npm run topics:create`. |
| Consumer gets nothing | New `groupId` + `fromBeginning: true` to replay; verify you subscribed to the right topic name. |
| Schema validation fails | Compare your payload to the JSON Schema in [`schemas/`](schemas); remove unknown fields (`additionalProperties: false`). |
| Local client hangs / `ECONNREFUSED` | `KAFKA_ADVERTISED_HOST` must match how clients reach the broker (localhost vs public IP). |
| `TimeoutNegativeWarning` on Node 25 | Harmless `kafkajs` log on very new Node; functionality is unaffected. Use Node 20 LTS to silence it. |

---

## Repository structure

```
kafka-queue/
├── docker-compose.yml          # Kafka (KRaft) + Kafka UI for local dev
├── .env.example                # connection settings (local & Confluent Cloud)
├── config/topics.json          # topic catalog (name, partitions, RF)
├── schemas/                    # JSON Schema per event type — the contract
├── src/
│   ├── kafka.js                # shared client (PLAINTEXT or SASL/SSL via env)
│   ├── producer.js             # publishEvent() — validates then sends
│   ├── consumer.js             # createConsumer() — subscribe + validate
│   ├── admin/                  # create-topics, list-topics, validate-schemas
│   └── demo/                   # full auction produce/consume demo
└── docs/
    ├── EVENT-CATALOG.md        # routing table + message flow
    └── REPORT.md               # SEDS 520 project report
```

## License

MIT
