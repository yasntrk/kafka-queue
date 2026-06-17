# Fish Auction Queue — Messaging Backbone (Microservice 0)

Kafka-based messaging backbone for the **Online Fish Auction System** of the
Balıklıova Seafood Cooperative (SEDS 520 project).

This component is **Microservice 0 — Queue**. It does not implement auction
business logic. It provides the **event bus** that connects the other five
services and owns the **event contract** (topics + JSON schemas) they all use to
talk to each other asynchronously.

> Services in the system: **1** User & Membership · **2** Pre-Auction Catalog ·
> **3** Auction & Bidding · **4** Post-Auction & Fulfillment · **5** Notification.

---

## Architecture

```
            ┌──────────────────────────────────────────────────────────┐
            │                    Apache Kafka (KRaft)                    │
   produce  │   17 topics, one per event type  (bid.basket.sold, ...)    │  consume
 ─────────► │                                                            │ ─────────►
            │   key = sessionId  →  per-session ordering per partition   │
            └──────────────────────────────────────────────────────────┘
   Producers: User(1), Catalog(2),        Consumers (one group per service):
   Auction(3), Fulfillment(4),            Auction(3), Fulfillment(4),
   Notification(5)                        Notification(5), ...
```

- **Broker:** Apache Kafka 3.9, KRaft mode (no ZooKeeper) — single container.
- **Dashboard:** Kafka UI at `http://<host>:8080`.
- **Contract:** every event type has a JSON Schema in [`schemas/`](schemas);
  the producer validates against it before publishing.
- Full routing table and message flow: **[docs/EVENT-CATALOG.md](docs/EVENT-CATALOG.md)**.

---

## Prerequisites

- **Docker** + **Docker Compose** (to run the broker)
- **Node.js ≥ 18** (to run the admin scripts and demo clients)

---

## Quick start (local)

```bash
# 1. Configure (KAFKA_ADVERTISED_HOST=localhost for local dev)
cp .env.example .env

# 2. Start Kafka + Kafka UI
docker compose up -d

# 3. Install Node deps and create all topics
npm install
npm run topics:create

# 4. (optional) open the dashboard
open http://localhost:8080
```

### Run the end-to-end demo

In **terminal A** start the observer (subscribes to all topics):

```bash
npm run demo:consume
```

In **terminal B** publish a full simulated auction (Levrek / Dil / Kalamar):

```bash
npm run demo:produce
```

Terminal A prints every event as it flows through the system, with an `OK`/`BAD`
schema-validation mark.

### Useful commands

| Command | What it does |
|---------|--------------|
| `npm run topics:create` | Create all topics from `config/topics.json` (idempotent) |
| `npm run topics:list` | List application topics and partition counts |
| `npm run validate:schemas` | **Offline** check — compiles schemas + validates example payloads (no broker needed) |
| `npm run demo:produce` | Publish a full simulated auction session |
| `npm run demo:consume` | Subscribe to all topics and print events |
| `docker compose down` | Stop the broker (add `-v` to also wipe data) |

---

## Deploy on AWS EC2

1. **Launch** an instance (Amazon Linux 2023 / Ubuntu, t3.small or larger) and
   install Docker + Compose:
   ```bash
   sudo dnf install -y docker && sudo systemctl enable --now docker   # AL2023
   sudo usermod -aG docker $USER && newgrp docker
   ```
2. **Open the security group** for inbound TCP **29092** (Kafka) and **8080**
   (Kafka UI) from the IPs that need access.
3. **Configure the advertised host** so remote clients can connect — this MUST be
   the instance's public IP/DNS, otherwise clients connect to the broker and then
   get redirected to an unreachable address:
   ```bash
   cp .env.example .env
   # Fetch the public IP via IMDSv2 (default on Amazon Linux 2023):
   TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" \
     -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
   PUBIP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
     http://169.254.169.254/latest/meta-data/public-ipv4)
   echo "KAFKA_ADVERTISED_HOST=$PUBIP" >> .env
   # (or just edit .env and set KAFKA_ADVERTISED_HOST to your instance's public IP)
   ```
4. **Start it:**
   ```bash
   docker compose up -d
   npm install && npm run topics:create   # or run topics:create from your laptop
   ```
5. Other services connect using `KAFKA_BROKERS=<EC2_PUBLIC_IP>:29092`.

> The default setup is **PLAINTEXT (no auth)** — fine for a class project. For
> anything beyond that, restrict the security group to known IPs and add
> SASL/TLS.

---

## Shared cluster for all teams (Confluent Cloud) — recommended

For a cluster every team can reach over the internet with **no server admin and
no firewall/advertised-listener debugging**, use a managed Kafka cluster. The
same code works — only `.env` changes (SASL/TLS is enabled automatically when a
SASL username is set).

**One-time setup (Queue team):**

1. Sign up at [confluent.cloud](https://confluent.cloud) and create a **Basic**
   cluster (pick a cloud/region close to the team).
2. Create an **API key** for the cluster (Cluster → API Keys → Create key). Save
   the **key** and **secret**.
3. Copy the cluster's **Bootstrap server** (Cluster settings → Endpoints), e.g.
   `pkc-xxxxx.eu-central-1.aws.confluent.cloud:9092`.
4. Fill in `.env`:
   ```bash
   KAFKA_BROKERS=pkc-xxxxx.eu-central-1.aws.confluent.cloud:9092
   KAFKA_SASL_MECHANISM=plain
   KAFKA_SASL_USERNAME=<API_KEY>
   KAFKA_SASL_PASSWORD=<API_SECRET>
   KAFKA_REPLICATION_FACTOR=3
   ```
5. Create all topics on the managed cluster:
   ```bash
   npm install && npm run topics:create
   ```

**What you hand to the other teams** — bootstrap server + an API key/secret
(ideally issue a separate key per team so they can be revoked individually) and
the topic/schema contract in [`schemas/`](schemas).

> `docker-compose` is still used for **local development**; Confluent Cloud is the
> **shared** cluster the teams connect to. Switching between them is just `.env`.

---

## Integrating your microservice

Other teams set their `.env` (`KAFKA_BROKERS`, plus the `KAFKA_SASL_*` vars for
the shared Confluent Cloud cluster) and use the topic + schema contract. Any
language with a Kafka client works; the schemas in [`schemas/`](schemas) are the
source of truth.

**Node.js (reuse this repo's helpers):**

```js
const { publishEvent } = require('./src/producer');
const { createConsumer } = require('./src/consumer');
const { randomUUID } = require('crypto');

// Produce (Auction service publishing a sale)
await publishEvent('bid.basket.sold', {
  eventId: randomUUID(),
  sessionId: 'session-2026-06-18',
  basketId: 'basket-1',
  buyerId: 'buy-001',
  winningBidId: 'bid-abc',
  salePrice: 110,
  occurredAt: new Date().toISOString(),
});

// Consume (Notification service)
await createConsumer({
  groupId: 'notification-service',
  topics: ['bid.basket.sold', 'bid.payment.confirmed'],
  onEvent: ({ topic, payload, valid }) => {
    if (valid) sendNotification(topic, payload);
  },
});
```

**Rules of the contract**

- Publish each event to the topic that matches its type (topic name = event type).
- Include `eventId` (UUID) and `occurredAt` (ISO-8601) on every event.
- Use `sessionId` as the message key for auction events to keep them ordered.
- Each service uses its **own** `groupId` (e.g. `auction-service`,
  `fulfillment-service`).

---

## Project structure

```
kafka-queue/
├── docker-compose.yml          # Kafka (KRaft) + Kafka UI
├── .env.example                # KAFKA_ADVERTISED_HOST / KAFKA_BROKERS
├── config/
│   └── topics.json             # topic catalog (name, partitions, RF)
├── schemas/                    # JSON Schema per event type (the contract)
│   ├── bid.basket.sold.schema.json
│   └── ... (17 total)
├── src/
│   ├── config.js               # env-based config
│   ├── kafka.js                # shared KafkaJS client
│   ├── schemaRegistry.js       # loads + compiles schemas, validates payloads
│   ├── producer.js             # publishEvent() — validates then sends
│   ├── consumer.js             # createConsumer() — subscribe + validate
│   ├── admin/
│   │   ├── create-topics.js    # npm run topics:create
│   │   ├── list-topics.js      # npm run topics:list
│   │   └── validate-schemas.js # npm run validate:schemas (offline)
│   └── demo/
│       ├── produce-demo.js     # full auction simulation
│       └── consume-demo.js     # observer for all topics
└── docs/
    ├── EVENT-CATALOG.md        # routing table + message flow
    └── REPORT.md               # SEDS 520 report draft
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| Client hangs / `ECONNREFUSED` | `KAFKA_ADVERTISED_HOST` must match how clients reach the broker (localhost vs EC2 public IP). Restart: `docker compose up -d`. |
| `There is no leader for this topic-partition` | Topics not created yet — run `npm run topics:create`. |
| Remote client connects then times out | Advertised listener points to an address the client can't reach. Set `KAFKA_ADVERTISED_HOST` to the public IP and open port 29092. |
| Want a clean slate | `docker compose down -v` then `up -d` and re-create topics. |

---

## License

MIT
