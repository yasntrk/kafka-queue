'use strict';

const { validators, topics, check } = require('../schemaRegistry');

/**
 * Offline sanity check — runs WITHOUT a Kafka broker.
 *
 * 1. Confirms every schema file compiles.
 * 2. Validates the canonical example payloads from the assignment against their
 *    schemas, proving the schemas faithfully encode the agreed event contract.
 *
 * Useful for verifying the repo on a machine that has no Docker/Kafka.
 */
const examples = {
  'bid.basket.sold': {
    eventId: '11111111-1111-1111-1111-111111111111',
    sessionId: 'session-2026-06-18',
    basketId: 'basket-1',
    buyerId: 'buyer-7',
    winningBidId: 'bid-42',
    salePrice: 110,
    occurredAt: '2026-06-18T07:35:00.000Z',
  },
  'bid.basket.unsold': {
    eventId: '22222222-2222-2222-2222-222222222222',
    sessionId: 'session-2026-06-18',
    basketId: 'basket-3',
    reason: 'NO_BIDS',
    occurredAt: '2026-06-18T07:36:00.000Z',
  },
  'bid.rebid.round.opened': {
    eventId: '33333333-3333-3333-3333-333333333333',
    sessionId: 'session-2026-06-18',
    roundNumber: 2,
    basketIds: ['basket-3'],
    occurredAt: '2026-06-18T07:40:00.000Z',
  },
  'bid.payment.confirmed': {
    eventId: '44444444-4444-4444-4444-444444444444',
    sessionId: 'session-2026-06-18',
    basketId: 'basket-1',
    buyerId: 'buyer-7',
    amount: 110,
    paymentStatus: 'CONFIRMED',
    occurredAt: '2026-06-18T07:50:00.000Z',
  },
  'bid.basket.sale.completed': {
    eventId: '55555555-5555-5555-5555-555555555555',
    sessionId: 'session-2026-06-18',
    basketId: 'basket-1',
    buyerId: 'buyer-7',
    salePrice: 110,
    paymentConfirmed: true,
    occurredAt: '2026-06-18T07:51:00.000Z',
  },
  'bid.all.baskets.finalized': {
    eventId: '66666666-6666-6666-6666-666666666666',
    sessionId: 'session-2026-06-18',
    totalBaskets: 3,
    soldBasketCount: 3,
    unsoldBasketCount: 0,
    occurredAt: '2026-06-18T07:55:00.000Z',
  },
};

let failures = 0;

console.log(`Compiled ${topics.length} schema(s):`);
for (const t of topics) console.log(`  - ${t}`);

console.log('\nValidating example payloads from the assignment contract:');
for (const [topic, payload] of Object.entries(examples)) {
  if (!validators[topic]) {
    console.log(`  ? ${topic}  (no schema found)`);
    failures += 1;
    continue;
  }
  const { valid, errors } = check(topic, payload);
  if (valid) {
    console.log(`  PASS  ${topic}`);
  } else {
    console.log(`  FAIL  ${topic}: ${errors.join('; ')}`);
    failures += 1;
  }
}

if (failures > 0) {
  console.error(`\n${failures} check(s) failed.`);
  process.exitCode = 1;
} else {
  console.log('\nAll schemas compiled and all example payloads are valid.');
}
