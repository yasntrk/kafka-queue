'use strict';

const { createConsumer } = require('../consumer');
const { topics } = require('../schemaRegistry');

/**
 * Demo consumer — subscribes to EVERY topic in the system and prints each event
 * as it arrives, marking whether it passed schema validation. This stands in
 * for all the downstream microservices at once so you can watch the full
 * message flow in one window.
 *
 * Run this first, then run `npm run demo:produce` in another terminal.
 */
async function main() {
  console.log(`Consumer group "demo-observer" subscribing to ${topics.length} topics...`);

  const consumer = await createConsumer({
    groupId: 'demo-observer',
    topics,
    fromBeginning: true,
    onEvent: ({ topic, partition, key, payload, valid, errors }) => {
      const mark = valid ? 'OK ' : 'BAD';
      const summary = summarize(topic, payload);
      console.log(`[${mark}] ${topic.padEnd(38)} p${partition} key=${key ?? '-'}  ${summary}`);
      if (!valid) console.log(`        validation: ${errors.join('; ')}`);
    },
  });

  const shutdown = async () => {
    console.log('\nDisconnecting consumer...');
    await consumer.disconnect().catch(() => {});
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

/** Build a short, human-friendly one-liner for the most relevant fields. */
function summarize(topic, p) {
  if (typeof p !== 'object' || p === null) return String(p);
  const parts = [];
  if (p.basketId) parts.push(`basket=${p.basketId}`);
  if (p.buyerId) parts.push(`buyer=${p.buyerId}`);
  if (p.boatName) parts.push(`boat=${p.boatName}`);
  if (p.salePrice != null) parts.push(`price=${p.salePrice}`);
  if (p.amount != null) parts.push(`amount=${p.amount}`);
  if (p.reason) parts.push(`reason=${p.reason}`);
  if (p.roundNumber != null) parts.push(`round=${p.roundNumber}`);
  if (p.totalRevenue != null) parts.push(`revenue=${p.totalRevenue}`);
  if (p.channel) parts.push(`channel=${p.channel}`);
  return parts.join(' ');
}

main().catch((err) => {
  console.error('demo:consume failed:', err.message);
  process.exitCode = 1;
});
