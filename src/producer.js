'use strict';

const kafka = require('./kafka');
const { assertValid } = require('./schemaRegistry');

/**
 * Reusable, schema-validating producer.
 *
 * Other microservices can copy this pattern: call publishEvent(topic, payload)
 * and the helper validates the payload against the topic's JSON Schema before
 * sending. Invalid events are rejected locally and never reach the broker.
 *
 * The producer is idempotent, so retries never create duplicate messages.
 */
const producer = kafka.producer({
  allowAutoTopicCreation: false,
  idempotent: true,
});

let connected = false;

async function connect() {
  if (!connected) {
    await producer.connect();
    connected = true;
  }
}

async function disconnect() {
  if (connected) {
    await producer.disconnect();
    connected = false;
  }
}

/**
 * Choose a partition key so that all events of one auction session land on the
 * same partition and are therefore consumed in order.
 */
function defaultKey(payload) {
  return (
    payload.sessionId ||
    payload.buyerId ||
    payload.memberId ||
    payload.basketId ||
    null
  );
}

/**
 * Validate and publish a single event.
 * @param {string} topic   topic / event-type name
 * @param {object} payload event body
 * @param {{ key?: string|null }} [opts]
 */
async function publishEvent(topic, payload, opts = {}) {
  assertValid(topic, payload);
  await connect();
  const key = opts.key !== undefined ? opts.key : defaultKey(payload);
  await producer.send({
    topic,
    messages: [
      {
        key: key == null ? null : String(key),
        value: JSON.stringify(payload),
        headers: { 'content-type': 'application/json', eventType: topic },
      },
    ],
  });
  return payload;
}

module.exports = { producer, connect, disconnect, publishEvent };
