'use strict';

const kafka = require('./kafka');
const { validators } = require('./schemaRegistry');

/**
 * Helper that creates a consumer, subscribes to topics, and invokes `onEvent`
 * for every message. The payload is JSON-parsed and validated against its
 * topic schema before being handed to the callback.
 *
 * Each microservice runs its own consumer group, so every service receives its
 * own independent copy of the events it cares about.
 *
 * @param {object}   opts
 * @param {string}   opts.groupId        consumer group id (one per service)
 * @param {string[]} opts.topics         topics to subscribe to
 * @param {boolean}  [opts.fromBeginning] read history on first run (default true)
 * @param {(msg: object) => Promise<void>|void} opts.onEvent
 */
async function createConsumer({ groupId, topics, fromBeginning = true, onEvent }) {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topics, fromBeginning });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const raw = message.value ? message.value.toString() : '';
      let payload;
      try {
        payload = JSON.parse(raw);
      } catch (err) {
        await onEvent({
          topic,
          partition,
          key: message.key ? message.key.toString() : null,
          offset: message.offset,
          payload: raw,
          valid: false,
          errors: ['payload is not valid JSON'],
        });
        return;
      }

      const validate = validators[topic];
      const valid = validate ? validate(payload) : true;
      const errors = valid || !validate
        ? []
        : (validate.errors || []).map((e) => `${e.instancePath || '(root)'} ${e.message}`);

      await onEvent({
        topic,
        partition,
        key: message.key ? message.key.toString() : null,
        offset: message.offset,
        payload,
        valid,
        errors,
      });
    },
  });

  return consumer;
}

module.exports = { createConsumer };
