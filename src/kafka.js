'use strict';

const { Kafka, logLevel } = require('kafkajs');
const config = require('./config');

/**
 * Shared KafkaJS client. Every script (admin, producer, consumer) builds its
 * producers/consumers from this single client so they all use the same
 * connection settings.
 */
const kafka = new Kafka({
  clientId: config.clientId,
  brokers: config.brokers,
  logLevel: logLevel.ERROR,
  retry: {
    initialRetryTime: 300,
    retries: 8,
  },
});

module.exports = kafka;
