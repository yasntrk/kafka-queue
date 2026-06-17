'use strict';

/**
 * Central configuration for all Kafka clients in this repo.
 * Values come from environment variables (see .env.example), with
 * sensible local-development defaults.
 */
const brokers = (process.env.KAFKA_BROKERS || 'localhost:29092')
  .split(',')
  .map((b) => b.trim())
  .filter(Boolean);

module.exports = {
  brokers,
  clientId: process.env.KAFKA_CLIENT_ID || 'fish-auction-queue',
};
