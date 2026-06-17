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

// Optional SASL/SSL. Set these to point at a managed cluster such as Confluent
// Cloud (username = API key, password = API secret). Leave them unset to use the
// local/self-hosted PLAINTEXT broker from docker-compose.
const sasl = process.env.KAFKA_SASL_USERNAME
  ? {
      mechanism: process.env.KAFKA_SASL_MECHANISM || 'plain',
      username: process.env.KAFKA_SASL_USERNAME,
      password: process.env.KAFKA_SASL_PASSWORD,
    }
  : undefined;

// SSL is implied by SASL (managed clusters require it) or forced via KAFKA_SSL=true.
const ssl = Boolean(sasl) || process.env.KAFKA_SSL === 'true';

module.exports = {
  brokers,
  clientId: process.env.KAFKA_CLIENT_ID || 'fish-auction-queue',
  sasl,
  ssl: ssl || undefined,
};
