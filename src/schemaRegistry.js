'use strict';

const fs = require('fs');
const path = require('path');
const Ajv = require('ajv');
const addFormats = require('ajv-formats');

/**
 * Lightweight, file-based schema registry.
 *
 * Every "<topic>.schema.json" file under /schemas is compiled once at startup.
 * The schema's "$id" is the topic name, so the topic a message goes to and the
 * schema it is validated against are guaranteed to match. This is the contract
 * that every other microservice in the system codes against.
 */
const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);

const schemasDir = path.join(__dirname, '..', 'schemas');

/** @type {Record<string, import('ajv').ValidateFunction>} */
const validators = {};

for (const file of fs.readdirSync(schemasDir)) {
  if (!file.endsWith('.schema.json')) continue;
  const raw = fs.readFileSync(path.join(schemasDir, file), 'utf8');
  const schema = JSON.parse(raw);
  const topic = schema.$id || file.replace('.schema.json', '');
  validators[topic] = ajv.compile(schema);
}

const topics = Object.keys(validators).sort();

/**
 * Validate a payload against the schema registered for `topic`.
 * @returns {{ valid: boolean, errors: string[] }}
 */
function check(topic, payload) {
  const validate = validators[topic];
  if (!validate) {
    return { valid: false, errors: [`no schema registered for topic "${topic}"`] };
  }
  const valid = validate(payload);
  const errors = valid
    ? []
    : (validate.errors || []).map((e) => `${e.instancePath || '(root)'} ${e.message}`);
  return { valid, errors };
}

/**
 * Validate and throw on failure. Used by the producer so a malformed event can
 * never enter the queue.
 */
function assertValid(topic, payload) {
  const { valid, errors } = check(topic, payload);
  if (!valid) {
    throw new Error(`Schema validation failed for "${topic}": ${errors.join('; ')}`);
  }
  return true;
}

module.exports = { validators, topics, check, assertValid };
