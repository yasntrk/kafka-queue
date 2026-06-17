'use strict';

const fs = require('fs');
const path = require('path');
const kafka = require('../kafka');

/**
 * Creates every topic defined in config/topics.json.
 * Idempotent: topics that already exist are skipped, so it is safe to re-run.
 */
async function main() {
  const configPath = path.join(__dirname, '..', '..', 'config', 'topics.json');
  const { topics } = JSON.parse(fs.readFileSync(configPath, 'utf8'));

  const admin = kafka.admin();
  await admin.connect();

  try {
    const existing = new Set(await admin.listTopics());
    const toCreate = topics.filter((t) => !existing.has(t.topic));

    if (toCreate.length === 0) {
      console.log(`All ${topics.length} topics already exist — nothing to do.`);
    } else {
      await admin.createTopics({
        waitForLeaders: true,
        topics: toCreate.map((t) => ({
          topic: t.topic,
          numPartitions: t.partitions || 1,
          replicationFactor: t.replicationFactor || 1,
          configEntries: t.config
            ? Object.entries(t.config).map(([name, value]) => ({ name, value: String(value) }))
            : undefined,
        })),
      });
      console.log(`Created ${toCreate.length} topic(s):`);
      for (const t of toCreate) {
        console.log(`  + ${t.topic}  (partitions=${t.partitions || 1}, RF=${t.replicationFactor || 1})`);
      }
    }

    const applicationTopics = (await admin.listTopics())
      .filter((t) => !t.startsWith('__'))
      .sort();
    console.log(`\nCluster now has ${applicationTopics.length} application topic(s).`);
  } finally {
    await admin.disconnect();
  }
}

main().catch((err) => {
  console.error('create-topics failed:', err.message);
  process.exitCode = 1;
});
