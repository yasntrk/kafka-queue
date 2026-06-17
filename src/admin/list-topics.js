'use strict';

const kafka = require('../kafka');

/**
 * Lists application topics on the cluster with their partition counts.
 */
async function main() {
  const admin = kafka.admin();
  await admin.connect();

  try {
    const names = (await admin.listTopics())
      .filter((t) => !t.startsWith('__'))
      .sort();

    if (names.length === 0) {
      console.log('No application topics found. Run: npm run topics:create');
      return;
    }

    const metadata = await admin.fetchTopicMetadata({ topics: names });
    console.log(`${names.length} application topic(s):\n`);
    for (const t of metadata.topics.sort((a, b) => a.name.localeCompare(b.name))) {
      console.log(`  ${t.name}  (partitions=${t.partitions.length})`);
    }
  } finally {
    await admin.disconnect();
  }
}

main().catch((err) => {
  console.error('list-topics failed:', err.message);
  process.exitCode = 1;
});
