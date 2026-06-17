'use strict';

const kafka = require('../kafka');
const {
  AclResourceTypes,
  AclOperationTypes,
  AclPermissionTypes,
  ResourcePatternTypes,
} = require('kafkajs');

/**
 * Grants a service account the ACLs it needs to produce and consume on every
 * topic of this cluster. Run with the cluster admin credentials in .env.
 *
 * Usage:
 *   node src/admin/create-acls.js <serviceAccountNumericId>
 *   # or set KAFKA_ACL_PRINCIPAL=User:<id>
 *
 * The principal for a Confluent Cloud service account is its NUMERIC id
 * (e.g. User:9076593), not the sa-xxxx resource id.
 */
const arg = process.argv[2];
const principal =
  process.env.KAFKA_ACL_PRINCIPAL ||
  (arg ? `User:${arg.replace(/^User:/, '')}` : null);

async function main() {
  if (!principal) {
    console.error(
      'Usage: node src/admin/create-acls.js <serviceAccountNumericId>\n' +
        '       (or set KAFKA_ACL_PRINCIPAL=User:<id>)'
    );
    process.exit(1);
  }

  const base = {
    principal,
    host: '*',
    permissionType: AclPermissionTypes.ALLOW,
  };

  const acl = [
    // Produce + consume on all topics
    { ...base, resourceType: AclResourceTypes.TOPIC, resourceName: '*', resourcePatternType: ResourcePatternTypes.LITERAL, operation: AclOperationTypes.READ },
    { ...base, resourceType: AclResourceTypes.TOPIC, resourceName: '*', resourcePatternType: ResourcePatternTypes.LITERAL, operation: AclOperationTypes.WRITE },
    { ...base, resourceType: AclResourceTypes.TOPIC, resourceName: '*', resourcePatternType: ResourcePatternTypes.LITERAL, operation: AclOperationTypes.DESCRIBE },
    // Use any consumer group
    { ...base, resourceType: AclResourceTypes.GROUP, resourceName: '*', resourcePatternType: ResourcePatternTypes.LITERAL, operation: AclOperationTypes.READ },
  ];

  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createAcls({ acl });
    console.log(`Created ${acl.length} ACL(s) for ${principal}:`);
    console.log('  TOPIC  "*"  ALLOW  READ, WRITE, DESCRIBE');
    console.log('  GROUP  "*"  ALLOW  READ');

    // Verify
    const existing = await admin.describeAcls({
      resourceType: AclResourceTypes.ANY,
      resourcePatternType: ResourcePatternTypes.ANY,
      permissionType: AclPermissionTypes.ANY,
      operation: AclOperationTypes.ANY,
      principal,
    });
    const count = existing.resources.reduce((n, r) => n + r.acls.length, 0);
    console.log(`\nVerified: ${count} ACL(s) now exist for ${principal}.`);
  } finally {
    await admin.disconnect();
  }
}

main().catch((err) => {
  console.error('create-acls failed:', err.message);
  process.exitCode = 1;
});
