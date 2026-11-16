/**
 * Registers the shared World suite of `@workflow/world-testing` (the
 * modules of its `createTestSuite`) against this World on the emulator, the
 * way world-postgres runs it in `test/spec.test.ts` (spec 8) and
 * `test/spec9.test.ts` (spec 9).
 *
 * Every test of the suite starts its own app server (a child process that
 * loads the World named by WORKFLOW_TARGET_WORLD from the environment), so:
 *
 * - the World is bundled once with tsup, into node_modules/.cache, and
 *   loaded from there (`pnpm build` is not a prerequisite);
 * - before each test the `WORKFLOW_AWS_*` variables are pointed at a fresh
 *   table and fresh queues, so no server picks up another test's messages
 *   or recovers another test's runs, and at the emulator's bucket, with
 *   live stream reads polling every 50 ms;
 * - the servers trace their queue deliveries (`DEBUG=workflow:world-aws`),
 *   and a failed test prints what its table and queues hold.
 *
 * Every test of the suite runs: the only ones it skips are its own
 * (queue-auth tests for `queueAuthMode: 'loopback'` Worlds).
 */
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { createRequire } from 'node:module';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { GetQueueAttributesCommand } from '@aws-sdk/client-sqs';
import { ScanCommand } from '@aws-sdk/lib-dynamodb';
import { afterAll, beforeAll, beforeEach, inject } from 'vitest';
import {
  createQueueWithDeadLetter,
  createWorkflowTable,
} from '../../scripts/bootstrap-local.js';
import {
  type AwsClients,
  createClients,
  destroyClients,
} from '../../src/clients.js';
import { DEFAULT_MAX_RECEIVE_COUNT } from '../../src/queue/index.js';

const aws = inject('aws');
const root = path.resolve(fileURLToPath(import.meta.url), '../../..');
const bundleDir = path.join(root, 'node_modules/.cache/world-aws-spec');
const world = path.join(bundleDir, 'index.js');
const suite = (name: string) =>
  createRequire(import.meta.url).resolve(
    `@workflow/world-testing/dist/src/${name}.mjs`
  );

type Suite = (world: string) => void;
const load = async (name: string, exported: string) =>
  ((await import(suite(name))) as Record<string, Suite>)[exported] as Suite;

/**
 * Registers the suite's tests in the calling file. The app servers create
 * the spec `specVersion` runs (`WORKFLOW_SPEC_VERSION`); spec 9 is possible
 * because the World declares the in-band fence.
 */
export async function registerWorldTestingSuite(
  specVersion: 8 | 9
): Promise<void> {
  let clients: AwsClients;
  const saved = { ...process.env };

  beforeAll(async () => {
    process.env.WORKFLOW_SPEC_VERSION = String(specVersion);
    execFileSync(
      process.execPath,
      [
        path.join(root, 'node_modules/tsup/dist/cli-default.js'),
        '--out-dir',
        bundleDir,
        '--silent',
      ],
      { cwd: root, stdio: 'inherit' }
    );
    clients = createClients({ region: aws.region, endpoint: aws.endpoint });
  }, 120_000);

  beforeEach(async (context) => {
    const id = randomUUID().slice(0, 8);
    const tableName = `wf-spec-${id}`;
    await createWorkflowTable(clients, tableName);
    const workflow = await createQueueWithDeadLetter(
      clients.sqs,
      `spec-${id}-wf`,
      DEFAULT_MAX_RECEIVE_COUNT,
      60
    );
    const step = await createQueueWithDeadLetter(
      clients.sqs,
      `spec-${id}-step`,
      DEFAULT_MAX_RECEIVE_COUNT,
      60
    );
    context.onTestFailed(async () => {
      console.log(
        await describeWorldState(clients, tableName, {
          workflow: workflow.queueUrl,
          'workflow DLQ': workflow.deadLetterQueueUrl,
          step: step.queueUrl,
          'step DLQ': step.deadLetterQueueUrl,
        })
      );
    });
    Object.assign(process.env, {
      // The queue worker's delivery trace, in the server output a failed
      // test prints (unless DEBUG is set already).
      DEBUG: saved.DEBUG ?? 'workflow:world-aws',
      WORKFLOW_AWS_REGION: aws.region,
      WORKFLOW_AWS_ENDPOINT: aws.endpoint,
      WORKFLOW_AWS_TABLE: tableName,
      WORKFLOW_AWS_WORKFLOW_QUEUE_URL: workflow.queueUrl,
      WORKFLOW_AWS_STEP_QUEUE_URL: step.queueUrl,
      WORKFLOW_AWS_BUCKET: aws.bucket,
      WORKFLOW_AWS_POLL_WAIT_SECONDS: '1',
      WORKFLOW_AWS_STREAM_POLL_INTERVAL_MS: '50',
    });
  }, 60_000);

  afterAll(() => {
    for (const key of Object.keys(process.env)) {
      if (
        (key.startsWith('WORKFLOW_AWS_') ||
          key === 'WORKFLOW_SPEC_VERSION' ||
          key === 'DEBUG') &&
        !(key in saved)
      ) {
        delete process.env[key];
      }
    }
    Object.assign(process.env, saved);
    destroyClients(clients);
  });

  (await load('addition', 'addition'))(world);
  (await load('event-ids', 'eventIds'))(world);
  (await load('idempotency', 'idempotency'))(world);
  (await load('in-band-fence', 'inBandFence'))(world);
  (await load('hooks', 'hooks'))(world);
  (await load('null-byte', 'nullByte'))(world);
  (await load('queue-auth', 'queueAuth'))(world);
  (await load('errors', 'errors'))(world);
  (await load('inline-execution', 'inlineExecution'))(world);
  (await load('lineage', 'lineage'))(world);
  (await load('resolve-data', 'resolveData'))(world);
}

/** Attributes the state dump leaves out: keys it prints otherwise, indexes. */
const OMITTED = new Set(['PK', 'SK', 'GSI1PK', 'GSI1SK', 'GSI2PK', 'GSI2SK']);

/** One item as a line: its sort key and its small, readable attributes. */
function describeItem(item: Record<string, unknown>): string {
  const fields = Object.entries(item)
    .filter(([name]) => !OMITTED.has(name))
    .map(([name, value]) => {
      if (value instanceof Uint8Array) return `${name}=<${value.length} B>`;
      const text =
        typeof value === 'string' ? value : (JSON.stringify(value) ?? '');
      return `${name}=${text.length > 80 ? `${text.slice(0, 77)}...` : text}`;
    });
  return `  ${String(item.SK)} ${fields.join(' ')}`;
}

/**
 * What a failed test of the suite left behind, for the log: every run's
 * item, events (in log order) and `STEP#` items, read straight from the
 * test's table, and the message counts of its queues. A completed step whose
 * `step_completed` event is missing points at the emulator (a transaction
 * applied in part); a log that is complete with no wake-up message left
 * points at the World.
 */
async function describeWorldState(
  clients: AwsClients,
  tableName: string,
  queues: Record<string, string>
): Promise<string> {
  const lines = [`=== WORLD STATE (table ${tableName}) ===`];
  try {
    const items: Record<string, unknown>[] = [];
    let startKey: Record<string, unknown> | undefined;
    do {
      const page = await clients.documents.send(
        new ScanCommand({
          TableName: tableName,
          ConsistentRead: true,
          ExclusiveStartKey: startKey,
        })
      );
      items.push(...(page.Items ?? []));
      startKey = page.LastEvaluatedKey;
    } while (startKey);
    const runs = new Map<string, Record<string, unknown>[]>();
    for (const item of items) {
      const pk = String(item.PK);
      if (!pk.startsWith('RUN#')) continue;
      runs.set(pk, [...(runs.get(pk) ?? []), item]);
    }
    for (const [pk, partition] of runs) {
      partition.sort((a, b) => String(a.SK).localeCompare(String(b.SK)));
      const steps = partition.filter((item) =>
        String(item.SK).startsWith('STEP#')
      );
      const events = partition.filter((item) =>
        String(item.SK).startsWith('EVT#')
      );
      lines.push(
        `${pk}: ${events.length} events, ${steps.length} steps`,
        ...partition
          .filter((item) => !String(item.SK).startsWith('EVT#'))
          .map(describeItem),
        ...events.map(describeItem)
      );
    }
  } catch (error) {
    lines.push(`(could not read the table: ${String(error)})`);
  }
  for (const [name, queueUrl] of Object.entries(queues)) {
    try {
      const { Attributes = {} } = await clients.sqs.send(
        new GetQueueAttributesCommand({
          QueueUrl: queueUrl,
          AttributeNames: [
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesNotVisible',
            'ApproximateNumberOfMessagesDelayed',
          ],
        })
      );
      lines.push(
        `queue ${name}: visible ${Attributes.ApproximateNumberOfMessages}, in flight ${Attributes.ApproximateNumberOfMessagesNotVisible}, delayed ${Attributes.ApproximateNumberOfMessagesDelayed}`
      );
    } catch (error) {
      lines.push(`queue ${name}: (could not read: ${String(error)})`);
    }
  }
  return lines.join('\n');
}
