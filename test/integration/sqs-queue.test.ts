/**
 * The queue end to end against the emulator's SQS: sends, deliveries,
 * delays (including the hop of a delay over 15 minutes), redeliveries,
 * failures moving to the dead-letter queue, idempotency keys, close() and
 * startup recovery. Each test gets its own queues, so no message leaks from
 * one test into another.
 */
import { randomUUID } from 'node:crypto';
import {
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { GetCommand } from '@aws-sdk/lib-dynamodb';
import type { MessageId, QueuePayload } from '@workflow/world';
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  inject,
  it,
  vi,
} from 'vitest';
import {
  createQueueWithDeadLetter,
  createWorkflowTable,
} from '../../scripts/bootstrap-local.js';
import {
  type AwsClients,
  createClients,
  destroyClients,
} from '../../src/clients.js';
import { createWorld } from '../../src/index.js';
import { createSqsQueue, type SqsQueueOptions } from '../../src/queue/index.js';
import { encodeMessage } from '../../src/queue/message.js';

const aws = inject('aws');
const prefix = '__wkf_workflow_';
const queueName = '__wkf_workflow_wf' as const;

type Meta = {
  messageId: string;
  attempt: number;
  deliveryCount?: number;
  queueName: string;
};

let clients: AwsClients;
let tableName: string;
const open: { close(): Promise<unknown> }[] = [];

beforeAll(async () => {
  clients = createClients({ region: aws.region, endpoint: aws.endpoint });
  tableName = `wf-queue-${randomUUID().slice(0, 8)}`;
  await createWorkflowTable(clients, tableName);
}, 120_000);

afterEach(async () => {
  await Promise.all(open.splice(0).map((each) => each.close()));
});

afterAll(() => {
  destroyClients(clients);
});

async function queues(maxReceiveCount = 5, visibilityTimeoutSeconds = 30) {
  const name = `q-${randomUUID().slice(0, 8)}`;
  const workflow = await createQueueWithDeadLetter(
    clients.sqs,
    `${name}-wf`,
    maxReceiveCount,
    visibilityTimeoutSeconds
  );
  const step = await createQueueWithDeadLetter(
    clients.sqs,
    `${name}-step`,
    maxReceiveCount,
    visibilityTimeoutSeconds
  );
  return { workflow, step };
}

/** A queue over fresh SQS queues, delivering in process to `handler`. */
async function setUp(
  handler: (message: unknown, meta: Meta) => Promise<unknown>,
  overrides: Partial<SqsQueueOptions> & { maxReceiveCount?: number } = {}
) {
  const { maxReceiveCount, ...rest } = overrides;
  const { workflow, step } = await queues(
    maxReceiveCount,
    rest.visibilityTimeoutSeconds
  );
  const options: SqsQueueOptions = {
    sqs: clients.sqs,
    documents: clients.documents,
    tableName,
    workflowQueueUrl: workflow.queueUrl,
    stepQueueUrl: step.queueUrl,
    endpoint: aws.endpoint,
    concurrency: 4,
    visibilityTimeoutSeconds: 30,
    pollWaitSeconds: 1,
    shutdownGraceMs: 5_000,
    internal: { log: () => {} },
    ...rest,
  };
  const queue = createSqsQueue(options);
  open.push(queue);
  queue.registerHandler(
    prefix,
    queue.createQueueHandler(prefix, (message, meta) =>
      handler(message, meta as Meta)
    )
  );
  return { queue, options, workflow, step };
}

async function receiveAll(queueUrl: string): Promise<Message[]> {
  const result = await clients.sqs.send(
    new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 1,
    })
  );
  return result.Messages ?? [];
}

const payload = (extra: Record<string, unknown> = {}) =>
  ({ runId: `wrun_${randomUUID()}`, ...extra }) as unknown as QueuePayload;

describe('the SQS queue', () => {
  it('delivers a message once, with its bytes and a stable id, and deletes it', async () => {
    const seen: [unknown, Meta][] = [];
    const { queue, options } = await setUp(async (message, meta) => {
      seen.push([message, meta]);
    });
    await queue.startWorker();
    const sent = payload({
      runInput: {
        input: new Uint8Array([0, 1, 255]),
        deploymentId: 'aws',
        workflowName: 'wf',
        specVersion: 8,
      },
    });
    const { messageId } = await queue.queue(queueName, sent);

    await vi.waitFor(() => expect(seen).toHaveLength(1), { timeout: 15_000 });
    expect(seen[0]?.[0]).toEqual(sent);
    expect(seen[0]?.[1]).toMatchObject({
      messageId,
      attempt: 1,
      deliveryCount: 1,
      queueName,
    });
    await queue.close();
    expect(await receiveAll(options.workflowQueueUrl)).toEqual([]);
  });

  it('routes step messages through the step queue', async () => {
    const seen: unknown[] = [];
    const { queue, options } = await setUp(async (message) => {
      seen.push(message);
    });
    // Not started: read the queues directly.
    await queue.queue(queueName, payload({ stepId: 'step_1' }));
    expect(await receiveAll(options.workflowQueueUrl)).toEqual([]);
    expect(await receiveAll(options.stepQueueUrl)).toHaveLength(1);
  });

  it('honors delaySeconds', async () => {
    const at: number[] = [];
    const { queue } = await setUp(async () => {
      at.push(Date.now());
    });
    await queue.startWorker();
    const sentAt = Date.now();
    await queue.queue(queueName, payload(), { delaySeconds: 2 });
    await vi.waitFor(() => expect(at).toHaveLength(1), { timeout: 15_000 });
    expect((at[0] ?? 0) - sentAt).toBeGreaterThanOrEqual(1_900);
  });

  it('sends an early hop of a long delay on with the rest of it', async () => {
    const seen: Meta[] = [];
    const { queue, options } = await setUp(async (_message, meta) => {
      seen.push(meta);
    });
    // What the last 15-minute hop of a long delay looks like when it
    // arrives with three seconds still to go.
    const notBefore = Date.now() + 3_000;
    await clients.sqs.send(
      new SendMessageCommand({
        QueueUrl: options.workflowQueueUrl,
        MessageBody: encodeMessage({
          v: 1,
          messageId: 'msg_01JD000000000000000000HOP1' as MessageId,
          queueName,
          payload: payload(),
          createdAt: new Date().toISOString(),
          notBefore,
        }),
      })
    );
    await queue.startWorker();
    await vi.waitFor(() => expect(seen).toHaveLength(1), { timeout: 15_000 });
    expect(Date.now()).toBeGreaterThanOrEqual(notBefore - 1_000);
    // The hop is not a delivery.
    expect(seen[0]).toMatchObject({
      messageId: 'msg_01JD000000000000000000HOP1',
      attempt: 1,
      deliveryCount: 1,
    });
  });

  it('redelivers the same message after { timeoutSeconds }', async () => {
    const seen: Meta[] = [];
    const { queue } = await setUp(async (_message, meta) => {
      seen.push(meta);
      return seen.length === 1 ? { timeoutSeconds: 1 } : undefined;
    });
    await queue.startWorker();
    const { messageId } = await queue.queue(queueName, payload());
    await vi.waitFor(() => expect(seen).toHaveLength(2), { timeout: 15_000 });
    expect(seen.map((meta) => meta.messageId)).toEqual([messageId, messageId]);
    expect(seen.map((meta) => meta.deliveryCount)).toEqual([1, 2]);
  });

  it('moves a message that keeps failing to the dead-letter queue after maxReceiveCount', async () => {
    const seen: Meta[] = [];
    const { queue, workflow } = await setUp(
      async (_message, meta) => {
        seen.push(meta);
        throw new Error('always fails');
      },
      {
        maxReceiveCount: 2,
        internal: { log: () => {}, retryDelaySeconds: () => 0 },
      }
    );
    await queue.startWorker();
    const { messageId } = await queue.queue(queueName, payload());

    let dead: Message[] = [];
    await vi.waitFor(
      async () => {
        dead = await receiveAll(workflow.deadLetterQueueUrl);
        expect(dead).toHaveLength(1);
      },
      { timeout: 30_000, interval: 500 }
    );
    expect(seen.map((meta) => meta.attempt)).toEqual([1, 2]);
    expect(JSON.parse(dead[0]?.Body ?? '{}').messageId).toBe(messageId);
  });

  it('sends a message once per idempotency key, and remembers it for 24 hours', async () => {
    const seen: unknown[] = [];
    const { queue } = await setUp(async (message) => {
      seen.push(message);
    });
    await queue.startWorker();
    const key = `step-${randomUUID()}`;
    const first = await queue.queue(queueName, payload(), {
      idempotencyKey: key,
    });
    const second = await queue.queue(queueName, payload(), {
      idempotencyKey: key,
    });
    const [third] = await queue.queueBatch(queueName, [
      { message: payload(), opts: { idempotencyKey: key } },
    ]);
    expect(second.messageId).toBe(first.messageId);
    expect(third).toEqual({ messageId: first.messageId });

    await vi.waitFor(() => expect(seen).toHaveLength(1), { timeout: 15_000 });
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    expect(seen).toHaveLength(1);
    const item = await clients.documents.send(
      new GetCommand({
        TableName: tableName,
        Key: { PK: `IDEMP#${key}`, SK: 'Q' },
        ConsistentRead: true,
      })
    );
    expect(item.Item).toMatchObject({
      messageId: first.messageId,
      state: 'sent',
    });
    expect(Number(item.Item?.expiresAt)).toBeGreaterThan(
      Date.now() / 1000 + 23 * 3600
    );
  });

  it('publishes a batch across both queues', async () => {
    const seen: unknown[] = [];
    const { queue } = await setUp(async (message) => {
      seen.push(message);
    });
    await queue.startWorker();
    const messages = Array.from({ length: 12 }, (_, i) => ({
      message: payload(i % 2 ? { stepId: `step_${i}` } : {}),
    }));
    const results = await queue.queueBatch(queueName, messages);
    expect(results.every((result) => result.error === undefined)).toBe(true);
    await vi.waitFor(() => expect(seen).toHaveLength(12), { timeout: 20_000 });
  });

  it('keeps a long delivery invisible with its heartbeat', async () => {
    const seen: Meta[] = [];
    const { queue, options } = await setUp(
      async (_message, meta) => {
        seen.push(meta);
        await new Promise((resolve) => setTimeout(resolve, 4_000));
      },
      { visibilityTimeoutSeconds: 2 }
    );
    await queue.startWorker();
    await queue.queue(queueName, payload());
    await vi.waitFor(() => expect(seen).toHaveLength(1), { timeout: 15_000 });
    await new Promise((resolve) => setTimeout(resolve, 5_000));
    expect(seen).toHaveLength(1);
    await queue.close();
    expect(await receiveAll(options.workflowQueueUrl)).toEqual([]);
  });

  it('close() waits for a running delivery and then stops receiving', async () => {
    const seen: unknown[] = [];
    let finished = false;
    const { queue, options } = await setUp(async (message) => {
      seen.push(message);
      if (seen.length === 1) {
        await new Promise((resolve) => setTimeout(resolve, 1_000));
        finished = true;
      }
    });
    await queue.startWorker();
    await queue.queue(queueName, payload());
    await vi.waitFor(() => expect(seen).toHaveLength(1), { timeout: 15_000 });
    await queue.close();
    expect(finished).toBe(true);

    // Sent after close: nobody here takes it.
    const sender = createSqsQueue(options);
    open.push(sender);
    await sender.queue(queueName, payload());
    await new Promise((resolve) => setTimeout(resolve, 1_500));
    expect(seen).toHaveLength(1);
    expect(await receiveAll(options.workflowQueueUrl)).toHaveLength(1);
  });

  it('a delivery abandoned by close() is delivered again after its visibility timeout', async () => {
    const seen: Meta[] = [];
    const { queue, options } = await setUp(
      async (_message, meta) => {
        seen.push(meta);
        if (seen.length === 1) {
          await new Promise((resolve) => setTimeout(resolve, 3_000));
        }
      },
      { visibilityTimeoutSeconds: 2, shutdownGraceMs: 100 }
    );
    await queue.startWorker();
    const { messageId } = await queue.queue(queueName, payload());
    await vi.waitFor(() => expect(seen).toHaveLength(1), { timeout: 15_000 });
    const before = Date.now();
    await queue.close();
    expect(Date.now() - before).toBeLessThan(2_000);

    // Another worker over the same queues takes it once it is visible.
    const next = createSqsQueue({ ...options, shutdownGraceMs: 5_000 });
    open.push(next);
    expect(await next.startWorker()).toBe(true);
    await vi.waitFor(() => expect(seen).toHaveLength(2), { timeout: 15_000 });
    expect(seen[1]).toMatchObject({ messageId, deliveryCount: 2 });
  });
});

describe('createWorld() with the SQS queue', () => {
  it('re-enqueues the runs that have not ended when it starts', async () => {
    const { workflow, step } = await queues();
    const world = createWorld({
      region: aws.region,
      endpoint: aws.endpoint,
      tableName,
      workflowQueueUrl: workflow.queueUrl,
      stepQueueUrl: step.queueUrl,
      clients,
      pollWaitSeconds: 1,
    });
    open.push(world);
    const created = await world.events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'aws',
        workflowName: 'recovered',
        input: new Uint8Array([1]),
      },
    });
    const runId = created.run.runId;
    await world.events.create(runId, { eventType: 'run_started' });
    const done = await world.events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'aws',
        workflowName: 'recovered',
        input: new Uint8Array([1]),
      },
    });
    await world.events.create(done.run.runId, { eventType: 'run_cancelled' });

    const seen: [unknown, Meta][] = [];
    world.registerHandler(
      prefix,
      world.createQueueHandler(prefix, async (message, meta) => {
        seen.push([message, meta as Meta]);
      })
    );
    await world.start();
    await vi.waitFor(
      () =>
        expect(
          seen.some(
            ([message]) => (message as { runId?: string }).runId === runId
          )
        ).toBe(true),
      { timeout: 15_000 }
    );
    expect(
      seen.some(
        ([message]) => (message as { runId?: string }).runId === done.run.runId
      )
    ).toBe(false);
    expect(
      seen.find(
        ([message]) => (message as { runId: string }).runId === runId
      )?.[1].queueName
    ).toBe('__wkf_workflow_recovered');
  });
});
