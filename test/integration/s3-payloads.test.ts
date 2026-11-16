/**
 * Payloads over the DynamoDB item limit against the emulator's S3: each
 * kind of entity writes them to the bucket, keeps a reference in its item
 * and reads them back unchanged; without a bucket the write is refused.
 */
import { GetItemCommand } from '@aws-sdk/client-dynamodb';
import { HeadObjectCommand, NotFound } from '@aws-sdk/client-s3';
import type { World } from '@workflow/world';
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  inject,
  it,
  onTestFinished,
} from 'vitest';
import { runMetaKey, snapshotKey, stepKey } from '../../src/keys.js';
import {
  createStorageTestContext,
  type StorageTestContext,
  s3Location,
} from './storage-context.js';

const aws = inject('aws');

let context: StorageTestContext;
let world: World;

beforeAll(async () => {
  context = await createStorageTestContext('s3');
  world = context.world;
}, 120_000);

beforeEach(async () => {
  await context.truncate();
});

afterAll(async () => {
  await context?.close();
});

/** `size` bytes that are not all the same. */
const bytes = (size: number, seed = 7) =>
  new Uint8Array(size).map((_, i) => (i * seed) % 251);

/** The S3 reference an item attribute holds, as stored. */
async function storedRef(
  key: { PK: string; SK: string },
  attribute: string
): Promise<string | undefined> {
  const { Item } = await context.clients.dynamodb.send(
    new GetItemCommand({
      TableName: context.tableName,
      Key: { PK: { S: key.PK }, SK: { S: key.SK } },
      ConsistentRead: true,
    })
  );
  return Item?.[attribute]?.M?.s3Ref?.S;
}

async function objectSize(ref: string): Promise<number | undefined> {
  const { bucket, key } = s3Location(ref);
  try {
    const head = await context.clients.s3.send(
      new HeadObjectCommand({ Bucket: bucket, Key: key })
    );
    return head.ContentLength;
  } catch (error) {
    if (error instanceof NotFound) return undefined;
    throw error;
  }
}

async function runningRun(input = new Uint8Array([1])): Promise<string> {
  const created = await world.events.create(null, {
    eventType: 'run_created',
    eventData: { deploymentId: 'dpl', workflowName: 'wf', input },
  });
  await world.events.create(created.run.runId, { eventType: 'run_started' });
  return created.run.runId;
}

describe('payloads over the item limit, with a bucket', () => {
  it('round-trips a run input and output through S3', async () => {
    const input = bytes(500 * 1024);
    const runId = await runningRun(input);

    const ref = await storedRef(runMetaKey(runId), 'input');
    expect(ref).toMatch(new RegExp(`^s3://${aws.bucket}/${runId}/run/`));
    expect(await objectSize(ref ?? '')).toBe(input.byteLength);
    expect((await world.runs.get(runId)).input).toEqual(input);

    const output = bytes(450 * 1024, 3);
    const completed = await world.events.create(runId, {
      eventType: 'run_completed',
      eventData: { output },
    });
    expect(completed.run?.output).toEqual(output);

    const run = await world.runs.get(runId);
    expect(run.input).toEqual(input);
    expect(run.output).toEqual(output);
    expect(run.status).toBe('completed');
    const listed = await world.runs.list({ workflowName: 'wf' });
    expect(listed.data[0]?.output).toEqual(output);
    // Without data, nothing of it comes back.
    const bare = await world.runs.get(runId, { resolveData: 'none' });
    expect(bare.input).toBeUndefined();
    expect(bare.output).toBeUndefined();
  });

  it('round-trips a step input and result, and the events that carry them', async () => {
    const runId = await runningRun();
    const input = bytes(400 * 1024);
    const result = bytes(700 * 1024, 5);

    await world.events.create(runId, {
      eventType: 'step_created',
      correlationId: 'step_big',
      eventData: { stepName: 's', input },
    });
    await world.events.create(runId, {
      eventType: 'step_started',
      correlationId: 'step_big',
    });
    const completed = await world.events.create(runId, {
      eventType: 'step_completed',
      correlationId: 'step_big',
      eventData: { result },
    });
    expect(completed.step?.output).toEqual(result);

    expect(await storedRef(stepKey(runId, 'step_big'), 'input')).toMatch(
      `/${runId}/step/step_big/input-`
    );
    const step = await world.steps.get(runId, 'step_big');
    expect(step.input).toEqual(input);
    expect(step.output).toEqual(result);
    expect(step.status).toBe('completed');

    const log = await world.events.list({ runId });
    const dataOf = (eventType: string) =>
      (
        log.data.find((event) => event.eventType === eventType) as {
          eventData?: { input?: unknown; result?: unknown };
        }
      )?.eventData;
    expect(dataOf('step_created')?.input).toEqual(input);
    expect(dataOf('step_completed')?.result).toEqual(result);
  });

  it('round-trips hook metadata', async () => {
    const runId = await runningRun();
    const metadata = bytes(380 * 1024, 11);

    await world.events.create(runId, {
      eventType: 'hook_created',
      correlationId: 'hook_big',
      eventData: { token: 'token_big', metadata },
    });

    expect((await world.hooks.get('hook_big')).metadata).toEqual(metadata);
    expect((await world.hooks.getByToken('token_big')).metadata).toEqual(
      metadata
    );
  });

  it('round-trips a VM snapshot, and deletes the object of the one it replaces', async () => {
    const runId = await runningRun();
    const snapshots = world.experimental_snapshots;
    if (!snapshots) throw new Error('snapshots expected');
    const metadata = {
      eventsCursor: 'evnt_00000000000000000000000002',
      createdAt: new Date(),
    };

    const first = bytes(600 * 1024);
    await snapshots.save(runId, first, metadata);
    const firstRef = await storedRef(snapshotKey(runId), 'data');
    expect(firstRef).toMatch(`/${runId}/snapshot/`);
    expect((await snapshots.load(runId))?.data).toEqual(first);

    const second = bytes(600 * 1024, 13);
    await snapshots.save(runId, second, metadata);
    expect((await snapshots.load(runId))?.data).toEqual(second);
    expect(await objectSize(firstRef ?? '')).toBeUndefined();
  });
});

describe('payloads over the item limit, without a bucket', () => {
  it('refuses the write with a typed error that names the setting, writing nothing', async () => {
    const noBucket = context.secondWorld({ bucket: undefined });
    onTestFinished(async () => {
      await noBucket.close?.();
    });
    const runId = await runningRun();

    const write = noBucket.events.create(runId, {
      eventType: 'step_created',
      correlationId: 'step_big',
      eventData: { stepName: 's', input: bytes(360 * 1024) },
    });
    await expect(write).rejects.toMatchObject({
      name: 'PayloadTooLargeError',
      status: 413,
      code: 'PAYLOAD_TOO_LARGE',
    });
    await expect(write).rejects.toThrow(/WORKFLOW_AWS_BUCKET/);
    const log = await world.events.list({ runId });
    expect(log.data.map((event) => event.eventType)).toEqual([
      'run_created',
      'run_started',
    ]);
  });

  it('still reads payloads written to S3 by a World that had one', async () => {
    const input = bytes(500 * 1024);
    const runId = await runningRun(input);
    const noBucket = context.secondWorld({ bucket: undefined });
    onTestFinished(async () => {
      await noBucket.close?.();
    });

    expect((await noBucket.runs.get(runId)).input).toEqual(input);
  });
});
