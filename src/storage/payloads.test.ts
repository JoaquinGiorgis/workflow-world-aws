import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DeleteObjectCommand,
  GetObjectCommand,
  type GetObjectCommandInput,
  PutObjectCommand,
  type PutObjectCommandInput,
  S3Client,
} from '@aws-sdk/client-s3';
import {
  DynamoDBDocumentClient,
  GetCommand,
  type GetCommandInput,
  PutCommand,
  QueryCommand,
  type QueryCommandInput,
  TransactWriteCommand,
  type TransactWriteCommandInput,
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createStorage } from './index.js';
import {
  isS3Ref,
  objectKeyPrefix,
  PayloadStore,
  type S3Ref,
} from './payloads.js';
import { Table } from './table.js';

const credentials = { accessKeyId: 'test', secretAccessKey: 'test' };
const documents = DynamoDBDocumentClient.from(
  new DynamoDBClient({ region: 'us-east-1', credentials })
);
const s3 = new S3Client({ region: 'us-east-1', credentials });
const ddb = mockClient(documents);
const s3Mock = mockClient(s3);

afterAll(() => {
  ddb.restore();
  s3Mock.restore();
});

const bucket = 'workflow-payloads';
const now = '2026-11-15T20:00:00.000Z';
const runId = 'wrun_01JD000000000000000000000A';

/** The bucket, as the mocked S3 holds it. */
let objects: Map<string, Uint8Array>;

beforeEach(() => {
  ddb.reset();
  s3Mock.reset();
  objects = new Map();
  s3Mock.on(PutObjectCommand).callsFake((input: PutObjectCommandInput) => {
    objects.set(input.Key ?? '', input.Body as Uint8Array);
    return {};
  });
  s3Mock.on(GetObjectCommand).callsFake((input: GetObjectCommandInput) => {
    const body = objects.get(input.Key ?? '');
    if (!body)
      throw Object.assign(new Error('NoSuchKey'), { name: 'NoSuchKey' });
    return { Body: { transformToByteArray: async () => body } as never };
  });
  s3Mock.on(DeleteObjectCommand).resolves({});
});

function runItem(overrides: Record<string, unknown> = {}) {
  return {
    PK: `RUN#${runId}`,
    SK: 'META',
    type: 'run',
    runId,
    status: 'running',
    deploymentId: 'aws',
    workflowName: 'wf',
    specVersion: 8,
    input: new Uint8Array([1]),
    createdAt: now,
    updatedAt: now,
    seq: 2,
    seqInBand: 1,
    rev: 5,
    ...overrides,
  };
}

/**
 * Serves GetItem from `items`, keyed by `PK|SK`, and queries on a key
 * prefix (`begins_with(SK, :prefix)`) from the same items.
 */
function serve(items: Record<string, Record<string, unknown>>) {
  ddb.on(GetCommand).callsFake((input: GetCommandInput) => {
    const key = input.Key as { PK: string; SK: string };
    return { Item: items[`${key.PK}|${key.SK}`] };
  });
  ddb.on(QueryCommand).callsFake((input: QueryCommandInput) => {
    const values = input.ExpressionAttributeValues ?? {};
    const prefix = values[':prefix'];
    if (typeof prefix !== 'string') return { Items: [] };
    return {
      Items: Object.values(items)
        .filter(
          (item) =>
            item.PK === values[':pk'] && String(item.SK).startsWith(prefix)
        )
        .sort((a, b) => String(a.SK).localeCompare(String(b.SK))),
    };
  });
}

function transactions(): TransactWriteCommandInput[] {
  return ddb
    .commandCalls(TransactWriteCommand)
    .map((call) => call.args[0].input);
}

const bytes = (size: number, seed = 7) =>
  new Uint8Array(size).map((_, i) => (i * seed) % 251);

describe('S3 offload of payloads too large for an item', () => {
  const stepCreated = (input: Uint8Array) => ({
    eventType: 'step_created' as const,
    correlationId: 'step_1',
    eventData: { stepName: 'add', input },
  });

  it('writes the values to S3, keeps references in the items, and reads them back', async () => {
    const items: Record<string, Record<string, unknown>> = {
      [`RUN#${runId}|META`]: runItem(),
    };
    serve(items);
    ddb
      .on(TransactWriteCommand)
      .callsFake((input: TransactWriteCommandInput) => {
        for (const entry of input.TransactItems ?? []) {
          const item = entry.Put?.Item;
          if (item) items[`${item.PK}|${item.SK}`] = item;
        }
        return {};
      });
    const storage = createStorage({
      documents,
      tableName: 'wf',
      payloads: new PayloadStore({ s3, bucket }),
    });
    const input = bytes(400 * 1024);

    const result = await storage.events.create(runId, stepCreated(input));
    expect(result.step?.input).toEqual(input);

    // The step's input and the event's data went to S3, under the run.
    const keys = s3Mock
      .commandCalls(PutObjectCommand)
      .map((call) => call.args[0].input.Key);
    expect(keys).toHaveLength(2);
    expect(keys).toContainEqual(
      expect.stringMatching(
        new RegExp(`^${runId}/step/step_1/input-[0-9a-f]{40}$`)
      )
    );
    expect(keys).toContainEqual(
      expect.stringMatching(new RegExp(`^${runId}/event/evnt_\\d+/eventData-`))
    );
    const [write] = transactions();
    const step = write?.TransactItems?.map((entry) => entry.Put?.Item).find(
      (item) => String(item?.SK).startsWith('STEP#')
    );
    const stepInput = step?.input as S3Ref;
    expect(isS3Ref(stepInput)).toBe(true);
    expect(stepInput.s3Ref).toMatch(`s3://${bucket}/${runId}/step/`);
    expect(stepInput.s3Encoding).toBe('binary');

    // Reads put the values back, from a fresh store (no cache).
    const reader = createStorage({
      documents,
      tableName: 'wf',
      payloads: new PayloadStore({ s3 }),
    });
    expect((await reader.steps.get(runId, 'step_1')).input).toEqual(input);
    const log = await reader.events.list({ runId });
    const event = log.data.find((each) => each.eventType === 'step_created');
    expect(
      (event as { eventData?: { input?: unknown } }).eventData?.input
    ).toEqual(input);
  });

  it('refuses the write without a bucket, naming WORKFLOW_AWS_BUCKET', async () => {
    serve({ [`RUN#${runId}|META`]: runItem() });
    for (const payloads of [undefined, new PayloadStore({ s3 })]) {
      const storage = createStorage({
        documents,
        tableName: 'wf',
        ...(payloads && { payloads }),
      });
      const write = storage.events.create(
        runId,
        stepCreated(bytes(400 * 1024))
      );
      await expect(write).rejects.toMatchObject({
        name: 'PayloadTooLargeError',
        status: 413,
        code: 'PAYLOAD_TOO_LARGE',
      });
      await expect(write).rejects.toThrow(/WORKFLOW_AWS_BUCKET/);
      await expect(write).rejects.toThrow(/`bucket` option of createWorld/);
    }
    expect(transactions()).toHaveLength(0);
    expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(0);
  });

  it('moves the largest value of a run item an update would grow over the limit', async () => {
    serve({
      [`RUN#${runId}|META`]: runItem({ input: bytes(250 * 1024) }),
    });
    ddb.on(TransactWriteCommand).resolves({});
    const storage = createStorage({
      documents,
      tableName: 'wf',
      payloads: new PayloadStore({ s3, bucket }),
    });
    const output = bytes(200 * 1024, 3);

    const result = await storage.events.create(runId, {
      eventType: 'run_completed',
      eventData: { output },
    });
    expect(result.run?.output).toEqual(output);

    // The input (already in the item, and the largest) moved to S3 by the
    // update itself; the output fits next to its reference.
    const update = transactions()[0]?.TransactItems?.find(
      (entry) => entry.Update?.Key?.SK === 'META'
    )?.Update;
    expect(update?.UpdateExpression).toMatch(/^SET #o0 = :o0, /);
    expect(update?.ExpressionAttributeNames?.['#o0']).toBe('input');
    expect(isS3Ref(update?.ExpressionAttributeValues?.[':o0'])).toBe(true);
    expect(update).not.toHaveProperty('offload');
    const outputValue = Object.entries(update?.ExpressionAttributeNames ?? {})
      .filter(([, name]) => name === 'output')
      .map(
        ([placeholder]) =>
          update?.ExpressionAttributeValues?.[`:${placeholder.slice(1)}`]
      );
    expect(outputValue).toEqual([output]);
    expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(1);
  });

  it('keeps a reference the run item holds, without uploading it again', async () => {
    const store = new PayloadStore({ s3, bucket });
    const input = bytes(300 * 1024);
    const ref = await store.put(`${runId}/run/${runId}/input`, input);
    s3Mock.resetHistory();
    serve({ [`RUN#${runId}|META`]: runItem({ input: ref }) });
    ddb.on(TransactWriteCommand).resolves({});
    const storage = createStorage({
      documents,
      tableName: 'wf',
      payloads: store,
    });
    const output = bytes(360 * 1024, 5);

    const result = await storage.events.create(runId, {
      eventType: 'run_completed',
      eventData: { output },
    });
    expect(result.run?.input).toEqual(input);
    expect(result.run?.output).toEqual(output);

    // 300 KB of input (by reference) and 360 KB of output: only the output
    // goes to S3 now, and the input is not uploaded again.
    const update = transactions()[0]?.TransactItems?.find(
      (entry) => entry.Update?.Key?.SK === 'META'
    )?.Update;
    const values = Object.values(update?.ExpressionAttributeValues ?? {});
    expect(values.filter(isS3Ref)).toHaveLength(1);
    expect(update?.UpdateExpression).not.toContain('#o0');
    // Uploaded: the run's output and the event's data, not the input.
    const keys = s3Mock
      .commandCalls(PutObjectCommand)
      .map((call) => call.args[0].input.Key ?? '');
    expect(keys).toHaveLength(2);
    expect(keys).toContainEqual(
      expect.stringMatching(`^${runId}/run/${runId}/output-`)
    );
    expect(keys).toContainEqual(
      expect.stringMatching(`^${runId}/event/evnt_\\d+/eventData-`)
    );
  });

  it('keeps a transaction under 4 MB by moving values to S3, largest first', async () => {
    ddb.on(TransactWriteCommand).resolves({});
    const table = new Table(documents, 'wf', new PayloadStore({ s3, bucket }));
    // Fifteen items of 300 KB each fit one by one, not together.
    await table.transact(
      Array.from({ length: 15 }, (_, i) => ({
        Put: {
          Item: {
            PK: `STREAM#${runId}#out`,
            SK: `CHUNK#${String(i).padStart(10, '0')}`,
            data: bytes(300 * 1024 + i),
          },
        },
      }))
    );

    const [write] = transactions();
    const datas = (write?.TransactItems ?? []).map(
      (entry) => entry.Put?.Item?.data
    );
    const inline = datas.filter((data) => data instanceof Uint8Array);
    const offloaded = datas.filter(isS3Ref);
    expect(inline.length + offloaded.length).toBe(15);
    expect(offloaded.length).toBeGreaterThan(0);
    const inlineBytes = inline.reduce(
      (sum, data) => sum + (data as Uint8Array).byteLength,
      0
    );
    expect(inlineBytes).toBeLessThanOrEqual(4 * 1024 * 1024);
    // The largest ones went: every inline chunk is smaller than any moved.
    const smallestMoved = Math.min(
      ...offloaded.map((ref) => (ref as S3Ref).s3Bytes)
    );
    for (const data of inline) {
      expect((data as Uint8Array).byteLength).toBeLessThan(smallestMoved);
    }
  });

  it('refuses a transaction over 4 MB without a bucket', async () => {
    const table = new Table(documents, 'wf', new PayloadStore({ s3 }));
    await expect(
      table.transact(
        Array.from({ length: 15 }, (_, i) => ({
          Put: {
            Item: {
              PK: `STREAM#${runId}#out`,
              SK: `CHUNK#${String(i).padStart(10, '0')}`,
              data: bytes(300 * 1024),
            },
          },
        }))
      )
    ).rejects.toMatchObject({
      name: 'PayloadTooLargeError',
      limit: 4 * 1024 * 1024,
      message: expect.stringContaining('WORKFLOW_AWS_BUCKET'),
    });
    expect(transactions()).toHaveLength(0);
  });

  it('deletes the object of a snapshot it replaces, and uploads equal content once', async () => {
    let stored: Record<string, unknown> | undefined;
    ddb.on(PutCommand).callsFake((input: { Item: Record<string, unknown> }) => {
      const old = stored;
      stored = input.Item;
      return { Attributes: old };
    });
    const storage = createStorage({
      documents,
      tableName: 'wf',
      payloads: new PayloadStore({ s3, bucket }),
    });
    const snapshots = storage.experimental_snapshots;
    const metadata = { eventsCursor: 'evnt_1', createdAt: Date.now() } as never;

    await snapshots?.save(runId, bytes(400 * 1024), metadata);
    const first = (stored as { data: S3Ref }).data.s3Ref;
    expect(first).toMatch(`s3://${bucket}/${runId}/snapshot/${runId}/data-`);
    expect(ddb.commandCalls(PutCommand)[0]?.args[0].input.ReturnValues).toBe(
      'ALL_OLD'
    );

    await snapshots?.save(runId, bytes(400 * 1024), metadata);
    expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(1);
    expect(s3Mock.commandCalls(DeleteObjectCommand)).toHaveLength(0);

    await snapshots?.save(runId, bytes(400 * 1024, 11), metadata);
    expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(2);
    expect(s3Mock.commandCalls(DeleteObjectCommand)[0]?.args[0].input).toEqual({
      Bucket: bucket,
      Key: first.slice(`s3://${bucket}/`.length),
    });
  });
});

describe('PayloadStore', () => {
  it('round-trips values that are not bytes as JSON, binaries included', async () => {
    const store = new PayloadStore({ s3, bucket });
    const value = {
      stepName: 'add',
      input: new Uint8Array([1, 2, 3]),
      nested: [
        { at: '2026-11-15T20:00:00.000Z', n: 1.5, ok: true, none: null },
      ],
    };
    const ref = await store.put(`${runId}/event/evnt_1/eventData`, value);
    expect(ref.s3Encoding).toBe('json');
    expect(
      s3Mock.commandCalls(PutObjectCommand)[0]?.args[0].input.ContentType
    ).toBe('application/json');

    const fresh = new PayloadStore({ s3 });
    expect(await fresh.get(ref)).toEqual(value);
  });

  it('names a reference whose object is gone', async () => {
    const store = new PayloadStore({ s3, bucket });
    await expect(
      store.get({
        s3Ref: `s3://${bucket}/gone`,
        s3Encoding: 'binary',
        s3Bytes: 1,
      })
    ).rejects.toMatchObject({
      name: 'PayloadObjectError',
      message: expect.stringContaining(`s3://${bucket}/gone`),
    });
  });

  it('recognizes only the exact reference shape', () => {
    const ref = { s3Ref: 's3://b/k', s3Encoding: 'binary', s3Bytes: 3 };
    expect(isS3Ref(ref)).toBe(true);
    expect(isS3Ref({ ...ref, extra: 1 })).toBe(false);
    expect(isS3Ref({ ...ref, s3Ref: 'http://b/k' })).toBe(false);
    expect(isS3Ref({ ...ref, s3Encoding: 'gzip' })).toBe(false);
    expect(isS3Ref([ref])).toBe(false);
    expect(isS3Ref(new Uint8Array(3))).toBe(false);
    expect(isS3Ref(null)).toBe(false);
  });

  it('lays keys out by run, kind and entity', () => {
    const key = (PK: string, SK: string, attribute: string) =>
      objectKeyPrefix({ PK, SK }, attribute);
    expect(key(`RUN#${runId}`, 'META', 'input')).toBe(
      `${runId}/run/${runId}/input`
    );
    expect(key(`RUN#${runId}`, 'EVT#evnt_0000000001', 'eventData')).toBe(
      `${runId}/event/evnt_0000000001/eventData`
    );
    expect(key(`RUN#${runId}`, 'STEP#step_1', 'output')).toBe(
      `${runId}/step/step_1/output`
    );
    expect(key(`RUN#${runId}`, 'HOOK#hook_1', 'metadata')).toBe(
      `${runId}/hook/hook_1/metadata`
    );
    expect(key(`RUN#${runId}`, 'SNAPSHOT', 'data')).toBe(
      `${runId}/snapshot/${runId}/data`
    );
    expect(key(`STREAM#${runId}#my/out`, 'CHUNK#0000000003', 'data')).toBe(
      `${runId}/stream/${encodeURIComponent('my/out/CHUNK#0000000003')}/data`
    );
  });
});
