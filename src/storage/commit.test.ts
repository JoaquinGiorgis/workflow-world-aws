import {
  DynamoDBClient,
  TransactionCanceledException,
} from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  type GetCommandInput,
  QueryCommand,
  TransactWriteCommand,
  type TransactWriteCommandInput,
} from '@aws-sdk/lib-dynamodb';
import { slotToEventId } from '@workflow/world';
import { mockClient } from 'aws-sdk-client-mock';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createStorage } from './index.js';

const documents = DynamoDBDocumentClient.from(
  new DynamoDBClient({
    region: 'us-east-1',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  })
);
const ddb = mockClient(documents);

afterAll(() => {
  ddb.restore();
});

const now = '2026-11-15T20:00:00.000Z';
const runId = 'wrun_01JD000000000000000000000A';

function runItem(overrides: Record<string, unknown> = {}) {
  return {
    PK: `RUN#${runId}`,
    SK: 'META',
    type: 'run',
    runId,
    status: 'running',
    deploymentId: 'dpl',
    workflowName: 'wf',
    specVersion: 8,
    input: new Uint8Array(),
    createdAt: now,
    updatedAt: now,
    seq: 2,
    seqInBand: 1,
    rev: 5,
    ...overrides,
  };
}

/** Serves GetItem from a table of items keyed by `PK|SK`. */
function serve(items: Record<string, Record<string, unknown>>) {
  ddb.on(GetCommand).callsFake((input: GetCommandInput) => {
    const key = input.Key as { PK: string; SK: string };
    return { Item: items[`${key.PK}|${key.SK}`] };
  });
  // Event lookups (duplicate checks) find nothing.
  ddb.on(QueryCommand).resolves({ Items: [] });
}

function cancelled(reasons: Record<string, unknown>[]) {
  return new TransactionCanceledException({
    message: 'Transaction cancelled',
    $metadata: {},
    CancellationReasons: reasons as never,
  });
}

function transactions(): TransactWriteCommandInput[] {
  return ddb
    .commandCalls(TransactWriteCommand)
    .map((call) => call.args[0].input);
}

const stepCreated = {
  eventType: 'step_created' as const,
  correlationId: 'step_1',
  eventData: { stepName: 'add', input: new Uint8Array([1]) },
};

describe('the event commit', () => {
  beforeEach(() => {
    ddb.reset();
  });

  it('retries on fresh reads when another writer moved the run counter', async () => {
    let rev = 5;
    let seq = 2;
    ddb.on(GetCommand).callsFake((input: GetCommandInput) => {
      const key = input.Key as { PK: string; SK: string };
      return key.SK === 'META' ? { Item: runItem({ rev, seq }) } : {};
    });
    ddb.on(QueryCommand).resolves({ Items: [] });
    ddb
      .on(TransactWriteCommand)
      .callsFakeOnce(() => {
        // Another writer committed slot 3 in between.
        rev = 6;
        seq = 3;
        throw cancelled([
          { Code: 'ConditionalCheckFailed' },
          { Code: 'None' },
          { Code: 'None' },
        ]);
      })
      .resolves({});

    const storage = createStorage({ documents, tableName: 'wf' });
    const result = await storage.events.create(runId, stepCreated);

    const [first, second] = transactions();
    expect(transactions()).toHaveLength(2);
    // Each attempt is conditioned on the counter it read.
    const update = (input?: TransactWriteCommandInput) =>
      input?.TransactItems?.[0]?.Update;
    expect(update(first)?.ConditionExpression).toBe('#rev = :rev');
    expect(update(first)?.ExpressionAttributeValues?.[':rev']).toBe(5);
    expect(update(second)?.ExpressionAttributeValues?.[':rev']).toBe(6);
    expect(update(second)?.ExpressionAttributeValues?.[':seq']).toBe(4);
    // The event takes the slot after the one the other writer took, and its
    // put cannot overwrite an event.
    const put = second?.TransactItems?.[1]?.Put;
    expect(put?.Item?.SK).toBe(`EVT#${slotToEventId(4)}`);
    expect(put?.ConditionExpression).toBe('attribute_not_exists(PK)');
    expect(result.event?.eventId).toBe(slotToEventId(4));
    expect(result.step?.status).toBe('pending');
  });

  it('gives up with a 503 after the configured attempts', async () => {
    serve({ [`RUN#${runId}|META`]: runItem() });
    ddb
      .on(TransactWriteCommand)
      .rejects(cancelled([{ Code: 'ConditionalCheckFailed' }]));

    const storage = createStorage({
      documents,
      tableName: 'wf',
      retry: {
        maxAttempts: 3,
        immediateAttempts: 1,
        baseDelayMs: 1,
        maxDelayMs: 2,
      },
    });
    await expect(
      storage.events.create(runId, stepCreated)
    ).rejects.toMatchObject({ name: 'WorkflowWorldError', status: 503 });
    expect(transactions()).toHaveLength(3);
  });

  it('retries a conflict with another transaction in flight', async () => {
    serve({ [`RUN#${runId}|META`]: runItem() });
    ddb
      .on(TransactWriteCommand)
      .rejectsOnce(cancelled([{ Code: 'TransactionConflict' }]))
      .resolves({});

    const storage = createStorage({ documents, tableName: 'wf' });
    await expect(
      storage.events.create(runId, stepCreated)
    ).resolves.toBeDefined();
    expect(transactions()).toHaveLength(2);
  });

  it('does not retry a cancellation no new decision can fix', async () => {
    serve({ [`RUN#${runId}|META`]: runItem() });
    ddb
      .on(TransactWriteCommand)
      .rejects(cancelled([{ Code: 'ValidationError', Message: 'bad' }]));

    const storage = createStorage({ documents, tableName: 'wf' });
    await expect(
      storage.events.create(runId, stepCreated)
    ).rejects.toMatchObject({ name: 'TransactionCanceledException' });
    expect(transactions()).toHaveLength(1);
  });

  it('answers hook_conflict naming the owner a cancelled token claim returned', async () => {
    const otherRun = 'wrun_01JD000000000000000000000B';
    const items: Record<string, Record<string, unknown>> = {
      [`RUN#${runId}|META`]: runItem(),
    };
    serve(items);
    ddb
      .on(TransactWriteCommand)
      .callsFakeOnce(() => {
        // Another run claimed the token after this writer read it free. The
        // cancellation carries the claim the condition saw (ALL_OLD), as
        // raw attribute values.
        items[`RUN#${otherRun}|META`] = { ...runItem(), runId: otherRun };
        items[`RUN#${otherRun}|HOOK#hook_theirs`] = {
          runId: otherRun,
          hookId: 'hook_theirs',
          token: 'shared',
        };
        throw cancelled([
          { Code: 'None' },
          { Code: 'None' },
          {
            Code: 'ConditionalCheckFailed',
            Item: {
              PK: { S: 'HOOKTOKEN#shared' },
              SK: { S: 'TOKEN' },
              runId: { S: otherRun },
              hookId: { S: 'hook_theirs' },
            },
          },
          { Code: 'None' },
          { Code: 'None' },
        ]);
      })
      .resolves({});

    const storage = createStorage({ documents, tableName: 'wf' });
    const result = await storage.events.create(runId, {
      eventType: 'hook_created',
      correlationId: 'hook_mine',
      eventData: { token: 'shared' },
    });

    expect(result.event?.eventType).toBe('hook_conflict');
    expect(result.event?.eventData).toEqual({
      token: 'shared',
      conflictingRunId: otherRun,
    });
    expect(result.hook).toBeUndefined();
    // The owner came from the cancellation reason, not from a second read
    // of the token claim.
    const tokenReads = ddb
      .commandCalls(GetCommand)
      .filter((call) => call.args[0].input.Key?.PK === 'HOOKTOKEN#shared');
    expect(tokenReads).toHaveLength(1);
    // The first attempt asked DynamoDB for the claim it lost to.
    const [first] = transactions();
    expect(first?.TransactItems?.[2]?.Put).toMatchObject({
      ConditionExpression: 'attribute_not_exists(PK)',
      ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
    });
  });

  it('refuses a payload over the item limit before writing anything', async () => {
    serve({ [`RUN#${runId}|META`]: runItem() });
    const storage = createStorage({ documents, tableName: 'wf' });
    await expect(
      storage.events.create(runId, {
        ...stepCreated,
        eventData: { stepName: 'add', input: new Uint8Array(400 * 1024) },
      })
    ).rejects.toMatchObject({
      name: 'PayloadTooLargeError',
      status: 413,
      code: 'PAYLOAD_TOO_LARGE',
    });
    expect(transactions()).toHaveLength(0);
  });

  it('refuses a stale in-band write before deciding anything', async () => {
    serve({ [`RUN#${runId}|META`]: runItem({ seqInBand: 3 }) });
    const storage = createStorage({ documents, tableName: 'wf' });
    await expect(
      storage.events.create(runId, stepCreated, {
        inBand: true,
        expectedSeqInBand: 2,
      })
    ).rejects.toMatchObject({ name: 'InBandSupersededError', status: 412 });
    expect(transactions()).toHaveLength(0);
  });

  it('takes a run out of the active-runs index in the write that ends it', async () => {
    serve({ [`RUN#${runId}|META`]: runItem() });
    ddb.on(TransactWriteCommand).resolves({});
    const storage = createStorage({ documents, tableName: 'wf' });
    await storage.events.create(runId, {
      eventType: 'run_completed',
      eventData: { output: new Uint8Array([1]) },
    });

    const update = transactions()[0]?.TransactItems?.[0]?.Update;
    const names = update?.ExpressionAttributeNames ?? {};
    const removed = (update?.UpdateExpression ?? '')
      .split(' REMOVE ')[1]
      ?.split(', ')
      .map((name) => names[name]);
    expect(removed).toEqual(['GSI3PK', 'GSI3SK']);
  });

  it('keeps a started run in the active-runs index', async () => {
    serve({ [`RUN#${runId}|META`]: runItem({ status: 'pending' }) });
    ddb.on(TransactWriteCommand).resolves({});
    const storage = createStorage({ documents, tableName: 'wf' });
    await storage.events.create(runId, { eventType: 'run_started' });

    const update = transactions()[0]?.TransactItems?.[0]?.Update;
    expect(update?.UpdateExpression).not.toContain('REMOVE');
    expect(Object.values(update?.ExpressionAttributeValues ?? {})).toEqual(
      expect.arrayContaining(['ACTIVE', runId, 'running'])
    );
  });

  it('lists only active runs from the sparse index', async () => {
    ddb.on(QueryCommand).resolves({ Items: [runItem()] });
    const storage = createStorage({ documents, tableName: 'wf' });
    const page = await storage.runs.list({
      status: ['pending', 'running'],
      resolveData: 'none',
    });

    expect(page.data.map((run) => run.runId)).toEqual([runId]);
    const query = ddb.commandCalls(QueryCommand)[0]?.args[0].input;
    expect(query?.IndexName).toBe('GSI3');
    expect(query?.KeyConditionExpression).toBe('GSI3PK = :pk');
    expect(query?.ExpressionAttributeValues?.[':pk']).toBe('ACTIVE');
  });

  it('reads all runs for a terminal status', async () => {
    ddb.on(QueryCommand).resolves({ Items: [] });
    const storage = createStorage({ documents, tableName: 'wf' });
    await storage.runs.list({ status: ['completed', 'running'] });
    expect(ddb.commandCalls(QueryCommand)[0]?.args[0].input.IndexName).toBe(
      'GSI1'
    );
  });
});
