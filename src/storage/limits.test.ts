import {
  DynamoDBClient,
  TransactionCanceledException,
} from '@aws-sdk/client-dynamodb';
import {
  BatchGetCommand,
  type BatchGetCommandInput,
  DynamoDBDocumentClient,
  GetCommand,
  type GetCommandInput,
  QueryCommand,
  type QueryCommandInput,
  TransactWriteCommand,
  type TransactWriteCommandInput,
} from '@aws-sdk/lib-dynamodb';
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

function runItem(id = runId, overrides: Record<string, unknown> = {}) {
  return {
    PK: `RUN#${id}`,
    SK: 'META',
    type: 'run',
    runId: id,
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

/** Serves GetItem from items keyed by `PK|SK`, and Query from `query`. */
function serve(
  items: Record<string, Record<string, unknown>>,
  query: (input: QueryCommandInput) => Record<string, unknown>[] = () => []
) {
  ddb.on(GetCommand).callsFake((input: GetCommandInput) => {
    const key = input.Key as { PK: string; SK: string };
    return { Item: items[`${key.PK}|${key.SK}`] };
  });
  ddb
    .on(QueryCommand)
    .callsFake((input: QueryCommandInput) => ({ Items: query(input) }));
}

function transactions(): TransactWriteCommandInput[] {
  return ddb
    .commandCalls(TransactWriteCommand)
    .map((call) => call.args[0].input);
}

/** The hooks of the run's partition, as the cleanup query sees them. */
function hookItems(count: number) {
  return Array.from({ length: count }, (_, i) => ({
    PK: `RUN#${runId}`,
    SK: `HOOK#hook_${i}`,
    runId,
    hookId: `hook_${i}`,
    token: `token_${i}`,
  }));
}

const runCompleted = {
  eventType: 'run_completed' as const,
  eventData: { output: new Uint8Array([1]) },
};

describe('DynamoDB request limits', () => {
  beforeEach(() => {
    ddb.reset();
  });

  it('sends no Limit outside 32 bits: an unbounded query leaves it out', async () => {
    serve({ [`RUN#${runId}|META`]: runItem() });
    ddb.on(TransactWriteCommand).resolves({});

    const storage = createStorage({ documents, tableName: 'wf' });
    await storage.events.create(runId, runCompleted);

    const queries = ddb
      .commandCalls(QueryCommand)
      .map((call) => call.args[0].input);
    // The end-of-run cleanup reads the run's hooks and waits.
    expect(queries.length).toBeGreaterThan(0);
    for (const query of queries) {
      if (query.Limit !== undefined) {
        expect(query.Limit).toBeLessThan(2 ** 31);
      }
    }
    const cleanup = queries.filter(
      (query) => query.ExpressionAttributeValues?.[':prefix'] === 'HOOK#'
    );
    expect(cleanup).toHaveLength(1);
    expect(cleanup[0]).not.toHaveProperty('Limit');
  });

  it('answers a committed terminal event even when its deferred cleanup fails', async () => {
    // 40 hooks are 120 deletes: over the 100 items of one transaction, so
    // part of the cleanup runs after the commit, and that part fails.
    serve({ [`RUN#${runId}|META`]: runItem() }, (input) =>
      input.ExpressionAttributeValues?.[':prefix'] === 'HOOK#'
        ? hookItems(40)
        : []
    );
    // Each hook's token claim still names it, so it is deleted too.
    ddb.on(BatchGetCommand).callsFake((input: BatchGetCommandInput) => ({
      Responses: {
        wf: (input.RequestItems?.wf?.Keys ?? []).map((key) => {
          const token = String(key.PK).slice('HOOKTOKEN#'.length);
          return {
            ...key,
            runId,
            hookId: `hook_${token.slice('token_'.length)}`,
          };
        }),
      },
    }));
    ddb
      .on(TransactWriteCommand)
      .resolvesOnce({})
      .rejects(new Error('socket hang up'));

    const storage = createStorage({ documents, tableName: 'wf' });
    const result = await storage.events.create(runId, runCompleted);

    expect(result.run?.status).toBe('completed');
    expect(result.event?.eventType).toBe('run_completed');
    expect(transactions().length).toBeGreaterThan(1);
  });

  it('refuses an update that would grow the run item over the limit, before writing', async () => {
    serve({
      [`RUN#${runId}|META`]: runItem(runId, {
        input: new Uint8Array(200 * 1024),
      }),
    });
    const storage = createStorage({ documents, tableName: 'wf' });

    // Settled to a small value: a resolved result holds 400 KB of bytes.
    const outcome = await storage.events
      .create(runId, {
        eventType: 'run_completed',
        eventData: { output: new Uint8Array(200 * 1024) },
      })
      .then(
        () => 'committed',
        (error: { name?: string; status?: number; code?: string }) => ({
          name: error.name,
          status: error.status,
          code: error.code,
        })
      );
    expect(outcome).toEqual({
      name: 'PayloadTooLargeError',
      status: 413,
      code: 'PAYLOAD_TOO_LARGE',
    });
    expect(transactions()).toHaveLength(0);
  });

  it('turns an item-size cancellation from DynamoDB into PayloadTooLargeError', async () => {
    serve({ [`RUN#${runId}|META`]: runItem() });
    ddb.on(TransactWriteCommand).rejects(
      new TransactionCanceledException({
        message: 'Transaction cancelled',
        $metadata: {},
        CancellationReasons: [
          {
            Code: 'ValidationError',
            Message: 'Item size has exceeded the maximum allowed size',
          },
          { Code: 'None' },
        ],
      })
    );
    const storage = createStorage({ documents, tableName: 'wf' });

    await expect(
      storage.events.create(runId, runCompleted)
    ).rejects.toMatchObject({
      name: 'PayloadTooLargeError',
      status: 413,
      code: 'PAYLOAD_TOO_LARGE',
    });
    expect(transactions()).toHaveLength(1);
  });
});
