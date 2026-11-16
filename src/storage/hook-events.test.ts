import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
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
const otherRunId = 'wrun_01JD000000000000000000000B';

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

describe('forced hook takeover', () => {
  beforeEach(() => {
    ddb.reset();
  });

  it('takes over a token whose hook id the claimer reuses with one write per item', async () => {
    // The victim, another run, holds the token with the same hook id the
    // claimer asks for: the hook id pointer is re-pointed, not deleted and
    // put in the same transaction (DynamoDB refuses two operations on one
    // item).
    serve({
      [`RUN#${runId}|META`]: runItem(),
      [`RUN#${otherRunId}|META`]: runItem(otherRunId),
      'HOOKTOKEN#shared|TOKEN': {
        PK: 'HOOKTOKEN#shared',
        SK: 'TOKEN',
        runId: otherRunId,
        hookId: 'hook_same',
      },
      'HOOKID#hook_same|HOOK': {
        PK: 'HOOKID#hook_same',
        SK: 'HOOK',
        runId: otherRunId,
        token: 'shared',
      },
      [`RUN#${otherRunId}|HOOK#hook_same`]: {
        PK: `RUN#${otherRunId}`,
        SK: 'HOOK#hook_same',
        runId: otherRunId,
        hookId: 'hook_same',
        token: 'shared',
        ownerId: '',
        projectId: '',
        environment: '',
        createdAt: now,
      },
    });
    ddb.on(TransactWriteCommand).resolves({});

    const storage = createStorage({ documents, tableName: 'wf' });
    const result = await storage.events.create(runId, {
      eventType: 'hook_created',
      correlationId: 'hook_same',
      eventData: { token: 'shared', force: true },
    });
    expect(result.event?.eventType).toBe('hook_created');

    const [transaction] = transactions();
    const keys = (transaction?.TransactItems ?? []).map((entry) => {
      const [, body] = Object.entries(entry)[0] as [
        string,
        { Key?: { PK: string; SK: string }; Item?: { PK: string; SK: string } },
      ];
      const key = body.Key ?? body.Item;
      return `${key?.PK}|${key?.SK}`;
    });
    expect(new Set(keys).size).toBe(keys.length);
    const pointer = transaction?.TransactItems?.find(
      (entry) => entry.Put?.Item?.PK === 'HOOKID#hook_same'
    )?.Put;
    expect(pointer?.Item).toMatchObject({ runId });
    expect(pointer?.ExpressionAttributeValues).toMatchObject({
      ':runId': otherRunId,
    });
  });
});
