import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  QueryCommand,
  type QueryCommandInput,
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { afterAll, describe, expect, it } from 'vitest';
import { cursorOf, decodeCursor, encodeCursor } from './cursor.js';
import { createStorage } from './index.js';

const runsScope = 'GSI1:RUNS';

describe('pagination cursors', () => {
  it('round-trips a LastEvaluatedKey', () => {
    const key = {
      PK: 'RUN#wrun_01JD0000000000000000000000',
      SK: 'META',
      GSI1PK: 'RUNS',
      GSI1SK: 'wrun_01JD0000000000000000000000',
    };
    const cursor = encodeCursor(runsScope, key);
    expect(cursor).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(decodeCursor(runsScope, cursor)).toEqual(key);
  });

  it('keeps keys with any characters, such as a step id with "#"', () => {
    const scope = 'table:RUN#wrun_A:STEP#';
    const key = { PK: 'RUN#wrun_A', SK: 'STEP#a#b/ü' };
    expect(decodeCursor(scope, encodeCursor(scope, key))).toEqual(key);
  });

  it('builds the cursor of an item from its key and index attributes', () => {
    const scope = 'GSI2:WF#wf';
    const item = {
      PK: 'RUN#wrun_A',
      SK: 'META',
      GSI2PK: 'WF#wf',
      GSI2SK: 'wrun_A',
      status: 'pending',
      seq: 3,
    };
    expect(
      decodeCursor(scope, cursorOf(scope, item, ['GSI2PK', 'GSI2SK']))
    ).toEqual({
      PK: 'RUN#wrun_A',
      SK: 'META',
      GSI2PK: 'WF#wf',
      GSI2SK: 'wrun_A',
    });
    expect(decodeCursor('t', cursorOf('t', item))).toEqual({
      PK: 'RUN#wrun_A',
      SK: 'META',
    });
  });

  it.each([
    'not a cursor',
    encodeCursor(runsScope, { PK: 'RUN#x' } as never),
    Buffer.from('[1,2]').toString('base64url'),
    Buffer.from('{"s":"GSI1:RUNS","k":{"PK":"a","SK":1}}').toString(
      'base64url'
    ),
  ])('refuses %s with a 400', (cursor) => {
    expect(() => decodeCursor(runsScope, cursor)).toThrowError(
      expect.objectContaining({ name: 'WorkflowWorldError', status: 400 })
    );
  });

  it('refuses a cursor minted by another query with a 400', () => {
    const cursor = encodeCursor(runsScope, {
      PK: 'RUN#wrun_A',
      SK: 'META',
      GSI1PK: 'RUNS',
      GSI1SK: 'wrun_A',
    });
    expect(() => decodeCursor('GSI2:WF#wf', cursor)).toThrowError(
      expect.objectContaining({ name: 'WorkflowWorldError', status: 400 })
    );
  });
});

describe('cursors across lists', () => {
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

  it('refuses a runs.list cursor in steps.list before querying', async () => {
    ddb.on(QueryCommand).callsFake((input: QueryCommandInput) => ({
      Items: input.IndexName
        ? [1, 2].map((n) => ({
            PK: `RUN#wrun_${n}`,
            SK: 'META',
            GSI1PK: 'RUNS',
            GSI1SK: `wrun_${n}`,
            runId: `wrun_${n}`,
            status: 'pending',
            deploymentId: 'd',
            workflowName: 'w',
            createdAt: '2026-11-15T20:00:00.000Z',
            updatedAt: '2026-11-15T20:00:00.000Z',
          }))
        : [],
    }));
    const storage = createStorage({ documents, tableName: 'wf' });
    const page = await storage.runs.list({ pagination: { limit: 1 } });
    expect(page.cursor).toBeTruthy();
    const queries = ddb.commandCalls(QueryCommand).length;

    await expect(
      storage.steps.list({
        runId: 'wrun_1',
        pagination: { cursor: page.cursor ?? undefined },
      })
    ).rejects.toMatchObject({ name: 'WorkflowWorldError', status: 400 });
    // Also a runs.list cursor on another workflow's query.
    await expect(
      storage.runs.list({
        workflowName: 'other',
        pagination: { cursor: page.cursor ?? undefined },
      })
    ).rejects.toMatchObject({ name: 'WorkflowWorldError', status: 400 });
    expect(ddb.commandCalls(QueryCommand)).toHaveLength(queries);
  });
});
