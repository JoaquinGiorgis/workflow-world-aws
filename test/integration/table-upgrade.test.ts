/**
 * A table created by an older bootstrap (before GSI3, the runs that have
 * not ended) against the emulator: the World names the missing index, and
 * the bootstrap adds it and backfills the runs already in the table.
 */
import { randomUUID } from 'node:crypto';
import {
  CreateTableCommand,
  DeleteTableCommand,
  DescribeTableCommand,
  waitUntilTableExists,
} from '@aws-sdk/client-dynamodb';
import { GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { afterAll, beforeAll, describe, expect, inject, it } from 'vitest';
import { createWorkflowTable } from '../../scripts/bootstrap-local.js';
import {
  type AwsClients,
  createClients,
  destroyClients,
} from '../../src/clients.js';
import { createWorld } from '../../src/index.js';

const aws = inject('aws');

describe('a table created before GSI3', () => {
  let clients: AwsClients;
  const tableName = `wf-upgrade-${randomUUID().slice(0, 8)}`;

  const run = (runId: string, status: string) => ({
    PK: `RUN#${runId}`,
    SK: 'META',
    type: 'run',
    runId,
    status,
    deploymentId: 'aws',
    workflowName: 'wf',
    specVersion: 8,
    input: new Uint8Array([1]),
    createdAt: '2026-11-15T20:00:00.000Z',
    updatedAt: '2026-11-15T20:00:00.000Z',
    GSI1PK: 'RUNS',
    GSI1SK: runId,
    GSI2PK: 'WF#wf',
    GSI2SK: runId,
    seq: 1,
    seqInBand: 1,
    rev: 1,
  });

  beforeAll(async () => {
    clients = createClients({ region: aws.region, endpoint: aws.endpoint });
    // The table as M1's bootstrap created it: GSI1 and GSI2 only.
    await clients.dynamodb.send(
      new CreateTableCommand({
        TableName: tableName,
        BillingMode: 'PAY_PER_REQUEST',
        AttributeDefinitions: [
          'PK',
          'SK',
          'GSI1PK',
          'GSI1SK',
          'GSI2PK',
          'GSI2SK',
        ].map((AttributeName) => ({ AttributeName, AttributeType: 'S' })),
        KeySchema: [
          { AttributeName: 'PK', KeyType: 'HASH' },
          { AttributeName: 'SK', KeyType: 'RANGE' },
        ],
        GlobalSecondaryIndexes: ['GSI1', 'GSI2'].map((IndexName) => ({
          IndexName,
          KeySchema: [
            { AttributeName: `${IndexName}PK`, KeyType: 'HASH' },
            { AttributeName: `${IndexName}SK`, KeyType: 'RANGE' },
          ],
          Projection: { ProjectionType: 'ALL' },
        })),
      })
    );
    await waitUntilTableExists(
      { client: clients.dynamodb, maxWaitTime: 60 },
      { TableName: tableName }
    );
    for (const item of [
      run('wrun_01JD00000000000000000000A1', 'running'),
      run('wrun_01JD00000000000000000000A2', 'pending'),
      run('wrun_01JD00000000000000000000A3', 'completed'),
    ]) {
      await clients.documents.send(
        new PutCommand({ TableName: tableName, Item: item })
      );
    }
  });

  afterAll(async () => {
    await clients.dynamodb.send(
      new DeleteTableCommand({ TableName: tableName })
    );
    destroyClients(clients);
  });

  const world = () =>
    createWorld({
      region: aws.region,
      endpoint: aws.endpoint,
      tableName,
      workflowQueueUrl: aws.workflowQueueUrl,
      stepQueueUrl: aws.stepQueueUrl,
      clients,
    });

  it('names the missing index instead of failing with a bare DynamoDB error', async () => {
    const before = world();
    try {
      await expect(
        before.runs.list({ status: 'running' })
      ).rejects.toMatchObject({
        name: 'MissingTableIndexError',
        indexName: 'GSI3',
        tableName,
      });
    } finally {
      await before.close();
    }
  });

  it('gets GSI3 and its active runs from the bootstrap', async () => {
    await createWorkflowTable(clients, tableName);

    const { Table } = await clients.dynamodb.send(
      new DescribeTableCommand({ TableName: tableName })
    );
    expect(
      Table?.GlobalSecondaryIndexes?.map((index) => [
        index.IndexName,
        index.IndexStatus,
      ])
    ).toContainEqual(['GSI3', 'ACTIVE']);
    const completed = await clients.documents.send(
      new GetCommand({
        TableName: tableName,
        Key: { PK: 'RUN#wrun_01JD00000000000000000000A3', SK: 'META' },
      })
    );
    expect(completed.Item?.GSI3PK).toBeUndefined();

    const after = world();
    try {
      const listed = await after.runs.list({
        status: ['pending', 'running'],
        pagination: { sortOrder: 'asc' },
      });
      expect(listed.data.map((each) => each.runId)).toEqual([
        'wrun_01JD00000000000000000000A1',
        'wrun_01JD00000000000000000000A2',
      ]);
    } finally {
      await after.close();
    }

    // Running it again changes nothing.
    await expect(createWorkflowTable(clients, tableName)).resolves.toBe(
      undefined
    );
  });
});
