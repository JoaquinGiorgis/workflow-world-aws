import {
  CreateTableCommand,
  DescribeTableCommand,
  type DescribeTableCommandOutput,
  DynamoDBClient,
  type IndexStatus,
  ResourceInUseException,
  ScanCommand,
  UpdateItemCommand,
  UpdateTableCommand,
  UpdateTimeToLiveCommand,
} from '@aws-sdk/client-dynamodb';
import {
  BucketAlreadyOwnedByYou,
  CreateBucketCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { mockClient } from 'aws-sdk-client-mock';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createClients, destroyClients } from '../src/clients.js';
import {
  type BootstrapOptions,
  bootstrapLocal,
  optionsFromEnv,
} from './bootstrap-local.js';

const dynamodb = mockClient(DynamoDBClient);
const sqs = mockClient(SQSClient);
const s3 = mockClient(S3Client);

const endpoint = 'http://localhost:4566';
const base: Omit<BootstrapOptions, 'clients'> = {
  region: 'us-east-1',
  endpoint,
  tableName: 'workflow',
  workflowQueueName: 'workflow',
  stepQueueName: 'step',
  bucket: 'workflow-payloads',
};

const queueUrl = (name: string) => `${endpoint}/000000000000/${name}`;
const queueArn = (name: string) => `arn:aws:sqs:us-east-1:000000000000:${name}`;

let clients: ReturnType<typeof createClients>;

beforeEach(() => {
  dynamodb.reset();
  sqs.reset();
  s3.reset();

  dynamodb.on(CreateTableCommand).resolves({});
  dynamodb.on(DescribeTableCommand).resolves({
    Table: {
      TableName: 'workflow',
      TableStatus: 'ACTIVE',
      GlobalSecondaryIndexes: ['GSI1', 'GSI2', 'GSI3'].map((IndexName) => ({
        IndexName,
        IndexStatus: 'ACTIVE',
      })),
    },
  });
  dynamodb.on(UpdateTimeToLiveCommand).resolves({});
  sqs.on(CreateQueueCommand).callsFake((input: { QueueName: string }) => ({
    QueueUrl: queueUrl(input.QueueName),
  }));
  sqs
    .on(GetQueueAttributesCommand)
    .callsFake((input: { QueueUrl: string }) => ({
      Attributes: {
        QueueArn: queueArn(input.QueueUrl.split('/').pop() ?? ''),
      },
    }));
  s3.on(CreateBucketCommand).resolves({});

  clients = createClients({ region: 'us-east-1', endpoint });
});

afterEach(() => {
  destroyClients(clients);
});

describe('bootstrapLocal', () => {
  it('creates the table with its three GSIs, on demand', async () => {
    await bootstrapLocal({ ...base, clients });

    const [create] = dynamodb.commandCalls(CreateTableCommand);
    const input = create?.args[0].input;
    expect(input?.TableName).toBe('workflow');
    expect(input?.BillingMode).toBe('PAY_PER_REQUEST');
    expect(input?.KeySchema).toEqual([
      { AttributeName: 'PK', KeyType: 'HASH' },
      { AttributeName: 'SK', KeyType: 'RANGE' },
    ]);
    expect(input?.GlobalSecondaryIndexes?.map((gsi) => gsi.IndexName)).toEqual([
      'GSI1',
      'GSI2',
      'GSI3',
    ]);
    expect(input?.GlobalSecondaryIndexes?.[0]?.KeySchema).toEqual([
      { AttributeName: 'GSI1PK', KeyType: 'HASH' },
      { AttributeName: 'GSI1SK', KeyType: 'RANGE' },
    ]);
    expect(input?.GlobalSecondaryIndexes?.[2]?.KeySchema).toEqual([
      { AttributeName: 'GSI3PK', KeyType: 'HASH' },
      { AttributeName: 'GSI3SK', KeyType: 'RANGE' },
    ]);
    expect(input?.ProvisionedThroughput).toBeUndefined();
  });

  it('enables TTL on expiresAt', async () => {
    await bootstrapLocal({ ...base, clients });

    expect(
      dynamodb.commandCalls(UpdateTimeToLiveCommand)[0]?.args[0].input
    ).toEqual({
      TableName: 'workflow',
      TimeToLiveSpecification: { AttributeName: 'expiresAt', Enabled: true },
    });
  });

  it('creates each queue after its dead-letter queue, with a redrive policy', async () => {
    const result = await bootstrapLocal({
      ...base,
      clients,
      maxReceiveCount: 7,
    });

    const created = sqs
      .commandCalls(CreateQueueCommand)
      .map((call) => call.args[0].input);
    expect(created.map((input) => input.QueueName)).toEqual([
      'workflow-dlq',
      'workflow',
      'step-dlq',
      'step',
    ]);

    const workflow = created[1];
    expect(JSON.parse(workflow?.Attributes?.RedrivePolicy ?? '{}')).toEqual({
      deadLetterTargetArn: queueArn('workflow-dlq'),
      maxReceiveCount: '7',
    });
    const step = created[3];
    expect(JSON.parse(step?.Attributes?.RedrivePolicy ?? '{}')).toEqual({
      deadLetterTargetArn: queueArn('step-dlq'),
      maxReceiveCount: '7',
    });

    expect(result).toEqual({
      tableName: 'workflow',
      workflowQueueUrl: queueUrl('workflow'),
      workflowDeadLetterQueueUrl: queueUrl('workflow-dlq'),
      stepQueueUrl: queueUrl('step'),
      stepDeadLetterQueueUrl: queueUrl('step-dlq'),
      bucket: 'workflow-payloads',
    });
  });

  it('lets SQS retry for days before the dead-letter queue, by default', async () => {
    await bootstrapLocal({ ...base, clients });
    const workflow = sqs
      .commandCalls(CreateQueueCommand)
      .map((call) => call.args[0].input)
      .find((input) => input.QueueName === 'workflow');
    expect(workflow?.Attributes?.MessageRetentionPeriod).toBe('1209600');
    expect(
      JSON.parse(workflow?.Attributes?.RedrivePolicy ?? '{}').maxReceiveCount
    ).toBe('73');
  });

  it('creates the bucket without a location constraint in us-east-1', async () => {
    await bootstrapLocal({ ...base, clients });

    expect(s3.commandCalls(CreateBucketCommand)[0]?.args[0].input).toEqual({
      Bucket: 'workflow-payloads',
    });
  });

  it('sets the location constraint in other regions', async () => {
    await bootstrapLocal({ ...base, region: 'eu-west-1', clients });

    expect(s3.commandCalls(CreateBucketCommand)[0]?.args[0].input).toEqual({
      Bucket: 'workflow-payloads',
      CreateBucketConfiguration: { LocationConstraint: 'eu-west-1' },
    });
  });

  it('keeps resources that already exist', async () => {
    dynamodb
      .on(CreateTableCommand)
      .rejects(
        new ResourceInUseException({ message: 'Table exists', $metadata: {} })
      );
    dynamodb
      .on(UpdateTimeToLiveCommand)
      .rejects(new Error('TimeToLive is already enabled'));
    s3.on(CreateBucketCommand).rejects(
      new BucketAlreadyOwnedByYou({ message: 'Owned', $metadata: {} })
    );

    await expect(bootstrapLocal({ ...base, clients })).resolves.toMatchObject({
      tableName: 'workflow',
    });
  });

  it('keeps the GSIs of a table that has them all', async () => {
    dynamodb
      .on(CreateTableCommand)
      .rejects(
        new ResourceInUseException({ message: 'Table exists', $metadata: {} })
      );

    await bootstrapLocal({ ...base, clients });

    expect(dynamodb.commandCalls(UpdateTableCommand)).toHaveLength(0);
    expect(dynamodb.commandCalls(ScanCommand)).toHaveLength(0);
  });

  it('adds GSI3 to a table created before it, waits for it and backfills the active runs', async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true, advanceTimeDelta: 50 });
    try {
      dynamodb
        .on(CreateTableCommand)
        .rejects(
          new ResourceInUseException({ message: 'Table exists', $metadata: {} })
        );
      const table = (
        indexes: [string, IndexStatus][]
      ): DescribeTableCommandOutput => ({
        $metadata: {},
        Table: {
          TableName: 'workflow',
          TableStatus: 'ACTIVE',
          GlobalSecondaryIndexes: indexes.map(([IndexName, IndexStatus]) => ({
            IndexName,
            IndexStatus,
          })),
        },
      });
      const before: [string, IndexStatus][] = [
        ['GSI1', 'ACTIVE'],
        ['GSI2', 'ACTIVE'],
      ];
      dynamodb
        .on(DescribeTableCommand)
        .resolvesOnce(table(before))
        .resolvesOnce(table(before))
        .resolvesOnce(table([...before, ['GSI3', 'CREATING']]))
        .resolves(table([...before, ['GSI3', 'ACTIVE']]));
      dynamodb.on(UpdateTableCommand).resolves({});
      const meta = (runId: string, status: string, extra = {}) => ({
        PK: { S: `RUN#${runId}` },
        SK: { S: 'META' },
        runId: { S: runId },
        status: { S: status },
        ...extra,
      });
      dynamodb
        .on(ScanCommand)
        .resolvesOnce({
          Items: [meta('wrun_A', 'running')],
          LastEvaluatedKey: { PK: { S: 'RUN#wrun_A' }, SK: { S: 'META' } },
        })
        .resolves({ Items: [meta('wrun_B', 'pending')] });
      dynamodb.on(UpdateItemCommand).resolves({});

      await bootstrapLocal({ ...base, clients });

      const [update] = dynamodb.commandCalls(UpdateTableCommand);
      expect(update?.args[0].input).toEqual({
        TableName: 'workflow',
        AttributeDefinitions: [
          { AttributeName: 'GSI3PK', AttributeType: 'S' },
          { AttributeName: 'GSI3SK', AttributeType: 'S' },
        ],
        GlobalSecondaryIndexUpdates: [
          {
            Create: {
              IndexName: 'GSI3',
              KeySchema: [
                { AttributeName: 'GSI3PK', KeyType: 'HASH' },
                { AttributeName: 'GSI3SK', KeyType: 'RANGE' },
              ],
              Projection: { ProjectionType: 'ALL' },
            },
          },
        ],
      });
      // Waited until the index was ACTIVE.
      expect(
        dynamodb.commandCalls(DescribeTableCommand).length
      ).toBeGreaterThanOrEqual(4);

      // The backfill scans the run items that have not ended and puts each
      // in GSI3, unless it ended or got the attributes meanwhile.
      const scans = dynamodb.commandCalls(ScanCommand);
      expect(scans).toHaveLength(2);
      expect(scans[0]?.args[0].input.FilterExpression).toContain(
        'attribute_not_exists(GSI3PK)'
      );
      expect(scans[1]?.args[0].input.ExclusiveStartKey).toEqual({
        PK: { S: 'RUN#wrun_A' },
        SK: { S: 'META' },
      });
      const updates = dynamodb
        .commandCalls(UpdateItemCommand)
        .map((call) => call.args[0].input);
      expect(updates.map((input) => input.Key)).toEqual([
        { PK: { S: 'RUN#wrun_A' }, SK: { S: 'META' } },
        { PK: { S: 'RUN#wrun_B' }, SK: { S: 'META' } },
      ]);
      expect(updates[0]?.UpdateExpression).toBe(
        'SET GSI3PK = :pk, GSI3SK = :runId'
      );
      expect(updates[0]?.ExpressionAttributeValues?.[':pk']).toEqual({
        S: 'ACTIVE',
      });
      expect(updates[0]?.ExpressionAttributeValues?.[':runId']).toEqual({
        S: 'wrun_A',
      });
      expect(updates[0]?.ConditionExpression).toContain(
        'attribute_not_exists(GSI3PK)'
      );
    } finally {
      vi.useRealTimers();
    }
  });

  it('surfaces any other error', async () => {
    dynamodb.on(CreateTableCommand).rejects(new Error('AccessDenied'));

    await expect(bootstrapLocal({ ...base, clients })).rejects.toThrow(
      'AccessDenied'
    );
  });
});

describe('optionsFromEnv', () => {
  it('refuses to run without an emulator endpoint', () => {
    expect(() => optionsFromEnv({})).toThrow(/WORKFLOW_AWS_ENDPOINT/);
  });

  it('fills in local defaults', () => {
    expect(optionsFromEnv({ WORKFLOW_AWS_ENDPOINT: endpoint })).toEqual({
      endpoint,
      region: 'us-east-1',
      tableName: 'workflow',
      workflowQueueName: 'workflow',
      stepQueueName: 'step',
      bucket: 'workflow-payloads',
    });
  });
});
