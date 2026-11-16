import {
  CreateTableCommand,
  DescribeTableCommand,
  DynamoDBClient,
  ResourceInUseException,
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
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
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
  dynamodb
    .on(DescribeTableCommand)
    .resolves({ Table: { TableName: 'workflow', TableStatus: 'ACTIVE' } });
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
