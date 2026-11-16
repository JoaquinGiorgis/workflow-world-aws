import { DescribeTimeToLiveCommand } from '@aws-sdk/client-dynamodb';
import { GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import {
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import { slotToEventId } from '@workflow/world';
import { afterAll, describe, expect, inject, it } from 'vitest';
import { createClients, destroyClients } from '../../src/clients.js';
import { resolveConfig } from '../../src/config.js';
import { eventKey, runMetaKey } from '../../src/keys.js';

const aws = inject('aws');

const config = resolveConfig({
  WORKFLOW_AWS_REGION: aws.region,
  WORKFLOW_AWS_TABLE: aws.tableName,
  WORKFLOW_AWS_WORKFLOW_QUEUE_URL: aws.workflowQueueUrl,
  WORKFLOW_AWS_STEP_QUEUE_URL: aws.stepQueueUrl,
  WORKFLOW_AWS_BUCKET: aws.bucket,
  WORKFLOW_AWS_ENDPOINT: aws.endpoint,
});
const clients = createClients(config);

afterAll(() => {
  destroyClients(clients);
});

describe('smoke test against the local emulator', () => {
  it('puts and gets items in DynamoDB with the table keys', async () => {
    const runId = 'wrun_SMOKE';
    await clients.documents.send(
      new PutCommand({
        TableName: config.tableName,
        Item: { ...runMetaKey(runId), status: 'pending', nextSlot: 2 },
      })
    );
    await clients.documents.send(
      new PutCommand({
        TableName: config.tableName,
        Item: {
          ...eventKey(runId, slotToEventId(1)),
          eventType: 'run_created',
        },
        ConditionExpression: 'attribute_not_exists(SK)',
      })
    );

    const run = await clients.documents.send(
      new GetCommand({
        TableName: config.tableName,
        Key: runMetaKey(runId),
        ConsistentRead: true,
      })
    );
    expect(run.Item).toEqual({
      PK: 'RUN#wrun_SMOKE',
      SK: 'META',
      status: 'pending',
      nextSlot: 2,
    });

    const event = await clients.documents.send(
      new GetCommand({
        TableName: config.tableName,
        Key: eventKey(runId, slotToEventId(1)),
      })
    );
    expect(event.Item?.SK).toBe('EVT#evnt_00000000000000000000000001');
  });

  it('has TTL enabled on expiresAt', async () => {
    const ttl = await clients.dynamodb.send(
      new DescribeTimeToLiveCommand({ TableName: config.tableName })
    );
    expect(ttl.TimeToLiveDescription?.AttributeName).toBe('expiresAt');
    expect(ttl.TimeToLiveDescription?.TimeToLiveStatus).toMatch(/ENABL/);
  });

  it('sends and receives an SQS message', async () => {
    const body = JSON.stringify({ messageId: 'msg_smoke', payload: 1 });
    await clients.sqs.send(
      new SendMessageCommand({
        QueueUrl: config.workflowQueueUrl,
        MessageBody: body,
      })
    );

    const received = await clients.sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: config.workflowQueueUrl,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 5,
      })
    );
    const [message] = received.Messages ?? [];
    expect(message?.Body).toBe(body);

    await clients.sqs.send(
      new DeleteMessageCommand({
        QueueUrl: config.workflowQueueUrl,
        ReceiptHandle: message?.ReceiptHandle,
      })
    );
  });

  it('attached a dead-letter queue to both queues', async () => {
    for (const queueUrl of [config.workflowQueueUrl, config.stepQueueUrl]) {
      const attributes = await clients.sqs.send(
        new GetQueueAttributesCommand({
          QueueUrl: queueUrl,
          AttributeNames: ['RedrivePolicy'],
        })
      );
      const policy = JSON.parse(
        attributes.Attributes?.RedrivePolicy ?? '{}'
      ) as { deadLetterTargetArn?: string; maxReceiveCount?: unknown };
      expect(policy.deadLetterTargetArn).toMatch(/-dlq$/);
      expect(Number(policy.maxReceiveCount)).toBe(5);
    }
  });

  it('puts and gets an S3 object', async () => {
    const key = 'wrun_SMOKE/output/1';
    await clients.s3.send(
      new PutObjectCommand({
        Bucket: aws.bucket,
        Key: key,
        Body: 'large payload',
      })
    );

    const object = await clients.s3.send(
      new GetObjectCommand({ Bucket: aws.bucket, Key: key })
    );
    expect(await object.Body?.transformToString()).toBe('large payload');
  });
});
