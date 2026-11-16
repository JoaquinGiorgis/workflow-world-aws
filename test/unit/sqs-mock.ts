/**
 * Mocked SQS and DynamoDB clients for the queue's unit tests, and helpers to
 * feed the worker messages and read back what it sent.
 */
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import type { SqsQueueOptions } from '../../src/queue/index.js';
import {
  decodeMessage,
  encodeMessage,
  type QueueMessage,
} from '../../src/queue/message.js';

const credentials = { accessKeyId: 'test', secretAccessKey: 'test' };

export function createMocks() {
  const sqs = new SQSClient({ region: 'us-east-1', credentials });
  const documents = DynamoDBDocumentClient.from(
    new DynamoDBClient({ region: 'us-east-1', credentials })
  );
  const sqsMock = mockClient(sqs);
  const ddbMock = mockClient(documents);
  return { sqs, documents, sqsMock, ddbMock };
}

export type Mocks = ReturnType<typeof createMocks>;

let queues = 0;

/**
 * Queue options over fresh queue URLs: the process registry keys handlers
 * and workers by URL, so each test gets its own.
 */
export function queueOptions(
  mocks: Mocks,
  overrides: Partial<SqsQueueOptions> = {}
): SqsQueueOptions {
  queues++;
  return {
    sqs: mocks.sqs,
    documents: mocks.documents,
    tableName: 'workflow',
    workflowQueueUrl: `http://localhost:4566/000000000000/workflow-${queues}`,
    stepQueueUrl: `http://localhost:4566/000000000000/step-${queues}`,
    concurrency: 4,
    visibilityTimeoutSeconds: 30,
    pollWaitSeconds: 0,
    shutdownGraceMs: 1_000,
    internal: { log: () => {} },
    ...overrides,
  };
}

let receipts = 0;

/** An SQS message carrying a World message. */
export function sqsMessage(
  message: Partial<QueueMessage> & { payload: unknown },
  receiveCount = 1
): Message {
  receipts++;
  return {
    MessageId: `sqs-${receipts}`,
    ReceiptHandle: `receipt-${receipts}`,
    Body: encodeMessage({
      v: 1,
      messageId:
        `msg_01JD00000000000000000000${String(receipts).padStart(2, '0')}` as QueueMessage['messageId'],
      queueName: '__wkf_workflow_wf' as QueueMessage['queueName'],
      createdAt: '2026-11-16T03:00:00.000Z',
      ...message,
    }),
    Attributes: { ApproximateReceiveCount: String(receiveCount) },
  };
}

/**
 * Serves `messages` to ReceiveMessage on `queueUrl`, one per call, and
 * nothing (after a short wait, as a long poll would) once they are gone.
 */
export function serveMessages(
  mocks: Mocks,
  queueUrl: string,
  messages: Message[]
): void {
  const pending = [...messages];
  mocks.sqsMock
    .on(ReceiveMessageCommand)
    .callsFake(async (input: { QueueUrl: string }) => {
      const next = input.QueueUrl === queueUrl ? pending.shift() : undefined;
      if (next) return { Messages: [next] };
      await new Promise((resolve) => setTimeout(resolve, 5));
      return { Messages: [] };
    });
}

/** Every SendMessage, decoded. */
export function sent(mocks: Mocks): {
  queueUrl: string;
  delaySeconds?: number;
  message: QueueMessage;
}[] {
  return mocks.sqsMock.commandCalls(SendMessageCommand).map((call) => {
    const input = call.args[0].input;
    return {
      queueUrl: input.QueueUrl ?? '',
      ...(input.DelaySeconds !== undefined && {
        delaySeconds: input.DelaySeconds,
      }),
      message: decodeMessage(input.MessageBody),
    };
  });
}
