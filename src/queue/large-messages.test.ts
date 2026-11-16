import {
  DeleteObjectCommand,
  GetObjectCommand,
  type GetObjectCommandInput,
  PutObjectCommand,
  type PutObjectCommandInput,
  S3Client,
} from '@aws-sdk/client-s3';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  type Message,
  SendMessageBatchCommand,
  type SendMessageBatchCommandInput,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import {
  DeleteCommand,
  PutCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import {
  afterAll,
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import {
  createMocks,
  type Mocks,
  queueOptions,
  sent,
  serveMessages,
} from '../../test/unit/sqs-mock.js';
import { PayloadStore } from '../storage/payloads.js';
import {
  createSqsQueue,
  type SqsQueue,
  type SqsQueueOptions,
} from './index.js';
import {
  decodeMessage,
  encodeForQueue,
  encodeMessage,
  MAX_SQS_MESSAGE_BYTES,
  type QueueMessage,
} from './message.js';

const queueName = '__wkf_workflow_wf' as const;
const prefix = '__wkf_workflow_';
const bucket = 'workflow-payloads';
const s3 = new S3Client({
  region: 'us-east-1',
  credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
});
const s3Mock = mockClient(s3);

let mocks: Mocks;
let objects: Map<string, Uint8Array>;
let queues: SqsQueue[];

beforeEach(() => {
  mocks = createMocks();
  queues = [];
  mocks.sqsMock.on(SendMessageCommand).resolves({ MessageId: 'sqs' });
  mocks.sqsMock.on(DeleteMessageCommand).resolves({});
  mocks.sqsMock.on(ChangeMessageVisibilityCommand).resolves({});
  mocks.ddbMock.on(PutCommand).resolves({});
  mocks.ddbMock.on(UpdateCommand).resolves({});
  mocks.ddbMock.on(DeleteCommand).resolves({});
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
  s3Mock.on(DeleteObjectCommand).callsFake((input: { Key: string }) => {
    objects.delete(input.Key);
    return {};
  });
});

afterEach(async () => {
  await Promise.all(queues.map((queue) => queue.close()));
  mocks.sqsMock.restore();
  mocks.ddbMock.restore();
});

afterAll(() => {
  s3Mock.restore();
});

/** A payload whose message is over SQS's 256 KiB. */
const largePayload = () => ({
  runId: 'wrun_01JD000000000000000000000A',
  runInput: {
    input: new Uint8Array(300 * 1024).map((_, i) => i % 251),
    deploymentId: 'aws',
    workflowName: 'wf',
    specVersion: 8,
  },
});

/** A queue over fresh queue URLs, with or without a bucket. */
function newQueue(
  withBucket = true,
  overrides: Partial<SqsQueueOptions> = {}
): { queue: SqsQueue; options: SqsQueueOptions } {
  const options = queueOptions(mocks, {
    payloads: new PayloadStore({ s3, ...(withBucket && { bucket }) }),
    ...overrides,
  });
  const queue = createSqsQueue(options);
  queues.push(queue);
  return { queue, options };
}

const deletedObjects = () =>
  s3Mock
    .commandCalls(DeleteObjectCommand)
    .map((call) => call.args[0].input.Key);

describe('messages too large for SQS', () => {
  it('travel in S3, with a body that points to them', async () => {
    const { queue } = newQueue();
    const payload = largePayload();

    const { messageId } = await queue.queue(queueName, payload as never);

    const [put] = s3Mock.commandCalls(PutObjectCommand);
    const key = put?.args[0].input.Key ?? '';
    expect(key).toMatch(
      new RegExp(
        `^queue/${payload.runId}/${messageId}/[0-9A-Z]{26}-[0-9a-f]{40}$`
      )
    );
    const body =
      mocks.sqsMock.commandCalls(SendMessageCommand)[0]?.args[0].input
        .MessageBody ?? '';
    expect(Buffer.byteLength(body)).toBeLessThan(1024);
    const pointer = decodeMessage(body);
    expect(pointer).toMatchObject({
      v: 1,
      messageId,
      queueName,
      payload: null,
      bodyRef: {
        s3Ref: `s3://${bucket}/${key}`,
        s3Encoding: 'binary',
      },
    });
    // The object is the whole message.
    const whole = decodeMessage(new TextDecoder().decode(objects.get(key)));
    expect(whole).toMatchObject({ messageId, queueName, payload });
  });

  it('stay in SQS when they fit', async () => {
    const { queue } = newQueue();
    await queue.queue(queueName, { runId: 'wrun_A' } as never);
    expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(0);
    expect(sent(mocks)[0]?.message.payload).toEqual({ runId: 'wrun_A' });
  });

  it('are refused without a bucket, naming WORKFLOW_AWS_BUCKET, and give their key back', async () => {
    const { queue } = newQueue(false);

    const sending = queue.queue(queueName, largePayload() as never, {
      idempotencyKey: 'big',
    });
    await expect(sending).rejects.toMatchObject({
      name: 'PayloadTooLargeError',
      status: 413,
      limit: MAX_SQS_MESSAGE_BYTES,
    });
    await expect(sending).rejects.toThrow(/WORKFLOW_AWS_BUCKET/);
    expect(mocks.sqsMock.commandCalls(SendMessageCommand)).toHaveLength(0);
    // The idempotency reservation was released for a later retry.
    expect(
      mocks.ddbMock.commandCalls(DeleteCommand)[0]?.args[0].input.Key
    ).toEqual({ PK: 'IDEMP#big', SK: 'Q' });
  });

  it('are offloaded one by one in a batch, and refused per entry without a bucket', async () => {
    mocks.sqsMock
      .on(SendMessageBatchCommand)
      .callsFake((input: SendMessageBatchCommandInput) => ({
        Successful: (input.Entries ?? []).map((entry) => ({
          Id: entry.Id,
          MessageId: 'sqs',
          MD5OfMessageBody: '',
        })),
      }));
    const { queue } = newQueue();
    const results = await queue.queueBatch(queueName, [
      { message: { runId: 'wrun_A' } as never },
      { message: largePayload() as never },
    ]);
    expect(results.map((result) => result.messageId)).toEqual([
      expect.stringMatching(/^msg_/),
      expect.stringMatching(/^msg_/),
    ]);
    expect(s3Mock.commandCalls(PutObjectCommand)).toHaveLength(1);
    const entries =
      mocks.sqsMock.commandCalls(SendMessageBatchCommand)[0]?.args[0].input
        .Entries ?? [];
    expect(
      entries.map((entry) => decodeMessage(entry.MessageBody).bodyRef)
    ).toEqual([undefined, expect.objectContaining({ s3Encoding: 'binary' })]);

    const { queue: without } = newQueue(false);
    const refused = await without.queueBatch(queueName, [
      { message: { runId: 'wrun_A' } as never },
      { message: largePayload() as never },
    ]);
    expect(refused[0]?.messageId).toMatch(/^msg_/);
    expect(refused[1]).toMatchObject({
      messageId: null,
      retryable: false,
      error: expect.stringContaining('WORKFLOW_AWS_BUCKET'),
    });
  });
});

describe('the worker, with a message in S3', () => {
  /** An SQS message whose body points to S3, as `queue()` sends it. */
  async function received(payload: unknown): Promise<Message> {
    const message: QueueMessage = {
      v: 1,
      messageId: 'msg_01JD0000000000000000000BIG' as never,
      queueName,
      createdAt: '2026-11-16T03:00:00.000Z',
      payload,
    };
    const body = await encodeForQueue(
      message,
      new PayloadStore({ s3, bucket }),
      100
    );
    return {
      MessageId: 'sqs-big',
      ReceiptHandle: 'receipt-big',
      Body: body,
      Attributes: { ApproximateReceiveCount: '1' },
    };
  }

  /** A worker whose prefix is served in process by `handler`. */
  function worker(handler: (payload: unknown) => Promise<unknown>) {
    const { queue, options } = newQueue();
    queue.registerHandler(
      prefix,
      queue.createQueueHandler(prefix, handler as never)
    );
    return { queue, url: options.workflowQueueUrl };
  }

  it('delivers the whole message, then deletes the object with the message', async () => {
    const handler = vi.fn(async () => undefined);
    const { queue, url } = worker(handler);
    const payload = { runId: 'wrun_A', blob: new Uint8Array([1, 2, 3]) };
    const message = await received(payload);
    const [key] = [...objects.keys()];
    serveMessages(mocks, url, [message]);
    await queue.startWorker();

    await vi.waitFor(() => expect(deletedObjects()).toEqual([key]));
    expect(handler).toHaveBeenCalledWith(payload, expect.anything());
    expect(
      mocks.sqsMock.commandCalls(DeleteMessageCommand)[0]?.args[0].input
        .ReceiptHandle
    ).toBe('receipt-big');
  });

  it('keeps the object of a delivery that failed', async () => {
    const handler = vi.fn(async () => {
      throw new Error('storage is down');
    });
    const { queue, url } = worker(handler);
    const message = await received({ runId: 'wrun_A' });
    serveMessages(mocks, url, [message]);
    await queue.startWorker();

    await vi.waitFor(() =>
      expect(
        mocks.sqsMock.commandCalls(ChangeMessageVisibilityCommand).length
      ).toBeGreaterThan(0)
    );
    expect(deletedObjects()).toEqual([]);
    expect(objects.size).toBe(1);
  });

  it('sends a redelivery with its own object, and deletes the old one', async () => {
    const handler = vi.fn(async () => ({ timeoutSeconds: 5 }));
    const { queue, url } = worker(handler);
    const payload = {
      runId: 'wrun_A',
      stepId: 'step_1',
      blob: new Uint8Array(300 * 1024),
    };
    const message = await received(payload);
    const [old] = [...objects.keys()];
    serveMessages(mocks, url, [message]);
    await queue.startWorker();

    await vi.waitFor(() => expect(deletedObjects()).toEqual([old]));
    const [again] = mocks.sqsMock.commandCalls(SendMessageCommand);
    const pointer = decodeMessage(again?.args[0].input.MessageBody);
    expect(pointer.bodyRef?.s3Ref).not.toContain(String(old));
    expect(objects.size).toBe(1);
    const whole = decodeMessage(
      new TextDecoder().decode([...objects.values()][0])
    );
    expect(whole).toMatchObject({
      messageId: 'msg_01JD0000000000000000000BIG',
      deliveryOffset: 1,
      payload,
    });
  });

  it('backs off a message whose object is gone, without delivering it', async () => {
    const handler = vi.fn(async () => undefined);
    const { queue, url } = worker(handler);
    const message = await received({ runId: 'wrun_A' });
    objects.clear();
    serveMessages(mocks, url, [message]);
    await queue.startWorker();

    await vi.waitFor(() =>
      expect(
        mocks.sqsMock.commandCalls(ChangeMessageVisibilityCommand)[0]?.args[0]
          .input.VisibilityTimeout
      ).toBe(3)
    );
    expect(handler).not.toHaveBeenCalled();
  });
});

describe('encodeForQueue', () => {
  it('keeps the body as is up to the limit', async () => {
    const message: QueueMessage = {
      v: 1,
      messageId: 'msg_01JD0000000000000000000ABC' as never,
      queueName,
      createdAt: '2026-11-16T03:00:00.000Z',
      payload: { runId: 'wrun_A' },
    };
    const body = encodeMessage(message);
    await expect(
      encodeForQueue(message, undefined, Buffer.byteLength(body))
    ).resolves.toBe(body);
  });
});
