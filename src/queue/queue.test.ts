import { ConditionalCheckFailedException } from '@aws-sdk/client-dynamodb';
import {
  SendMessageBatchCommand,
  type SendMessageBatchCommandInput,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import {
  DeleteCommand,
  GetCommand,
  PutCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  createMocks,
  type Mocks,
  queueOptions,
  sent,
} from '../../test/unit/sqs-mock.js';
import { SQS_MAX_SEND_MS } from '../clients.js';
import {
  IDEMPOTENCY_TTL_SECONDS,
  RESERVATION_TIMEOUT_MS,
} from './idempotency.js';
import { createSqsQueue, DEPLOYMENT_ID } from './index.js';
import { decodeMessage } from './message.js';

const queueName = '__wkf_workflow_wf' as const;
let mocks: Mocks;

beforeEach(() => {
  mocks = createMocks();
  mocks.sqsMock.on(SendMessageCommand).resolves({ MessageId: 'sqs' });
  mocks.ddbMock.on(PutCommand).resolves({});
  mocks.ddbMock.on(UpdateCommand).resolves({});
  mocks.ddbMock.on(DeleteCommand).resolves({});
});

afterEach(() => {
  mocks.sqsMock.restore();
  mocks.ddbMock.restore();
  vi.useRealTimers();
});

function conditionFailed() {
  return new ConditionalCheckFailedException({
    message: 'The conditional request failed',
    $metadata: {},
  });
}

describe('queue()', () => {
  it('sends orchestrator messages to the workflow queue with a stable id in the body', async () => {
    const options = queueOptions(mocks);
    const queue = createSqsQueue(options);
    const { messageId } = await queue.queue(queueName, {
      runId: 'wrun_A',
      runInput: {
        input: new Uint8Array([1, 2]),
        deploymentId: 'aws',
        workflowName: 'wf',
        specVersion: 8,
      },
    } as never);

    expect(messageId).toMatch(/^msg_[0-9A-Z]{26}$/);
    const [send] = sent(mocks);
    expect(send?.queueUrl).toBe(options.workflowQueueUrl);
    expect(send?.delaySeconds).toBeUndefined();
    expect(send?.message).toMatchObject({
      v: 1,
      messageId,
      queueName,
      payload: { runId: 'wrun_A', runInput: { input: new Uint8Array([1, 2]) } },
    });
    await queue.close();
  });

  it('sends step messages to the step queue', async () => {
    const options = queueOptions(mocks);
    const queue = createSqsQueue(options);
    await queue.queue(queueName, {
      runId: 'wrun_A',
      stepId: 'step_1',
    } as never);
    expect(sent(mocks)[0]?.queueUrl).toBe(options.stepQueueUrl);
    await queue.close();
  });

  it('refuses an invalid queue name', async () => {
    const queue = createSqsQueue(queueOptions(mocks));
    await expect(
      queue.queue('nope' as never, { runId: 'wrun_A' } as never)
    ).rejects.toThrow();
    expect(sent(mocks)).toHaveLength(0);
    await queue.close();
  });

  it('delays up to 15 minutes with SQS DelaySeconds', async () => {
    const queue = createSqsQueue(queueOptions(mocks));
    await queue.queue(queueName, { runId: 'wrun_A' } as never, {
      delaySeconds: 899.2,
    });
    expect(sent(mocks)[0]?.delaySeconds).toBe(900);
    expect(sent(mocks)[0]?.message.notBefore).toBeUndefined();
    await queue.close();
  });

  it('chains a longer delay: 15 minutes now, and notBefore for the rest', async () => {
    const queue = createSqsQueue(queueOptions(mocks));
    const before = Date.now();
    await queue.queue(queueName, { runId: 'wrun_A' } as never, {
      delaySeconds: 3_600,
    });
    const [send] = sent(mocks);
    expect(send?.delaySeconds).toBe(900);
    expect(send?.message.notBefore).toBeGreaterThanOrEqual(before + 3_600_000);
    expect(send?.message.notBefore).toBeLessThanOrEqual(Date.now() + 3_600_000);
    await queue.close();
  });

  it('carries caller headers and the producer route', async () => {
    const queue = createSqsQueue(queueOptions(mocks));
    queue.registerRoute('__wkf_workflow_', '/api/flow');
    await queue.queue(queueName, { runId: 'wrun_A' } as never, {
      headers: { 'x-trace': '1' },
    });
    expect(sent(mocks)[0]?.message).toMatchObject({
      headers: { 'x-trace': '1' },
      route: '/api/flow',
    });
    await queue.close();
  });

  it('surfaces an SQS error', async () => {
    mocks.sqsMock
      .on(SendMessageCommand)
      .rejects(new Error('QueueDoesNotExist'));
    const queue = createSqsQueue(queueOptions(mocks));
    await expect(
      queue.queue(queueName, { runId: 'wrun_A' } as never)
    ).rejects.toThrow('QueueDoesNotExist');
    await queue.close();
  });

  it('declares queueAuthMode skip and one deployment', async () => {
    const queue = createSqsQueue(queueOptions(mocks));
    expect(queue.queueAuthMode).toBe('skip');
    expect(await queue.getDeploymentId()).toBe(DEPLOYMENT_ID);
    await queue.close();
  });
});

describe('queue() with an idempotency key', () => {
  it('reserves the key with a conditional put and TTL, sends, then marks it sent', async () => {
    const queue = createSqsQueue(queueOptions(mocks));
    const nowSeconds = Math.floor(Date.now() / 1000);
    const { messageId } = await queue.queue(
      queueName,
      { runId: 'wrun_A', stepId: 'step_1' } as never,
      { idempotencyKey: 'step-key' }
    );

    const put = mocks.ddbMock.commandCalls(PutCommand)[0]?.args[0].input;
    expect(put?.Item).toMatchObject({
      PK: 'IDEMP#step-key',
      SK: 'Q',
      messageId,
      state: 'reserved',
    });
    expect(put?.ConditionExpression).toBe(
      'attribute_not_exists(PK) OR expiresAt < :now'
    );
    expect(Number(put?.Item?.expiresAt)).toBeGreaterThanOrEqual(
      nowSeconds + IDEMPOTENCY_TTL_SECONDS
    );
    expect(sent(mocks)[0]?.message).toMatchObject({
      messageId,
      idempotencyKey: 'step-key',
    });
    const update = mocks.ddbMock.commandCalls(UpdateCommand)[0]?.args[0].input;
    // Confirmed only while the reservation is still the one this send made.
    expect(update?.ConditionExpression).toBe(
      'messageId = :id AND reservedAt = :mine'
    );
    expect(update?.ExpressionAttributeValues).toEqual({
      ':sent': 'sent',
      ':id': messageId,
      ':mine': put?.Item?.reservedAt,
    });
    await queue.close();
  });

  it('warns when another sender overtook the reservation before it was confirmed', async () => {
    mocks.ddbMock.on(UpdateCommand).rejects(conditionFailed());
    const log = vi.fn();
    const queue = createSqsQueue(queueOptions(mocks, { internal: { log } }));
    const { messageId } = await queue.queue(
      queueName,
      { runId: 'wrun_A', stepId: 'step_1' } as never,
      { idempotencyKey: 'overtaken' }
    );
    expect(sent(mocks)).toHaveLength(1);
    expect(log).toHaveBeenCalledWith(
      expect.stringContaining('overtaken'),
      expect.objectContaining({ idempotencyKey: 'overtaken', messageId })
    );
    await queue.close();
  });

  it('answers the stored message id and sends nothing when the key was used', async () => {
    mocks.ddbMock.on(PutCommand).rejects(conditionFailed());
    mocks.ddbMock.on(GetCommand).resolves({
      Item: {
        messageId: 'msg_01JD000000000000000000OLD0',
        state: 'sent',
        reservedAt: Date.now() - 1_000,
        expiresAt: Math.floor(Date.now() / 1000) + 100,
      },
    });
    const queue = createSqsQueue(queueOptions(mocks));
    const result = await queue.queue(queueName, { runId: 'wrun_A' } as never, {
      idempotencyKey: 'used',
    });
    expect(result.messageId).toBe('msg_01JD000000000000000000OLD0');
    expect(sent(mocks)).toHaveLength(0);
    await queue.close();
  });

  it('waits for a reservation another sender is still sending', async () => {
    mocks.ddbMock.on(PutCommand).rejects(conditionFailed());
    const item = {
      messageId: 'msg_01JD000000000000000000OTHR',
      state: 'reserved',
      reservedAt: Date.now(),
      expiresAt: Math.floor(Date.now() / 1000) + 100,
    };
    mocks.ddbMock
      .on(GetCommand)
      .resolvesOnce({ Item: item })
      .resolves({ Item: { ...item, state: 'sent' } });
    const queue = createSqsQueue(queueOptions(mocks));
    const result = await queue.queue(queueName, { runId: 'wrun_A' } as never, {
      idempotencyKey: 'busy',
    });
    expect(result.messageId).toBe(item.messageId);
    expect(mocks.ddbMock.commandCalls(GetCommand)).toHaveLength(2);
    expect(sent(mocks)).toHaveLength(0);
    await queue.close();
  });

  it('takes over a reservation its sender never confirmed, under the same id', async () => {
    mocks.ddbMock.on(PutCommand).rejects(conditionFailed());
    mocks.ddbMock.on(GetCommand).resolves({
      Item: {
        messageId: 'msg_01JD000000000000000000DEAD',
        state: 'reserved',
        reservedAt: Date.now() - RESERVATION_TIMEOUT_MS - 1,
        expiresAt: Math.floor(Date.now() / 1000) + 100,
      },
    });
    const queue = createSqsQueue(queueOptions(mocks));
    const result = await queue.queue(queueName, { runId: 'wrun_A' } as never, {
      idempotencyKey: 'stale',
    });
    expect(result.messageId).toBe('msg_01JD000000000000000000DEAD');
    expect(sent(mocks)[0]?.message.messageId).toBe(
      'msg_01JD000000000000000000DEAD'
    );
    const [takeOver, confirm] = mocks.ddbMock
      .commandCalls(UpdateCommand)
      .map((call) => call.args[0].input);
    expect(takeOver?.ConditionExpression).toContain('reservedAt = :then');
    // The confirmation is conditioned on the taker's own reservation time.
    expect(confirm?.ExpressionAttributeValues?.[':mine']).toBe(
      takeOver?.ExpressionAttributeValues?.[':now']
    );
    await queue.close();
  });

  it('does not take over a reservation while its sender can still be sending', async () => {
    expect(RESERVATION_TIMEOUT_MS).toBeGreaterThanOrEqual(4 * SQS_MAX_SEND_MS);
    vi.useFakeTimers({ toFake: ['Date', 'setTimeout', 'clearTimeout'] });
    mocks.ddbMock.on(PutCommand).rejects(conditionFailed());
    // The original sender reserved the key as long ago as its SQS client
    // can keep one send going (every attempt timing out, plus backoff).
    const reservedAt = Date.now() - SQS_MAX_SEND_MS;
    mocks.ddbMock.on(GetCommand).resolves({
      Item: {
        messageId: 'msg_01JD000000000000000000SLOW',
        state: 'reserved',
        reservedAt,
        expiresAt: Math.floor(Date.now() / 1000) + IDEMPOTENCY_TTL_SECONDS,
      },
    });
    const queue = createSqsQueue(queueOptions(mocks));
    const result = queue.queue(queueName, { runId: 'wrun_A' } as never, {
      idempotencyKey: 'slow',
    });

    await vi.advanceTimersByTimeAsync(500);
    expect(mocks.ddbMock.commandCalls(UpdateCommand)).toHaveLength(0);
    expect(sent(mocks)).toHaveLength(0);

    // Still waiting just before the reservation times out.
    await vi.advanceTimersByTimeAsync(
      reservedAt + RESERVATION_TIMEOUT_MS - Date.now() - 200
    );
    expect(mocks.ddbMock.commandCalls(UpdateCommand)).toHaveLength(0);
    expect(sent(mocks)).toHaveLength(0);

    // Once it has, the sender is gone: take the reservation over.
    await vi.advanceTimersByTimeAsync(400);
    expect((await result).messageId).toBe('msg_01JD000000000000000000SLOW');
    expect(sent(mocks)).toHaveLength(1);
    expect(
      mocks.ddbMock.commandCalls(UpdateCommand)[0]?.args[0].input
        .ConditionExpression
    ).toContain('reservedAt = :then');
    await queue.close();
  });

  it('gives the key back when the send fails, so a retry can send', async () => {
    mocks.sqsMock.on(SendMessageCommand).rejects(new Error('throttled'));
    const queue = createSqsQueue(queueOptions(mocks));
    await expect(
      queue.queue(queueName, { runId: 'wrun_A' } as never, {
        idempotencyKey: 'retry-me',
      })
    ).rejects.toThrow('throttled');
    const release = mocks.ddbMock.commandCalls(DeleteCommand)[0]?.args[0].input;
    expect(release?.Key).toEqual({ PK: 'IDEMP#retry-me', SK: 'Q' });
    // Only its own reservation: one another sender took over stays.
    expect(release?.ConditionExpression).toBe(
      'messageId = :id AND reservedAt = :mine AND #state = :reserved'
    );
    const put = mocks.ddbMock.commandCalls(PutCommand)[0]?.args[0].input;
    expect(release?.ExpressionAttributeValues?.[':mine']).toBe(
      put?.Item?.reservedAt
    );
    expect(mocks.ddbMock.commandCalls(UpdateCommand)).toHaveLength(0);
    await queue.close();
  });

  it('surfaces a DynamoDB error other than the lost condition', async () => {
    mocks.ddbMock
      .on(PutCommand)
      .rejects(new Error('ProvisionedThroughputExceeded'));
    const queue = createSqsQueue(queueOptions(mocks));
    await expect(
      queue.queue(queueName, { runId: 'wrun_A' } as never, {
        idempotencyKey: 'k',
      })
    ).rejects.toThrow('ProvisionedThroughputExceeded');
    expect(sent(mocks)).toHaveLength(0);
    await queue.close();
  });
});

describe('queueBatch()', () => {
  it('sends in batches of 10 per queue and reports each message in order', async () => {
    mocks.sqsMock
      .on(SendMessageBatchCommand)
      .callsFake((input: SendMessageBatchCommandInput) => ({
        Successful: (input.Entries ?? [])
          .filter((entry) => entry.Id !== '3')
          .map((entry) => ({ Id: entry.Id, MessageId: 'sqs' })),
        Failed: (input.Entries ?? [])
          .filter((entry) => entry.Id === '3')
          .map((entry) => ({
            Id: entry.Id,
            Code: 'InvalidMessageContents',
            Message: 'bad',
            SenderFault: true,
          })),
      }));
    mocks.ddbMock
      .on(PutCommand, { Item: { PK: 'IDEMP#dup' } })
      .rejects(conditionFailed());
    mocks.ddbMock.on(GetCommand).resolves({
      Item: {
        messageId: 'msg_01JD000000000000000000DUP0',
        state: 'sent',
        reservedAt: Date.now(),
        expiresAt: Math.floor(Date.now() / 1000) + 100,
      },
    });
    const options = queueOptions(mocks);
    const queue = createSqsQueue(options);
    const messages = Array.from({ length: 13 }, (_, i) => ({
      message: { runId: 'wrun_A', stepId: `step_${i}` } as never,
      opts: {
        idempotencyKey: i === 5 ? 'dup' : `key-${i}`,
        ...(i === 7 && { delaySeconds: 2_000 }),
      },
    }));
    const results = await queue.queueBatch(queueName, messages);

    expect(results).toHaveLength(13);
    expect(results[3]).toEqual({
      messageId: null,
      error: 'InvalidMessageContents: bad',
      retryable: false,
    });
    expect(results[5]).toEqual({ messageId: 'msg_01JD000000000000000000DUP0' });
    for (const i of [0, 1, 2, 4, 6, 7, 8, 9, 10, 11, 12]) {
      expect(results[i]?.messageId).toMatch(/^msg_/);
      expect(results[i]?.error).toBeUndefined();
    }
    const batches = mocks.sqsMock
      .commandCalls(SendMessageBatchCommand)
      .map((call) => call.args[0].input);
    expect(batches.map((batch) => batch.Entries?.length)).toEqual([10, 2]);
    expect(
      batches.every((batch) => batch.QueueUrl === options.stepQueueUrl)
    ).toBe(true);
    const delayed = batches[0]?.Entries?.find((entry) => entry.Id === '7');
    expect(delayed?.DelaySeconds).toBe(900);
    expect(decodeMessage(delayed?.MessageBody).notBefore).toBeGreaterThan(
      Date.now() + 1_000_000
    );
    // The failed entry's key is given back; the sent ones are confirmed.
    expect(
      mocks.ddbMock
        .commandCalls(DeleteCommand)
        .map((call) => call.args[0].input.Key?.PK)
    ).toEqual(['IDEMP#key-3']);
    expect(mocks.ddbMock.commandCalls(UpdateCommand)).toHaveLength(11);
    // Each conditioned on the reservation its own put made.
    const reservedAt = new Map(
      mocks.ddbMock
        .commandCalls(PutCommand)
        .map((call) => call.args[0].input.Item)
        .map((item) => [item?.PK, item?.reservedAt])
    );
    for (const call of [
      ...mocks.ddbMock.commandCalls(UpdateCommand),
      ...mocks.ddbMock.commandCalls(DeleteCommand),
    ]) {
      const input = call.args[0].input;
      expect(input.ExpressionAttributeValues?.[':mine']).toBe(
        reservedAt.get(input.Key?.PK)
      );
    }
    await queue.close();
  });

  it('reports a failed request per message instead of throwing', async () => {
    mocks.sqsMock.on(SendMessageBatchCommand).rejects(new Error('network'));
    const queue = createSqsQueue(queueOptions(mocks));
    const results = await queue.queueBatch(queueName, [
      { message: { runId: 'wrun_A' } as never },
      { message: { runId: 'wrun_B' } as never },
    ]);
    expect(results).toEqual([
      { messageId: null, error: 'Error: network', retryable: true },
      { messageId: null, error: 'Error: network', retryable: true },
    ]);
    await queue.close();
  });
});
