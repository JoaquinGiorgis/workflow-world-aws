import { createServer, type Server } from 'node:http';
import type { AddressInfo } from 'node:net';
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import {
  createMocks,
  type Mocks,
  queueOptions,
  sent,
  serveMessages,
  sqsMessage,
} from '../../test/unit/sqs-mock.js';
import { defaultRetryDelaySeconds, followUp, scheduleOf } from './consumer.js';
import {
  createSqsQueue,
  type SqsQueue,
  type SqsQueueOptions,
} from './index.js';

const prefix = '__wkf_workflow_';
type Handler = Parameters<SqsQueue['createQueueHandler']>[1];

let mocks: Mocks;
let queues: SqsQueue[];

beforeEach(() => {
  mocks = createMocks();
  queues = [];
  mocks.sqsMock.on(SendMessageCommand).resolves({ MessageId: 'sqs' });
  mocks.sqsMock.on(DeleteMessageCommand).resolves({});
  mocks.sqsMock.on(ChangeMessageVisibilityCommand).resolves({});
});

afterEach(async () => {
  await Promise.all(queues.map((queue) => queue.close()));
  mocks.sqsMock.restore();
  mocks.ddbMock.restore();
});

/** A started worker whose prefix is served in process by `handler`. */
async function worker(
  handler: Handler,
  overrides: Partial<SqsQueueOptions> = {}
): Promise<{ queue: SqsQueue; options: SqsQueueOptions }> {
  const options = queueOptions(mocks, overrides);
  const queue = createSqsQueue(options);
  queues.push(queue);
  queue.registerHandler(prefix, queue.createQueueHandler(prefix, handler));
  return { queue, options };
}

const deleted = () =>
  mocks.sqsMock
    .commandCalls(DeleteMessageCommand)
    .map((call) => call.args[0].input.ReceiptHandle);
const visibilityChanges = () =>
  mocks.sqsMock
    .commandCalls(ChangeMessageVisibilityCommand)
    .map((call) => call.args[0].input);

describe('the worker', () => {
  it('delivers a message with stable metadata and deletes it when acknowledged', async () => {
    const handler = vi.fn(async () => undefined);
    const { queue, options } = await worker(handler);
    const message = sqsMessage(
      {
        messageId: 'msg_01JD0000000000000000000ABC' as never,
        payload: { runId: 'wrun_A', input: new Uint8Array([7]) },
        attemptOffset: 1,
        deliveryOffset: 3,
      },
      2
    );
    serveMessages(mocks, options.workflowQueueUrl, [message]);
    expect(await queue.startWorker()).toBe(true);

    await vi.waitFor(() => expect(deleted()).toEqual([message.ReceiptHandle]));
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0]).toEqual([
      { runId: 'wrun_A', input: new Uint8Array([7]) },
      expect.objectContaining({
        messageId: 'msg_01JD0000000000000000000ABC',
        queueName: '__wkf_workflow_wf',
        attempt: 3,
        deliveryCount: 5,
        createdAt: new Date('2026-11-16T03:00:00.000Z'),
        invocationStartedAt: expect.any(Number),
      }),
    ]);
    const receive = mocks.sqsMock.commandCalls(ReceiveMessageCommand)[0]
      ?.args[0].input;
    expect(receive).toMatchObject({
      WaitTimeSeconds: 0,
      VisibilityTimeout: 30,
      MessageSystemAttributeNames: ['ApproximateReceiveCount'],
    });
  });

  it('redelivers the same message after { timeoutSeconds } as a new SQS message', async () => {
    const handler = vi.fn(async () => ({ timeoutSeconds: 5 }));
    const { queue, options } = await worker(handler);
    const message = sqsMessage({
      messageId: 'msg_01JD0000000000000000000RTY' as never,
      payload: { runId: 'wrun_A', stepId: 'step_1' },
      idempotencyKey: 'step-key',
    });
    serveMessages(mocks, options.workflowQueueUrl, [message]);
    await queue.startWorker();

    await vi.waitFor(() => expect(deleted()).toEqual([message.ReceiptHandle]));
    const [again] = sent(mocks);
    expect(again?.queueUrl).toBe(options.workflowQueueUrl);
    expect(again?.delaySeconds).toBe(5);
    expect(again?.message).toMatchObject({
      messageId: 'msg_01JD0000000000000000000RTY',
      idempotencyKey: 'step-key',
      deliveryOffset: 1,
    });
    // A step retried in place starts its attempts over.
    expect(again?.message.attemptOffset).toBeUndefined();
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('traces every delivery outcome under DEBUG, reschedules included', async () => {
    vi.stubEnv('DEBUG', 'workflow:world-aws');
    const debug = vi.spyOn(console, 'debug').mockImplementation(() => {});
    try {
      const handler = vi.fn(async () => ({ timeoutSeconds: 7 }));
      const { queue, options } = await worker(handler);
      const message = sqsMessage(
        {
          messageId: 'msg_01JD0000000000000000000DBG' as never,
          payload: { runId: 'wrun_A', runSpecVersion: 9 },
        },
        2
      );
      serveMessages(mocks, options.workflowQueueUrl, [message]);
      await queue.startWorker();

      await vi.waitFor(() =>
        expect(deleted()).toEqual([message.ReceiptHandle])
      );
      expect(debug).toHaveBeenCalledWith(
        '[world-aws] queue delivery rescheduled',
        expect.objectContaining({
          messageId: 'msg_01JD0000000000000000000DBG',
          runId: 'wrun_A',
          attempt: 2,
          deliveryCount: 2,
          timeoutSeconds: 7,
        })
      );
    } finally {
      debug.mockRestore();
      vi.unstubAllEnvs();
    }
  });

  it('counts attempts across redeliveries as world-postgres does', () => {
    const base = {
      v: 1 as const,
      messageId: 'msg_01JD00000000000000000000AA' as never,
      queueName: '__wkf_workflow_wf' as never,
      createdAt: '2026-11-16T03:00:00.000Z',
    };
    const spec9 = { ...base, payload: { runId: 'wrun_A', runSpecVersion: 9 } };
    // A spec 9 handoff keeps the attempt.
    expect(followUp(spec9, 3, 4, 0)).toMatchObject({
      attemptOffset: 2,
      deliveryOffset: 4,
    });
    // A spec 9 orchestrator's timed redelivery counts one.
    expect(followUp(spec9, 3, 4, 10)).toMatchObject({
      attemptOffset: 3,
      deliveryOffset: 4,
    });
    // A spec 8 orchestrator's redelivery starts over.
    const spec8 = { ...base, payload: { runId: 'wrun_A' }, attemptOffset: 1 };
    expect(followUp(spec8, 3, 4, 0)).toMatchObject({
      attemptOffset: 1,
      deliveryOffset: 4,
    });
  });

  it('leaves a failed delivery in the queue, invisible for a backoff', async () => {
    const handler = vi.fn(async () => {
      throw new Error('storage is down');
    });
    const log = vi.fn();
    const { queue, options } = await worker(handler, { internal: { log } });
    const message = sqsMessage({ payload: { runId: 'wrun_A' } }, 3);
    serveMessages(mocks, options.workflowQueueUrl, [message]);
    await queue.startWorker();

    await vi.waitFor(() =>
      expect(visibilityChanges()).toContainEqual(
        expect.objectContaining({
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: defaultRetryDelaySeconds(3),
        })
      )
    );
    expect(defaultRetryDelaySeconds(3)).toBe(21);
    expect(deleted()).toEqual([]);
    expect(log).toHaveBeenCalledWith(
      'Queue delivery failed; retrying after a backoff',
      expect.objectContaining({ status: 500, attempt: 3 })
    );
  });

  it('caps the retry backoff at an hour', () => {
    expect(defaultRetryDelaySeconds(1)).toBe(3);
    expect(defaultRetryDelaySeconds(8)).toBe(2_981);
    expect(defaultRetryDelaySeconds(9)).toBe(3_600);
    expect(defaultRetryDelaySeconds(70)).toBe(3_600);
  });

  it('sends a message on with the rest of a long delay instead of running it', async () => {
    const handler = vi.fn();
    const { queue, options } = await worker(handler);
    const notBefore = Date.now() + 2_000_000;
    const message = sqsMessage({
      messageId: 'msg_01JD0000000000000000000HOP' as never,
      payload: { runId: 'wrun_A' },
      notBefore,
    });
    serveMessages(mocks, options.workflowQueueUrl, [message]);
    await queue.startWorker();

    await vi.waitFor(() => expect(deleted()).toEqual([message.ReceiptHandle]));
    expect(handler).not.toHaveBeenCalled();
    const [hop] = sent(mocks);
    expect(hop?.delaySeconds).toBe(900);
    expect(hop?.message).toMatchObject({
      messageId: 'msg_01JD0000000000000000000HOP',
      notBefore,
    });
  });

  it('sends the last hop of a long delay without notBefore', async () => {
    const { queue, options } = await worker(vi.fn());
    const message = sqsMessage({
      payload: { runId: 'wrun_A' },
      notBefore: Date.now() + 120_000,
    });
    serveMessages(mocks, options.workflowQueueUrl, [message]);
    await queue.startWorker();

    await vi.waitFor(() => expect(sent(mocks)).toHaveLength(1));
    const [hop] = sent(mocks);
    expect(hop?.delaySeconds).toBeGreaterThanOrEqual(119);
    expect(hop?.delaySeconds).toBeLessThanOrEqual(120);
    expect(hop?.message.notBefore).toBeUndefined();
  });

  it('runs a message whose notBefore has come', async () => {
    const handler = vi.fn(async () => undefined);
    const { queue, options } = await worker(handler);
    const message = sqsMessage({
      payload: { runId: 'wrun_A' },
      notBefore: Date.now() + 500,
    });
    serveMessages(mocks, options.workflowQueueUrl, [message]);
    await queue.startWorker();
    await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(1));
    expect(sent(mocks)).toHaveLength(0);
  });

  it('keeps a running message invisible with a heartbeat', async () => {
    let finish!: () => void;
    const handler = vi.fn(
      () =>
        new Promise<undefined>((resolve) => (finish = () => resolve(undefined)))
    );
    const { queue, options } = await worker(handler, {
      visibilityTimeoutSeconds: 45,
      internal: { log: () => {}, heartbeatIntervalMs: 20 },
    });
    const message = sqsMessage({ payload: { runId: 'wrun_A' } });
    serveMessages(mocks, options.workflowQueueUrl, [message]);
    await queue.startWorker();

    await vi.waitFor(() =>
      expect(visibilityChanges().length).toBeGreaterThanOrEqual(3)
    );
    expect(visibilityChanges()).toEqual(
      visibilityChanges().map(() => ({
        QueueUrl: options.workflowQueueUrl,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: 45,
      }))
    );
    finish();
    await vi.waitFor(() => expect(deleted()).toEqual([message.ReceiptHandle]));
    const beats = visibilityChanges().length;
    await new Promise((resolve) => setTimeout(resolve, 80));
    expect(visibilityChanges()).toHaveLength(beats);
  });

  describe('a heartbeat in flight when the delivery ends', () => {
    /** Resolves when the next heartbeat starts. */
    let beatStarted: () => Promise<void>;

    /**
     * Records when each SQS call of a receipt starts and ends; heartbeats
     * (`VisibilityTimeout` 45) take 40 ms, and `beatStarted()` resolves
     * while one is in flight.
     */
    function recordTimeline(): string[] {
      const timeline: string[] = [];
      let onBeat: (() => void)[] = [];
      beatStarted = () =>
        new Promise<void>((resolve) => {
          onBeat.push(resolve);
        });
      mocks.sqsMock
        .on(ChangeMessageVisibilityCommand)
        .callsFake(async (input: { VisibilityTimeout: number }) => {
          timeline.push(`visibility ${input.VisibilityTimeout} start`);
          if (input.VisibilityTimeout === 45) {
            const waiting = onBeat;
            onBeat = [];
            for (const resolve of waiting) resolve();
            await new Promise((resolve) => setTimeout(resolve, 40));
          }
          timeline.push(`visibility ${input.VisibilityTimeout} end`);
          return {};
        });
      mocks.sqsMock.on(DeleteMessageCommand).callsFake(async () => {
        timeline.push('delete');
        return {};
      });
      return timeline;
    }

    /** Whether every heartbeat that started before `index` had ended. */
    function beatsSettledBefore(timeline: string[], index: number): boolean {
      const before = timeline.slice(0, index);
      return (
        before.filter((entry) => entry === 'visibility 45 start').length ===
        before.filter((entry) => entry === 'visibility 45 end').length
      );
    }

    it('settles the beat before deleting an acknowledged message', async () => {
      const timeline = recordTimeline();
      const handler = vi.fn(async () => {
        // End the delivery while a heartbeat is in flight.
        await beatStarted();
      });
      const { queue, options } = await worker(handler, {
        visibilityTimeoutSeconds: 45,
        internal: { log: () => {}, heartbeatIntervalMs: 10 },
      });
      serveMessages(mocks, options.workflowQueueUrl, [
        sqsMessage({ payload: { runId: 'wrun_A' } }),
      ]);
      await queue.startWorker();

      await vi.waitFor(() => expect(timeline).toContain('delete'));
      const deleteAt = timeline.indexOf('delete');
      expect(beatsSettledBefore(timeline, deleteAt)).toBe(true);
      await new Promise((resolve) => setTimeout(resolve, 60));
      expect(timeline.slice(deleteAt + 1)).toEqual([]);
    });

    it('settles the beat before setting the backoff of a failed delivery', async () => {
      const timeline = recordTimeline();
      const handler = vi.fn(async () => {
        await beatStarted();
        throw new Error('storage is down');
      });
      const { queue, options } = await worker(handler, {
        visibilityTimeoutSeconds: 45,
        internal: {
          log: () => {},
          heartbeatIntervalMs: 10,
          retryDelaySeconds: () => 3,
        },
      });
      serveMessages(mocks, options.workflowQueueUrl, [
        sqsMessage({ payload: { runId: 'wrun_A' } }),
      ]);
      await queue.startWorker();

      await vi.waitFor(() => expect(timeline).toContain('visibility 3 end'));
      const backoffAt = timeline.indexOf('visibility 3 start');
      expect(beatsSettledBefore(timeline, backoffAt)).toBe(true);
      // The backoff is the last visibility the message gets.
      await new Promise((resolve) => setTimeout(resolve, 60));
      expect(timeline.slice(backoffAt)).toEqual([
        'visibility 3 start',
        'visibility 3 end',
      ]);
    });
  });

  it('polls again after ReceiveMessage fails', async () => {
    const handler = vi.fn(async () => undefined);
    const log = vi.fn();
    const { queue, options } = await worker(handler, { internal: { log } });
    const message = sqsMessage({ payload: { runId: 'wrun_A' } });
    let calls = 0;
    mocks.sqsMock
      .on(ReceiveMessageCommand)
      .callsFake(async (input: { QueueUrl: string }) => {
        if (input.QueueUrl !== options.workflowQueueUrl) {
          await new Promise((resolve) => setTimeout(resolve, 5));
          return {};
        }
        calls++;
        if (calls === 1) throw new Error('AWS.SimpleQueueService.Unavailable');
        if (calls === 2) return { Messages: [message] };
        await new Promise((resolve) => setTimeout(resolve, 5));
        return {};
      });
    await queue.startWorker();

    await vi.waitFor(() => expect(handler).toHaveBeenCalledTimes(1));
    expect(log).toHaveBeenCalledWith(
      'ReceiveMessage failed; polling again after a backoff',
      expect.objectContaining({
        error: 'Error: AWS.SimpleQueueService.Unavailable',
      })
    );
  });

  it('backs off a message that is not a World message', async () => {
    const handler = vi.fn();
    const { queue, options } = await worker(handler);
    serveMessages(mocks, options.workflowQueueUrl, [
      {
        MessageId: 'x',
        ReceiptHandle: 'garbage',
        Body: 'not json',
        Attributes: { ApproximateReceiveCount: '1' },
      },
    ]);
    await queue.startWorker();
    await vi.waitFor(() =>
      expect(visibilityChanges()).toContainEqual(
        expect.objectContaining({
          ReceiptHandle: 'garbage',
          VisibilityTimeout: 3,
        })
      )
    );
    expect(handler).not.toHaveBeenCalled();
    expect(deleted()).toEqual([]);
  });

  it("runs one spec 9 run's orchestrator deliveries one at a time", async () => {
    let running = 0;
    let most = 0;
    const handler = vi.fn(async () => {
      running++;
      most = Math.max(most, running);
      await new Promise((resolve) => setTimeout(resolve, 30));
      running--;
    });
    const { queue, options } = await worker(handler);
    serveMessages(mocks, options.workflowQueueUrl, [
      sqsMessage({ payload: { runId: 'wrun_A', runSpecVersion: 9 } }),
      sqsMessage({ payload: { runId: 'wrun_A', runSpecVersion: 9 } }),
      sqsMessage({ payload: { runId: 'wrun_A', runSpecVersion: 9 } }),
    ]);
    await queue.startWorker();
    await vi.waitFor(() => expect(deleted()).toHaveLength(3));
    expect(most).toBe(1);
  });

  it('runs messages of different runs side by side', async () => {
    let running = 0;
    let most = 0;
    const handler = vi.fn(async () => {
      running++;
      most = Math.max(most, running);
      await new Promise((resolve) => setTimeout(resolve, 50));
      running--;
    });
    const { queue, options } = await worker(handler);
    serveMessages(mocks, options.workflowQueueUrl, [
      sqsMessage({ payload: { runId: 'wrun_A', runSpecVersion: 9 } }),
      sqsMessage({ payload: { runId: 'wrun_B', runSpecVersion: 9 } }),
      sqsMessage({ payload: { runId: 'wrun_C' } }),
    ]);
    await queue.startWorker();
    await vi.waitFor(() => expect(deleted()).toHaveLength(3));
    expect(most).toBeGreaterThan(1);
  });

  it('runs at most `concurrency` messages of a queue at once', async () => {
    let running = 0;
    let most = 0;
    const handler = vi.fn(async () => {
      running++;
      most = Math.max(most, running);
      await new Promise((resolve) => setTimeout(resolve, 20));
      running--;
    });
    const { queue, options } = await worker(handler, { concurrency: 2 });
    serveMessages(
      mocks,
      options.workflowQueueUrl,
      Array.from({ length: 6 }, (_, i) =>
        sqsMessage({ payload: { runId: `wrun_${i}` } })
      )
    );
    await queue.startWorker();
    await vi.waitFor(() => expect(deleted()).toHaveLength(6));
    expect(most).toBe(2);
  });

  it('starts one worker per process for the same queues', async () => {
    const options = queueOptions(mocks);
    serveMessages(mocks, options.workflowQueueUrl, []);
    const first = createSqsQueue(options);
    const second = createSqsQueue({ ...options });
    queues.push(first, second);
    expect(await first.startWorker()).toBe(true);
    expect(await first.startWorker()).toBe(false);
    expect(await second.startWorker()).toBe(false);
    // Once the first one closes, another World may take over.
    await first.close();
    expect(await second.startWorker()).toBe(true);
  });

  it('delivers over HTTP to the flow route when no handler is registered', async () => {
    const handler = vi.fn(async () => ({ timeoutSeconds: 2 }));
    const options = queueOptions(mocks);
    const queue = createSqsQueue(options);
    queues.push(queue);
    const route = queue.createQueueHandler(prefix, handler);
    const paths: string[] = [];
    const server: Server = createServer(async (req, res) => {
      paths.push(req.url ?? '');
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      const response = await route(
        new Request(`http://localhost${req.url}`, {
          method: 'POST',
          headers: req.headers as Record<string, string>,
          body: Buffer.concat(chunks),
        })
      );
      res.writeHead(response.status, { 'content-type': 'application/json' });
      res.end(await response.text());
    });
    await new Promise<void>((resolve) => server.listen(0, resolve));
    try {
      const { port } = server.address() as AddressInfo;
      const httpQueue = createSqsQueue({
        ...options,
        baseUrl: `http://127.0.0.1:${port}/`,
      });
      queues.push(httpQueue);
      await queue.close();
      const message = sqsMessage({ payload: { runId: 'wrun_A' } });
      serveMessages(mocks, options.workflowQueueUrl, [message]);
      expect(await httpQueue.startWorker()).toBe(true);

      await vi.waitFor(() =>
        expect(deleted()).toEqual([message.ReceiptHandle])
      );
      expect(paths).toEqual(['/.well-known/workflow/v1/flow']);
      expect(handler).toHaveBeenCalledWith(
        { runId: 'wrun_A' },
        expect.objectContaining({ attempt: 1, deliveryCount: 1 })
      );
      expect(sent(mocks)[0]?.delaySeconds).toBe(2);
    } finally {
      await new Promise((resolve) => server.close(resolve));
    }
  });

  it('retries a delivery that has nowhere to go', async () => {
    const log = vi.fn();
    const options = queueOptions(mocks, { internal: { log } });
    const queue = createSqsQueue(options);
    queues.push(queue);
    vi.stubEnv('PORT', '');
    vi.stubEnv('WORKFLOW_LOCAL_BASE_URL', '');
    try {
      const message = sqsMessage({ payload: { runId: 'wrun_A' } });
      serveMessages(mocks, options.workflowQueueUrl, [message]);
      await queue.startWorker();
      await vi.waitFor(() =>
        expect(visibilityChanges()).toContainEqual(
          expect.objectContaining({ ReceiptHandle: message.ReceiptHandle })
        )
      );
      expect(log.mock.calls[0]?.[1]).toMatchObject({
        error: expect.stringContaining('no base URL is known'),
      });
    } finally {
      vi.unstubAllEnvs();
    }
  });
});

describe('close()', () => {
  it('stops receiving and waits for a delivery that ends within the grace period', async () => {
    let started!: () => void;
    const running = new Promise<void>((resolve) => (started = resolve));
    const handler = vi.fn(async () => {
      started();
      await new Promise((resolve) => setTimeout(resolve, 150));
    });
    const { queue, options } = await worker(handler, {
      shutdownGraceMs: 2_000,
    });
    const message = sqsMessage({ payload: { runId: 'wrun_A' } });
    serveMessages(mocks, options.workflowQueueUrl, [message]);
    await queue.startWorker();
    await running;

    const before = Date.now();
    await queue.close();
    expect(Date.now() - before).toBeGreaterThanOrEqual(100);
    expect(deleted()).toEqual([message.ReceiptHandle]);
    const receives = mocks.sqsMock.commandCalls(ReceiveMessageCommand).length;
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(mocks.sqsMock.commandCalls(ReceiveMessageCommand)).toHaveLength(
      receives
    );
  });

  it('gives up on a delivery still running at the deadline, without acknowledging it', async () => {
    let started!: () => void;
    const running = new Promise<void>((resolve) => (started = resolve));
    let finished = false;
    const handler = vi.fn(async () => {
      started();
      await new Promise((resolve) => setTimeout(resolve, 400));
      finished = true;
    });
    const log = vi.fn();
    const { queue, options } = await worker(handler, {
      shutdownGraceMs: 50,
      internal: { log, heartbeatIntervalMs: 10 },
    });
    serveMessages(mocks, options.workflowQueueUrl, [
      sqsMessage({ payload: { runId: 'wrun_A' } }),
    ]);
    await queue.startWorker();
    await running;

    const before = Date.now();
    await queue.close();
    const took = Date.now() - before;
    expect(took).toBeGreaterThanOrEqual(45);
    expect(took).toBeLessThan(300);
    expect(finished).toBe(false);
    expect(log).toHaveBeenCalledWith(
      expect.stringContaining('close() gave up on 1 running deliveries'),
      undefined
    );
    // No more heartbeats, and no acknowledgment once the handler ends: SQS
    // delivers the message again after its visibility timeout.
    const beats = visibilityChanges().length;
    await vi.waitFor(() => expect(finished).toBe(true));
    await new Promise((resolve) => setTimeout(resolve, 30));
    expect(visibilityChanges()).toHaveLength(beats);
    expect(deleted()).toEqual([]);
  });

  it('gives a received message that has not started back at once, after its heartbeat', async () => {
    const timeline: string[] = [];
    mocks.sqsMock
      .on(ChangeMessageVisibilityCommand)
      .callsFake(
        async (input: { ReceiptHandle: string; VisibilityTimeout: number }) => {
          timeline.push(`${input.ReceiptHandle} ${input.VisibilityTimeout}`);
          if (input.VisibilityTimeout === 45) {
            await new Promise((resolve) => setTimeout(resolve, 30));
          }
          timeline.push(
            `${input.ReceiptHandle} ${input.VisibilityTimeout} end`
          );
          return {};
        }
      );
    let started!: () => void;
    const running = new Promise<void>((resolve) => (started = resolve));
    const handler = vi.fn(async () => {
      started();
      await new Promise((resolve) => setTimeout(resolve, 150));
    });
    const { queue, options } = await worker(handler, {
      shutdownGraceMs: 2_000,
      visibilityTimeoutSeconds: 45,
      internal: { log: () => {}, heartbeatIntervalMs: 10 },
    });
    // Two orchestrator messages of one spec 9 run: the second waits for the
    // first at the run gate, received but not started.
    const first = sqsMessage({
      payload: { runId: 'wrun_A', runSpecVersion: 9 },
    });
    const second = sqsMessage({
      payload: { runId: 'wrun_A', runSpecVersion: 9 },
    });
    serveMessages(mocks, options.workflowQueueUrl, [first, second]);
    await queue.startWorker();
    await running;
    await vi.waitFor(() =>
      expect(timeline).toContain(`${second.ReceiptHandle} 45`)
    );

    await queue.close();

    expect(handler).toHaveBeenCalledTimes(1);
    expect(deleted()).toEqual([first.ReceiptHandle]);
    const mine = timeline.filter((entry) =>
      entry.startsWith(`${second.ReceiptHandle} `)
    );
    // Given back with visibility 0, after every heartbeat of it had ended,
    // and nothing after that.
    const givenBack = mine.indexOf(`${second.ReceiptHandle} 0`);
    expect(givenBack).toBeGreaterThan(0);
    const before = mine.slice(0, givenBack);
    expect(before.filter((entry) => entry.endsWith(' 45')).length).toBe(
      before.filter((entry) => entry.endsWith(' 45 end')).length
    );
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(
      timeline
        .filter((entry) => entry.startsWith(`${second.ReceiptHandle} `))
        .slice(givenBack)
    ).toEqual([`${second.ReceiptHandle} 0`, `${second.ReceiptHandle} 0 end`]);
  });

  it('is idempotent and leaves a closed queue stopped', async () => {
    const { queue, options } = await worker(vi.fn());
    serveMessages(mocks, options.workflowQueueUrl, []);
    await queue.startWorker();
    const first = queue.close();
    expect(queue.close()).toBe(first);
    await first;
    expect(await queue.startWorker()).toBe(false);
  });
});

describe('scheduleOf', () => {
  it('rounds SQS delays up to whole seconds and chains past 15 minutes', () => {
    expect(scheduleOf(undefined)).toEqual({ delaySeconds: 0 });
    expect(scheduleOf(0)).toEqual({ delaySeconds: 0 });
    expect(scheduleOf(0.2)).toEqual({ delaySeconds: 1 });
    expect(scheduleOf(900)).toEqual({ delaySeconds: 900 });
    expect(scheduleOf(901, 1_000)).toEqual({
      delaySeconds: 900,
      notBefore: 902_000,
    });
  });
});
