/**
 * The World's queue on two SQS standard queues: one for workflow
 * (orchestrator) messages and one for step messages, each with its own
 * dead-letter queue. Both carry the same queue names (`__wkf_workflow_*`);
 * a message with a `stepId` goes to the step queue, so step work and
 * orchestration poll and scale apart.
 *
 * - `queue()` sends one SQS message (`message.ts`), deduplicated by
 *   `idempotencyKey` on the World's table (`idempotency.ts`), with delays
 *   over 15 minutes chained (`consumer.ts`).
 * - `start()` runs the worker (`consumer.ts`), which delivers each message
 *   to the handler registered for its prefix in this process, or over HTTP
 *   to the app's route.
 * - `createQueueHandler()` is the route side (`handler.ts`).
 */
import {
  SendMessageBatchCommand,
  type SendMessageBatchRequestEntry,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import {
  MessageId,
  parseQueueName,
  parseQueueRoute,
  type Queue,
  type QueueBatchResult,
  type QueueOptions,
  type QueuePayload,
  QueuePrefix,
  type ValidQueueName,
} from '@workflow/world';
import {
  createNodeHttpAgents,
  destroyNodeHttpAgents,
} from '@workflow/world/node-http.js';
import { PayloadBucketError, PayloadTooLargeError } from '../storage/errors.js';
import type { PayloadStore } from '../storage/payloads.js';
import {
  Consumer,
  type ConsumerOptions,
  scheduleOf,
  sendQueueMessage,
} from './consumer.js';
import { deliver } from './delivery.js';
import { createQueueHandler } from './handler.js';
import { IdempotencyStore } from './idempotency.js';
import {
  encodeForQueue,
  MESSAGE_FORMAT_VERSION,
  type QueueMessage,
} from './message.js';
import { nextUlid, type SharedQueues, sharedQueues } from './state.js';

/** What `getDeploymentId()` answers: this World has one deployment. */
export const DEPLOYMENT_ID = 'aws';

/**
 * The `maxReceiveCount` the local bootstrap gives the queues' redrive
 * policy: world-postgres' job attempts (the runtime's delivery cap of 49 and
 * 24 more for its terminal write). With the worker's backoff that is days of
 * retries, well past the contract's minimum redelivery span of an hour.
 */
export const DEFAULT_MAX_RECEIVE_COUNT = 73;

/**
 * The most body bytes one SendMessageBatch carries: the size every queue
 * accepts for one message (`MAX_SQS_MESSAGE_BYTES`). SQS takes up to 1 MiB
 * per batch request, so this is conservative.
 */
const MAX_BATCH_BYTES = 256 * 1024;
const MAX_BATCH_ENTRIES = 10;

export interface SqsQueueOptions {
  sqs: SQSClient;
  documents: DynamoDBDocumentClient;
  tableName: string;
  workflowQueueUrl: string;
  stepQueueUrl: string;
  /** Identifies the queues in the process registry (see `state.ts`). */
  endpoint?: string;
  baseUrl?: string;
  concurrency: number;
  visibilityTimeoutSeconds: number;
  pollWaitSeconds: number;
  shutdownGraceMs: number;
  /**
   * Where messages too large for SQS travel (S3). Without a bucket such a
   * send fails with `PayloadTooLargeError`.
   */
  payloads?: PayloadStore;
  /** @internal Test hooks. */
  internal?: Pick<
    ConsumerOptions,
    'retryDelaySeconds' | 'heartbeatIntervalMs' | 'log'
  >;
}

export type SqsQueue = Queue &
  Required<Pick<Queue, 'queueBatch' | 'registerHandler' | 'registerRoute'>> & {
    /**
     * Starts the worker. Resolves `true` when this call started it, `false`
     * when it was running already (here or in another World over the same
     * queues in this process) or the queue is closed.
     */
    startWorker(): Promise<boolean>;
    /**
     * Whether `startWorker()` would start the worker now: the queue is open
     * and no worker runs over these queues in this process.
     */
    canStartWorker(): boolean;
    /**
     * Resolves once the idempotency keys of earlier sends are marked sent.
     * `queue()` answers before that write lands; a process about to be
     * frozen (a Lambda function answering its event) waits here first.
     * `close()` waits too.
     */
    settled(): Promise<void>;
    start(): Promise<void>;
    close(): Promise<void>;
  };

/** Whether a payload is a step's execution message. */
function isStepMessage(payload: unknown): boolean {
  return (
    typeof payload === 'object' &&
    payload !== null &&
    typeof (payload as { stepId?: unknown }).stepId === 'string'
  );
}

export function createSqsQueue(options: SqsQueueOptions): SqsQueue {
  const { sqs, workflowQueueUrl, stepQueueUrl } = options;
  const shared: SharedQueues = sharedQueues(
    [options.endpoint ?? '', workflowQueueUrl, stepQueueUrl].join('|')
  );
  const log =
    options.internal?.log ??
    ((message: string, detail?: Record<string, unknown>) =>
      detail === undefined
        ? console.warn(`[world-aws] ${message}`)
        : console.warn(`[world-aws] ${message}`, detail));
  const idempotency = new IdempotencyStore(
    options.documents,
    options.tableName,
    log
  );
  const agents = createNodeHttpAgents({
    maxSockets: Number.POSITIVE_INFINITY,
    keepAliveMs: 30_000,
  });
  let consumer: Consumer | undefined;
  let closed = false;
  let closing: Promise<void> | undefined;
  /** Confirmations of idempotency keys still in flight. */
  const confirming = new Set<Promise<void>>();

  /**
   * Marks a key's message sent without holding up the send: the message is
   * in SQS already, and a confirmation that never lands only lets the key
   * be taken over after `RESERVATION_TIMEOUT_MS`. `confirm` logs and
   * swallows its own errors.
   */
  const confirmLater = (key: string, messageId: MessageId, at: number) => {
    const pending: Promise<void> = idempotency
      .confirm(key, messageId, at)
      .finally(() => confirming.delete(pending));
    confirming.add(pending);
  };

  /** Resolves once no confirmation is in flight. */
  const settled = async () => {
    while (confirming.size > 0) await Promise.allSettled([...confirming]);
  };

  const queueUrlFor = (payload: unknown) =>
    isStepMessage(payload) ? stepQueueUrl : workflowQueueUrl;

  /** The SQS message of a send, before deduplication. */
  const prepare = (
    queueName: ValidQueueName,
    payload: QueuePayload,
    opts: QueueOptions | undefined,
    messageId: MessageId
  ): QueueMessage => {
    const { prefix } = parseQueueName(queueName);
    const route = shared.routes.get(prefix);
    return {
      v: MESSAGE_FORMAT_VERSION,
      messageId,
      queueName,
      payload,
      createdAt: new Date().toISOString(),
      ...(opts?.idempotencyKey !== undefined && {
        idempotencyKey: opts.idempotencyKey,
      }),
      ...(opts?.headers && { headers: opts.headers }),
      ...(route !== undefined && { route }),
    };
  };

  const newMessageId = () => MessageId.parse(`msg_${nextUlid()}`);

  const queue: Queue['queue'] = async (queueName, payload, opts) => {
    parseQueueName(queueName);
    const key = opts?.idempotencyKey;
    let messageId = newMessageId();
    let reserved: { key: string; at: number } | undefined;
    if (key) {
      const reservation = await idempotency.reserve(key, messageId);
      if (!reservation.owner) return { messageId: reservation.messageId };
      messageId = reservation.messageId;
      reserved = { key, at: reservation.reservedAt };
    }

    const message = prepare(queueName, payload, opts, messageId);
    try {
      await sendQueueMessage(
        sqs,
        queueUrlFor(payload),
        message,
        opts?.delaySeconds,
        options.payloads
      );
    } catch (error) {
      if (reserved) {
        await idempotency.release(reserved.key, messageId, reserved.at);
      }
      throw error;
    }
    if (reserved) confirmLater(reserved.key, messageId, reserved.at);
    return { messageId };
  };

  const queueBatch: NonNullable<Queue['queueBatch']> = async (
    queueName,
    messages
  ) => {
    parseQueueName(queueName);
    const results = messages.map(
      (): QueueBatchResult => ({
        messageId: null,
        error: 'not sent',
        retryable: true,
      })
    );
    type Pending = {
      index: number;
      /** The idempotency key this send reserved, and when. */
      reserved?: { key: string; at: number };
      message: QueueMessage;
      entry: SendMessageBatchRequestEntry;
      bytes: number;
    };
    const byQueue = new Map<string, Pending[]>();
    await Promise.all(
      messages.map(async ({ message: payload, opts }, index) => {
        const key = opts?.idempotencyKey;
        let messageId = newMessageId();
        let reserved: Pending['reserved'];
        try {
          if (key) {
            const reservation = await idempotency.reserve(key, messageId);
            if (!reservation.owner) {
              results[index] = { messageId: reservation.messageId };
              return;
            }
            messageId = reservation.messageId;
            reserved = { key, at: reservation.reservedAt };
          }
        } catch (error) {
          results[index] = {
            messageId: null,
            error: describe(error),
            retryable: true,
          };
          return;
        }
        const schedule = scheduleOf(opts?.delaySeconds);
        const message: QueueMessage = {
          ...prepare(queueName, payload, opts, messageId),
          ...(schedule.notBefore !== undefined && {
            notBefore: schedule.notBefore,
          }),
        };
        let body: string;
        try {
          body = await encodeForQueue(message, options.payloads);
        } catch (error) {
          results[index] = {
            messageId: null,
            error: describe(error),
            retryable: !(
              error instanceof PayloadTooLargeError ||
              error instanceof PayloadBucketError
            ),
          };
          if (reserved) {
            await idempotency.release(reserved.key, messageId, reserved.at);
          }
          return;
        }
        const url = queueUrlFor(payload);
        const pending = byQueue.get(url) ?? [];
        pending.push({
          index,
          ...(reserved !== undefined && { reserved }),
          message,
          entry: {
            Id: String(index),
            MessageBody: body,
            ...(schedule.delaySeconds > 0 && {
              DelaySeconds: schedule.delaySeconds,
            }),
          },
          bytes: Buffer.byteLength(body),
        });
        byQueue.set(url, pending);
      })
    );

    const sendChunk = async (url: string, chunk: Pending[]) => {
      const byId = new Map(chunk.map((each) => [each.entry.Id, each]));
      try {
        const output = await sqs.send(
          new SendMessageBatchCommand({
            QueueUrl: url,
            Entries: chunk.map((each) => each.entry),
          })
        );
        for (const ok of output.Successful ?? []) {
          const each = byId.get(ok.Id);
          if (!each) continue;
          results[each.index] = { messageId: each.message.messageId };
          if (each.reserved) {
            confirmLater(
              each.reserved.key,
              each.message.messageId,
              each.reserved.at
            );
          }
        }
        for (const failed of output.Failed ?? []) {
          const each = byId.get(failed.Id);
          if (!each) continue;
          results[each.index] = {
            messageId: null,
            error: `${failed.Code ?? 'Error'}: ${failed.Message ?? 'not sent'}`,
            retryable: failed.SenderFault !== true,
          };
          if (each.reserved) {
            await idempotency.release(
              each.reserved.key,
              each.message.messageId,
              each.reserved.at
            );
          }
        }
      } catch (error) {
        for (const each of chunk) {
          results[each.index] = {
            messageId: null,
            error: describe(error),
            retryable: true,
          };
          if (each.reserved) {
            await idempotency.release(
              each.reserved.key,
              each.message.messageId,
              each.reserved.at
            );
          }
        }
      }
    };

    const sends: Promise<void>[] = [];
    for (const [url, pending] of byQueue) {
      let chunk: Pending[] = [];
      let bytes = 0;
      for (const each of pending) {
        if (
          chunk.length === MAX_BATCH_ENTRIES ||
          (chunk.length > 0 && bytes + each.bytes > MAX_BATCH_BYTES)
        ) {
          sends.push(sendChunk(url, chunk));
          chunk = [];
          bytes = 0;
        }
        chunk.push(each);
        bytes += each.bytes;
      }
      if (chunk.length > 0) sends.push(sendChunk(url, chunk));
    }
    await Promise.all(sends);
    return results;
  };

  const canStartWorker = () => !closed && !consumer && !shared.consumer;

  const startWorker = async (): Promise<boolean> => {
    if (!canStartWorker()) return false;
    consumer = new Consumer({
      sqs,
      queueUrls: [workflowQueueUrl, stepQueueUrl],
      concurrency: options.concurrency,
      visibilityTimeoutSeconds: options.visibilityTimeoutSeconds,
      pollWaitSeconds: options.pollWaitSeconds,
      shutdownGraceMs: options.shutdownGraceMs,
      runGate: shared.runGate,
      ...(options.payloads && { payloads: options.payloads }),
      deliver: (delivery) =>
        deliver(
          {
            handlers: shared.handlers,
            routes: shared.routes,
            ...(options.baseUrl !== undefined && { baseUrl: options.baseUrl }),
            agents,
          },
          delivery
        ),
      ...options.internal,
    });
    shared.consumer = consumer;
    consumer.start();
    return true;
  };

  return {
    // As world-postgres: the runtime does not check deliveries to the flow
    // route. IAM decides who can send to the queues, and the route has to be
    // protected at the network edge (see the README).
    queueAuthMode: 'skip',
    getDeploymentId: async () => DEPLOYMENT_ID,
    queue,
    queueBatch,
    createQueueHandler,
    registerHandler(prefix, handler) {
      shared.handlers.set(QueuePrefix.parse(prefix), handler);
    },
    registerRoute(prefix, path) {
      shared.routes.set(QueuePrefix.parse(prefix), parseQueueRoute(path));
    },
    startWorker,
    canStartWorker,
    async start() {
      await startWorker();
    },
    settled,
    close() {
      closed = true;
      closing ??= (async () => {
        if (consumer) {
          await consumer.close();
          if (shared.consumer === consumer) delete shared.consumer;
        }
        await settled();
        destroyNodeHttpAgents(agents);
      })();
      return closing;
    },
  };
}

function describe(error: unknown): string {
  return error instanceof Error
    ? `${error.name}: ${error.message}`
    : String(error);
}
