import {
  mintedSpecVersion,
  type Queue,
  reenqueueActiveRuns,
  type World,
} from '@workflow/world';
import { type AwsClients, createClients, destroyClients } from './clients.js';
import { type AwsWorldOptions, resolveConfig } from './config.js';
import { createSqsQueue } from './queue/index.js';
import {
  createStorage,
  type RetryOptions,
  STORAGE_CAPABILITIES,
} from './storage/index.js';

export interface CreateWorldOptions extends AwsWorldOptions {
  /**
   * SDK clients to use instead of creating them, for example clients with
   * custom middleware, instrumentation or credentials. The World does not
   * destroy clients it was given; `region` and `endpoint` still have to
   * describe where they point.
   */
  clients?: AwsClients;
  /**
   * How a write that loses a race with another writer of the same run is
   * retried: by default 40 attempts, the first two at once and then with
   * exponential backoff and full jitter capped at 250 ms, after which it
   * fails with a 503 `WorkflowWorldError`.
   */
  retry?: Partial<RetryOptions>;
}

/** Thrown by the stream methods (until M3). */
export class NotImplementedError extends Error {
  constructor(method: string, milestone: 'M3') {
    super(
      `@joaquingiorgis/world-aws: ${method} is not implemented yet (${milestone})`
    );
    this.name = 'NotImplementedError';
  }
}

function notImplemented(
  method: string,
  milestone: 'M3'
): (...args: unknown[]) => never {
  return () => {
    throw new NotImplementedError(method, milestone);
  };
}

/** The AWS World: a `World` whose optional queue methods are all there. */
export type AwsWorld = World &
  Required<Pick<Queue, 'queueBatch' | 'registerHandler' | 'registerRoute'>> & {
    start(): Promise<void>;
    close(): Promise<void>;
  };

/**
 * Creates the AWS World.
 *
 * Storage (runs, steps, events, hooks, VM snapshots) lives in one DynamoDB
 * table, and the queue on two SQS standard queues (workflow and step).
 * `start()` re-enqueues the runs that have not ended and runs the queue
 * worker in this process. Streams (M3) are not implemented yet and throw
 * {@link NotImplementedError}.
 *
 * @param options - Explicit settings; anything left out is read from the
 *   `WORKFLOW_AWS_*` environment variables.
 */
export function createWorld(options: CreateWorldOptions = {}): AwsWorld {
  const config = resolveConfig(process.env, options);
  const clients = options.clients ?? createClients(config);
  const storage = createStorage({
    documents: clients.documents,
    tableName: config.tableName,
    ...(options.retry && { retry: options.retry }),
  });
  const queue = createSqsQueue({
    sqs: clients.sqs,
    documents: clients.documents,
    tableName: config.tableName,
    workflowQueueUrl: config.workflowQueueUrl,
    stepQueueUrl: config.stepQueueUrl,
    ...(config.endpoint !== undefined && { endpoint: config.endpoint }),
    ...(config.baseUrl !== undefined && { baseUrl: config.baseUrl }),
    concurrency: config.concurrency,
    visibilityTimeoutSeconds: config.visibilityTimeoutSeconds,
    pollWaitSeconds: config.pollWaitSeconds,
    shutdownGraceMs: config.shutdownGraceMs,
  });
  let closing: Promise<void> | undefined;
  let starting: Promise<void> | undefined;

  return {
    // Spec 9 runs only on request (`WORKFLOW_SPEC_VERSION=9`): the storage
    // implements the in-band fence they need. `maxConcurrency` is not
    // declared: SQS standard queues cannot serialize a run's orchestrator
    // deliveries across processes (see `queue/consumer.ts`).
    specVersion: mintedSpecVersion(process.env, {
      inBandFence: STORAGE_CAPABILITIES.inBandFence,
    }),
    capabilities: STORAGE_CAPABILITIES,

    ...storage,

    queueAuthMode: queue.queueAuthMode,
    getDeploymentId: queue.getDeploymentId,
    queue: queue.queue,
    queueBatch: queue.queueBatch,
    createQueueHandler: queue.createQueueHandler,
    registerHandler: queue.registerHandler,
    registerRoute: queue.registerRoute,

    streams: {
      write: notImplemented('streams.write', 'M3'),
      close: notImplemented('streams.close', 'M3'),
      get: notImplemented('streams.get', 'M3'),
      list: notImplemented('streams.list', 'M3'),
      getChunks: notImplemented('streams.getChunks', 'M3'),
      getInfo: notImplemented('streams.getInfo', 'M3'),
    },

    start() {
      // Recovery, as world-postgres does on start: every run that has not
      // ended gets a message, in case the one that would continue it was
      // lost. A duplicate costs a replay that finds nothing to do, so the
      // eventually consistent index read (a run created a moment ago may be
      // missing, and its own message is in SQS anyway) is fine.
      //
      // It runs before the worker starts: when it fails (a missing index, a
      // throttled query), start() rejects with no worker running, and the
      // next start() recovers and starts it.
      starting ??= (async () => {
        if (!queue.canStartWorker()) return;
        await reenqueueActiveRuns(storage.runs, queue.queue, 'world-aws');
        await queue.startWorker();
      })().finally(() => {
        starting = undefined;
      });
      return starting;
    },

    close() {
      closing ??= (async () => {
        await queue.close();
        if (!options.clients) destroyClients(clients);
      })();
      return closing;
    },
  };
}

export default createWorld;

export { type AwsClients, createClients, destroyClients } from './clients.js';
export {
  type AwsWorldConfig,
  AwsWorldConfigError,
  type AwsWorldOptions,
  resolveConfig,
} from './config.js';
export {
  DEFAULT_MAX_RECEIVE_COUNT,
  DEPLOYMENT_ID,
  type SqsQueue,
  type SqsQueueOptions,
} from './queue/index.js';
export {
  MAX_ITEM_BYTES,
  MISSING_TABLE_INDEX_CODE,
  MissingTableIndexError,
  PAYLOAD_TOO_LARGE_CODE,
  PayloadTooLargeError,
  type RetryOptions,
} from './storage/index.js';
