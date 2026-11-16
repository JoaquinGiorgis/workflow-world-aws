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
import { PayloadStore } from './storage/payloads.js';
import { Table } from './storage/table.js';
import { createStreamer } from './streams/index.js';

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

/** The AWS World: a `World` whose optional queue methods are all there. */
export type AwsWorld = World &
  Required<Pick<Queue, 'queueBatch' | 'registerHandler' | 'registerRoute'>> & {
    start(): Promise<void>;
    close(): Promise<void>;
  };

/**
 * Creates the AWS World.
 *
 * Storage (runs, steps, events, hooks, VM snapshots) and streams live in
 * one DynamoDB table, the queue on two SQS standard queues (workflow and
 * step), and values too large for either in the S3 bucket, when one is
 * configured. `start()` re-enqueues the runs that have not ended and runs
 * the queue worker in this process.
 *
 * @param options - Explicit settings; anything left out is read from the
 *   `WORKFLOW_AWS_*` environment variables.
 */
export function createWorld(options: CreateWorldOptions = {}): AwsWorld {
  const config = resolveConfig(process.env, options);
  const clients = options.clients ?? createClients(config);
  // Reads resolve payloads in S3 even without a bucket configured now.
  const payloads = new PayloadStore({
    s3: clients.s3,
    ...(config.bucket !== undefined && { bucket: config.bucket }),
  });
  const storage = createStorage({
    documents: clients.documents,
    tableName: config.tableName,
    payloads,
    ...(options.retry && { retry: options.retry }),
  });
  const streamer = createStreamer({
    table: new Table(clients.documents, config.tableName, payloads),
    pollIntervalMs: config.streamPollIntervalMs,
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
    payloads,
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

    streams: streamer.streams,

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
        await streamer.close();
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
  PAYLOAD_BUCKET_NOT_FOUND_CODE,
  PAYLOAD_TOO_LARGE_CODE,
  PayloadBucketError,
  PayloadTooLargeError,
  type RetryOptions,
} from './storage/index.js';
