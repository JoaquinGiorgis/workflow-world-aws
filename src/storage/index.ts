import type { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { Storage, WorldCapabilities } from '@workflow/world';
import { DEFAULT_RETRY, type RetryOptions } from './commit.js';
import type { StorageContext } from './context.js';
import { createEventsStorage } from './events.js';
import { createHooksStorage } from './hooks.js';
import type { PayloadStore } from './payloads.js';
import { createRunsStorage } from './runs.js';
import { createSnapshotsStorage } from './snapshots.js';
import { createStepsStorage } from './steps.js';
import { Table } from './table.js';

export interface StorageOptions {
  documents: DynamoDBDocumentClient;
  tableName: string;
  /** Retries of a write that loses a race; see `commit.ts`. */
  retry?: Partial<RetryOptions>;
  /**
   * Where values too large for an item go (S3). Without one, a write that
   * does not fit is refused with `PayloadTooLargeError`.
   */
  payloads?: PayloadStore;
}

/**
 * What the storage implements, in `WorldCapabilities` terms. Queue
 * capabilities (`maxConcurrency`, `invoke`) belong to the queue.
 */
export const STORAGE_CAPABILITIES = {
  hookRetention: { active: true },
  atomicStartHook: { active: true },
  dynamicWorkflowCode: true,
  hookResumeDedup: true,
  hookForceClaim: true,
  inBandFence: true,
} as const satisfies WorldCapabilities;

/** The World's storage over one DynamoDB table. */
export function createStorage(options: StorageOptions): Storage {
  const context: StorageContext = {
    table: new Table(options.documents, options.tableName, options.payloads),
    retry: { ...DEFAULT_RETRY, ...options.retry },
  };
  return {
    runs: createRunsStorage(context.table, context.retry),
    steps: createStepsStorage(context.table),
    events: createEventsStorage(context),
    hooks: createHooksStorage(context.table),
    experimental_snapshots: createSnapshotsStorage(context.table),
  };
}

export type { RetryOptions } from './commit.js';
export {
  MAX_ITEM_BYTES,
  MISSING_TABLE_INDEX_CODE,
  MissingTableIndexError,
  PAYLOAD_BUCKET_NOT_FOUND_CODE,
  PAYLOAD_TOO_LARGE_CODE,
  PayloadBucketError,
  PayloadTooLargeError,
} from './errors.js';
