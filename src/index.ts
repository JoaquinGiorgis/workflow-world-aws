import { mintedSpecVersion, type World } from '@workflow/world';
import { type AwsClients, createClients, destroyClients } from './clients.js';
import { type AwsWorldOptions, resolveConfig } from './config.js';
import { createStorage, STORAGE_CAPABILITIES } from './storage/index.js';

export interface CreateWorldOptions extends AwsWorldOptions {
  /**
   * SDK clients to use instead of creating them, for example clients with
   * custom middleware, instrumentation or credentials. The World does not
   * destroy clients it was given; `region` and `endpoint` still have to
   * describe where they point.
   */
  clients?: AwsClients;
}

/** Thrown by the queue (until M2) and stream (until M3) methods. */
export class NotImplementedError extends Error {
  constructor(method: string, milestone: 'M2' | 'M3') {
    super(
      `@joaquingiorgis/world-aws: ${method} is not implemented yet (${milestone})`
    );
    this.name = 'NotImplementedError';
  }
}

function notImplemented(
  method: string,
  milestone: 'M2' | 'M3'
): (...args: unknown[]) => never {
  return () => {
    throw new NotImplementedError(method, milestone);
  };
}

/**
 * Creates the AWS World.
 *
 * Storage (runs, steps, events, hooks, VM snapshots) lives in one DynamoDB
 * table. The queue (M2) and streams (M3) are not implemented yet and throw
 * {@link NotImplementedError}.
 *
 * @param options - Explicit settings; anything left out is read from the
 *   `WORKFLOW_AWS_*` environment variables.
 */
export function createWorld(options: CreateWorldOptions = {}): World {
  const config = resolveConfig(process.env, options);
  const clients = options.clients ?? createClients(config);

  return {
    // Spec 9 runs only on request (`WORKFLOW_SPEC_VERSION=9`): the storage
    // implements the in-band fence they need.
    specVersion: mintedSpecVersion(process.env, {
      inBandFence: STORAGE_CAPABILITIES.inBandFence,
    }),
    capabilities: STORAGE_CAPABILITIES,

    ...createStorage({
      documents: clients.documents,
      tableName: config.tableName,
    }),

    getDeploymentId: notImplemented('getDeploymentId', 'M2'),
    queue: notImplemented('queue', 'M2'),
    createQueueHandler: notImplemented('createQueueHandler', 'M2'),

    streams: {
      write: notImplemented('streams.write', 'M3'),
      close: notImplemented('streams.close', 'M3'),
      get: notImplemented('streams.get', 'M3'),
      list: notImplemented('streams.list', 'M3'),
      getChunks: notImplemented('streams.getChunks', 'M3'),
      getInfo: notImplemented('streams.getInfo', 'M3'),
    },

    async close() {
      if (!options.clients) destroyClients(clients);
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
  MAX_ITEM_BYTES,
  PAYLOAD_TOO_LARGE_CODE,
  PayloadTooLargeError,
} from './storage/index.js';
