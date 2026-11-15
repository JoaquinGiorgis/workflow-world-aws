import { mintedSpecVersion, type World } from '@workflow/world';
import { createClients, destroyClients } from './clients.js';
import { type AwsWorldOptions, resolveConfig } from './config.js';

const NOT_IMPLEMENTED_MESSAGE = 'not implemented yet (M1)';

/** Thrown by every storage, queue and stream method until M1 lands. */
export class NotImplementedError extends Error {
  constructor(method: string) {
    super(`@joaquingiorgis/world-aws: ${method} is ${NOT_IMPLEMENTED_MESSAGE}`);
    this.name = 'NotImplementedError';
  }
}

function notImplemented(method: string): (...args: unknown[]) => never {
  return () => {
    throw new NotImplementedError(method);
  };
}

/**
 * Creates the AWS World.
 *
 * M0 scaffold: the configuration is resolved and validated, and the SDK
 * clients are created, but every storage, queue and stream method throws
 * {@link NotImplementedError}.
 *
 * @param options - Explicit settings; anything left out is read from the
 *   `WORKFLOW_AWS_*` environment variables.
 */
export function createWorld(options: AwsWorldOptions = {}): World {
  const config = resolveConfig(process.env, options);
  const clients = createClients(config);

  return {
    specVersion: mintedSpecVersion(),

    runs: {
      get: notImplemented('runs.get'),
      list: notImplemented('runs.list'),
    },
    steps: {
      get: notImplemented('steps.get'),
      list: notImplemented('steps.list'),
    },
    events: {
      create: notImplemented('events.create'),
      get: notImplemented('events.get'),
      list: notImplemented('events.list'),
      listByCorrelationId: notImplemented('events.listByCorrelationId'),
    },
    hooks: {
      get: notImplemented('hooks.get'),
      getByToken: notImplemented('hooks.getByToken'),
      list: notImplemented('hooks.list'),
    },

    getDeploymentId: notImplemented('getDeploymentId'),
    queue: notImplemented('queue'),
    createQueueHandler: notImplemented('createQueueHandler'),

    streams: {
      write: notImplemented('streams.write'),
      close: notImplemented('streams.close'),
      get: notImplemented('streams.get'),
      list: notImplemented('streams.list'),
      getChunks: notImplemented('streams.getChunks'),
      getInfo: notImplemented('streams.getInfo'),
    },

    async close() {
      destroyClients(clients);
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
