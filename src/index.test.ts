import { SPEC_VERSION_DEFAULT_MINTED } from '@workflow/world';
import { describe, expect, it } from 'vitest';
import {
  AwsWorldConfigError,
  createWorld,
  NotImplementedError,
} from './index.js';

const options = {
  region: 'us-east-1',
  tableName: 'workflow',
  workflowQueueUrl: 'http://localhost:4566/000000000000/workflow',
  stepQueueUrl: 'http://localhost:4566/000000000000/step',
  endpoint: 'http://localhost:4566',
};

describe('createWorld (M0)', () => {
  it('validates the configuration up front', () => {
    expect(() => createWorld({ ...options, region: 'nope' })).toThrow(
      AwsWorldConfigError
    );
  });

  it('declares the minted spec version', async () => {
    const world = createWorld(options);
    expect(world.specVersion).toBe(SPEC_VERSION_DEFAULT_MINTED);
    await world.close?.();
  });

  it('throws "not implemented yet (M1)" from storage, queue and streams', async () => {
    const world = createWorld(options);
    const calls: [string, () => unknown][] = [
      ['runs.get', () => world.runs.get('wrun_x')],
      ['events.list', () => world.events.list({ runId: 'wrun_x' })],
      ['hooks.getByToken', () => world.hooks.getByToken('t')],
      ['getDeploymentId', () => world.getDeploymentId()],
      ['streams.write', () => world.streams.write('wrun_x', 's', 'chunk')],
    ];
    for (const [method, call] of calls) {
      expect(call).toThrow(NotImplementedError);
      expect(call).toThrow(`${method} is not implemented yet (M1)`);
    }
    await world.close?.();
  });
});
