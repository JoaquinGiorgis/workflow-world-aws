import { TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import {
  GetCommand,
  QueryCommand,
  TransactWriteCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  SPEC_VERSION_DEFAULT_MINTED,
  SPEC_VERSION_SINGLE_ORCHESTRATOR,
} from '@workflow/world';
import { mockClient } from 'aws-sdk-client-mock';
import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  AwsWorldConfigError,
  createClients,
  createWorld,
  destroyClients,
  NotImplementedError,
} from './index.js';

const options = {
  region: 'us-east-1',
  tableName: 'workflow',
  workflowQueueUrl: 'http://localhost:4566/000000000000/workflow',
  stepQueueUrl: 'http://localhost:4566/000000000000/step',
  endpoint: 'http://localhost:4566',
};

afterEach(() => {
  vi.unstubAllEnvs();
});

describe('createWorld (M1)', () => {
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

  it('creates spec 9 runs on request, since the storage fences in-band writes', async () => {
    vi.stubEnv('WORKFLOW_SPEC_VERSION', '9');
    const world = createWorld(options);
    expect(world.specVersion).toBe(SPEC_VERSION_SINGLE_ORCHESTRATOR);
    await world.close?.();
  });

  it('declares the storage capabilities and no queue capability', async () => {
    const world = createWorld(options);
    expect(world.capabilities).toEqual({
      hookRetention: { active: true },
      atomicStartHook: { active: true },
      dynamicWorkflowCode: true,
      hookResumeDedup: true,
      hookForceClaim: true,
      inBandFence: true,
    });
    await world.close?.();
  });

  it('implements storage, including the optional parts', async () => {
    const world = createWorld(options);
    expect(typeof world.runs.getMany).toBe('function');
    expect(typeof world.runs.waitForTerminalStatus).toBe('function');
    expect(typeof world.runs.experimentalSetAttributes).toBe('function');
    expect(typeof world.experimental_snapshots?.save).toBe('function');
    expect(world.events.createBatch).toBeUndefined();
    expect(world.events.subscribe).toBeUndefined();
    await world.close?.();
  });

  it('throws "(M2)" from the queue and "(M3)" from streams', async () => {
    const world = createWorld(options);
    const calls: [string, string, () => unknown][] = [
      ['getDeploymentId', 'M2', () => world.getDeploymentId()],
      [
        'queue',
        'M2',
        () => world.queue('__wkf_workflow_x' as never, {} as never),
      ],
      [
        'streams.write',
        'M3',
        () => world.streams.write('wrun_x', 's', 'chunk'),
      ],
      ['streams.get', 'M3', () => world.streams.get('wrun_x', 's')],
    ];
    for (const [method, milestone, call] of calls) {
      expect(call).toThrow(NotImplementedError);
      expect(call).toThrow(`${method} is not implemented yet (${milestone})`);
    }
    await world.close?.();
  });

  it('passes `retry` to the storage', async () => {
    const clients = createClients(options);
    const ddb = mockClient(clients.documents);
    ddb.on(GetCommand).resolves({});
    ddb.on(GetCommand, { Key: { PK: 'RUN#wrun_A', SK: 'META' } }).resolves({
      Item: {
        PK: 'RUN#wrun_A',
        SK: 'META',
        runId: 'wrun_A',
        status: 'running',
        deploymentId: 'd',
        workflowName: 'w',
        specVersion: 8,
        createdAt: '2026-11-15T20:00:00.000Z',
        updatedAt: '2026-11-15T20:00:00.000Z',
        seq: 1,
        seqInBand: 1,
        rev: 1,
      },
    });
    ddb.on(QueryCommand).resolves({ Items: [] });
    ddb.on(TransactWriteCommand).rejects(
      new TransactionCanceledException({
        message: 'Transaction cancelled',
        $metadata: {},
        CancellationReasons: [{ Code: 'ConditionalCheckFailed' }],
      })
    );
    const world = createWorld({
      ...options,
      clients,
      retry: { maxAttempts: 2, immediateAttempts: 2 },
    });
    try {
      await expect(
        world.events.create('wrun_A', {
          eventType: 'wait_created',
          correlationId: 'wait_1',
          eventData: { resumeAt: new Date() },
        })
      ).rejects.toMatchObject({ name: 'WorkflowWorldError', status: 503 });
      expect(ddb.commandCalls(TransactWriteCommand)).toHaveLength(2);
    } finally {
      ddb.restore();
      destroyClients(clients);
    }
  });

  it('leaves clients it was given open on close', async () => {
    const clients = createClients(options);
    const destroy = vi.spyOn(clients.dynamodb, 'destroy');
    const world = createWorld({ ...options, clients });
    await world.close?.();
    expect(destroy).not.toHaveBeenCalled();
    destroyClients(clients);
  });
});
