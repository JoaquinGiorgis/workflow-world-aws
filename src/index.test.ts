import { TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import { ReceiveMessageCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
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

describe('createWorld', () => {
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

  it('implements the queue', async () => {
    const world = createWorld(options);
    expect(world.queueAuthMode).toBe('skip');
    expect(await world.getDeploymentId()).toBe('aws');
    expect(typeof world.queue).toBe('function');
    expect(typeof world.queueBatch).toBe('function');
    expect(typeof world.createQueueHandler('__wkf_workflow_', vi.fn())).toBe(
      'function'
    );
    await world.close();
  });

  it('implements streams, with every reader stopped by close()', async () => {
    const clients = createClients(options);
    const ddb = mockClient(clients.documents);
    ddb.on(QueryCommand).resolves({ Items: [] });
    ddb.on(GetCommand).resolves({});
    const world = createWorld({
      ...options,
      clients,
      streamPollIntervalMs: 10,
    });
    try {
      for (const method of [
        'write',
        'writeMulti',
        'close',
        'get',
        'list',
        'getChunks',
        'getInfo',
      ] as const) {
        expect(typeof world.streams[method]).toBe('function');
      }
      // No write session: the runtime uses the stateless methods, as with
      // world-postgres.
      expect(world.streams.createWriteSession).toBeUndefined();
      const reader = (await world.streams.get('wrun_x', 's')).getReader();
      const pending = reader.read();
      await world.close();
      await expect(pending).rejects.toThrow('the AWS World has been closed');
      await expect(world.streams.get('wrun_x', 's')).rejects.toThrow(
        'the AWS World has been closed'
      );
    } finally {
      await world.close();
      ddb.restore();
      destroyClients(clients);
    }
  });

  it('re-enqueues the runs that have not ended when it starts', async () => {
    const clients = createClients(options);
    const ddb = mockClient(clients.documents);
    const sqs = mockClient(clients.sqs);
    const run = (runId: string, extra: Record<string, unknown> = {}) => ({
      PK: `RUN#${runId}`,
      SK: 'META',
      runId,
      status: 'running',
      deploymentId: 'aws',
      workflowName: 'wf',
      specVersion: 9,
      input: new Uint8Array(),
      createdAt: '2026-11-15T20:00:00.000Z',
      updatedAt: '2026-11-15T20:00:00.000Z',
      ...extra,
    });
    ddb.on(QueryCommand).resolves({
      Items: [
        run('wrun_01JD000000000000000000000A'),
        run('wrun_01JD000000000000000000000B', {
          status: 'pending',
          specVersion: 8,
          executionContext: { queueNamespace: 'agent' },
        }),
      ],
    });
    sqs.on(SendMessageCommand).resolves({});
    sqs.on(ReceiveMessageCommand).callsFake(async () => {
      await new Promise((resolve) => setTimeout(resolve, 5));
      return {};
    });
    const world = createWorld({
      ...options,
      workflowQueueUrl: `${options.workflowQueueUrl}-recovery`,
      clients,
      pollWaitSeconds: 0,
    });
    try {
      await world.start();
      const query = ddb.commandCalls(QueryCommand)[0]?.args[0].input;
      expect(query?.IndexName).toBe('GSI3');
      const sentBodies = sqs
        .commandCalls(SendMessageCommand)
        .map((call) => JSON.parse(call.args[0].input.MessageBody ?? '{}'));
      expect(sentBodies.map((body) => [body.queueName, body.payload])).toEqual([
        [
          '__wkf_workflow_wf',
          { runId: 'wrun_01JD000000000000000000000A', runSpecVersion: 9 },
        ],
        [
          '__agent_wkf_workflow_wf',
          { runId: 'wrun_01JD000000000000000000000B', runSpecVersion: 8 },
        ],
      ]);
      // A second start of the same World recovers nothing again.
      await world.start();
      expect(sqs.commandCalls(SendMessageCommand)).toHaveLength(2);
    } finally {
      await world.close();
      ddb.restore();
      sqs.restore();
      destroyClients(clients);
    }
  });

  describe('when recovery fails', () => {
    const missingIndex = (name: string, message: string) =>
      Object.assign(new Error(message), { name });
    const activeRun = {
      PK: 'RUN#wrun_01JD000000000000000000000A',
      SK: 'META',
      runId: 'wrun_01JD000000000000000000000A',
      status: 'running',
      deploymentId: 'aws',
      workflowName: 'wf',
      specVersion: 8,
      input: new Uint8Array(),
      createdAt: '2026-11-15T20:00:00.000Z',
      updatedAt: '2026-11-15T20:00:00.000Z',
    };

    it.each([
      [
        'DynamoDB',
        'ValidationException',
        'The table does not have the specified index: GSI3',
      ],
      [
        'moto',
        'ResourceNotFoundException',
        'Invalid index: GSI3 for table: workflow. Available indexes are: GSI1, GSI2',
      ],
    ])(
      'names the missing GSI3 and how to add it (%s)',
      async (_, name, message) => {
        const clients = createClients(options);
        const ddb = mockClient(clients.documents);
        ddb.on(QueryCommand).rejects(missingIndex(name, message));
        const world = createWorld({ ...options, clients });
        try {
          const listing = world.runs.list({ status: 'running' });
          await expect(listing).rejects.toMatchObject({
            name: 'MissingTableIndexError',
            indexName: 'GSI3',
            tableName: 'workflow',
          });
          await expect(listing).rejects.toThrow(
            /table "workflow" has no index "GSI3"/
          );
          await expect(listing).rejects.toThrow(/bootstrap:local/);
          await expect(listing).rejects.toThrow(/UpdateTable/);
        } finally {
          await world.close();
          ddb.restore();
          destroyClients(clients);
        }
      }
    );

    it('does not start the worker, and the next start() recovers', async () => {
      const clients = createClients(options);
      const ddb = mockClient(clients.documents);
      const sqs = mockClient(clients.sqs);
      ddb
        .on(QueryCommand)
        .rejectsOnce(
          missingIndex(
            'ProvisionedThroughputExceededException',
            'Throughput exceeds the current capacity of your table or index'
          )
        )
        .resolves({ Items: [activeRun] });
      sqs.on(SendMessageCommand).resolves({});
      sqs.on(ReceiveMessageCommand).callsFake(async () => {
        await new Promise((resolve) => setTimeout(resolve, 5));
        return {};
      });
      const world = createWorld({
        ...options,
        workflowQueueUrl: `${options.workflowQueueUrl}-failed-recovery`,
        clients,
        pollWaitSeconds: 0,
      });
      try {
        await expect(world.start()).rejects.toThrow(/Throughput exceeds/);
        await new Promise((resolve) => setTimeout(resolve, 30));
        expect(sqs.commandCalls(ReceiveMessageCommand)).toHaveLength(0);
        expect(sqs.commandCalls(SendMessageCommand)).toHaveLength(0);

        await world.start();
        expect(sqs.commandCalls(SendMessageCommand)).toHaveLength(1);
        await vi.waitFor(() =>
          expect(
            sqs.commandCalls(ReceiveMessageCommand).length
          ).toBeGreaterThan(0)
        );
        // Recovered once: a later start() does nothing more.
        await world.start();
        expect(sqs.commandCalls(SendMessageCommand)).toHaveLength(1);
      } finally {
        await world.close();
        ddb.restore();
        sqs.restore();
        destroyClients(clients);
      }
    });
  });

  it('closes once', async () => {
    const world = createWorld(options);
    const first = world.close();
    expect(world.close()).toBe(first);
    await first;
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
