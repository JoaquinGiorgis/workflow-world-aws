/**
 * What the ported suites do not cover because it is specific to DynamoDB:
 * how values are stored, the 100-item transaction limit, the item size
 * limit, and the opaque cursors of the index queries.
 */
import { GetItemCommand } from '@aws-sdk/client-dynamodb';
import { slotToEventId, type World } from '@workflow/world';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  eventKey,
  hookIdKey,
  hookKey,
  hookTokenKey,
  runMetaKey,
} from '../../src/keys.js';
import {
  appendsEvent,
  createStorageTestContext,
  type StorageTestContext,
} from './storage-context.js';

let context: StorageTestContext;
let world: World;

beforeAll(async () => {
  context = await createStorageTestContext('dynamodb');
  world = context.world;
}, 120_000);

beforeEach(async () => {
  await context.truncate();
});

afterAll(async () => {
  await context?.close();
});

async function rawItem(key: { PK: string; SK: string }) {
  const result = await context.clients.dynamodb.send(
    new GetItemCommand({
      TableName: context.tableName,
      Key: { PK: { S: key.PK }, SK: { S: key.SK } },
      ConsistentRead: true,
    })
  );
  return result.Item;
}

async function runningRun(workflowName = 'wf'): Promise<string> {
  const created = await world.events.create(null, {
    eventType: 'run_created',
    eventData: {
      deploymentId: 'dpl',
      workflowName,
      input: new Uint8Array([1, 2, 3]),
    },
  });
  await world.events.create(created.run.runId, { eventType: 'run_started' });
  return created.run.runId;
}

describe('DynamoDB storage', () => {
  it('stores payloads as Binary attributes and dates as ISO strings', async () => {
    const runId = await runningRun();
    await world.events.create(runId, {
      eventType: 'step_created',
      correlationId: 'step_1',
      eventData: { stepName: 's', input: new Uint8Array([9, 8]) },
    });

    const run = await rawItem(runMetaKey(runId));
    expect(run?.input).toEqual({ B: new Uint8Array([1, 2, 3]) });
    expect(run?.createdAt?.S).toMatch(/^\d{4}-\d\d-\d\dT.*Z$/);
    expect(run?.seq).toEqual({ N: '3' });

    const event = await rawItem(eventKey(runId, slotToEventId(3)));
    expect(event?.eventData?.M?.input).toEqual({ B: new Uint8Array([9, 8]) });

    const step = await world.steps.get(runId, 'step_1');
    expect(step.input).toEqual(new Uint8Array([9, 8]));
    expect(step.input?.constructor).toBe(Uint8Array);
  });

  it('round-trips an empty binary', async () => {
    const runId = await runningRun();
    const result = await world.events.create(runId, {
      eventType: 'step_created',
      correlationId: 'step_empty',
      eventData: { stepName: 's', input: new Uint8Array() },
    });
    expect(result.step?.input).toEqual(new Uint8Array());
    expect((await world.steps.get(runId, 'step_empty')).input).toEqual(
      new Uint8Array()
    );
  });

  it('ends a run whose cleanup does not fit one transaction', async () => {
    // 40 hooks are 120 items to delete (hook, id pointer, token claim) plus
    // the run item and the terminal event: over DynamoDB's 100 items per
    // transaction. What does not fit is deleted right after the commit.
    const runId = await runningRun();
    const hookIds = Array.from({ length: 40 }, (_, i) => `hook_${i}`);
    for (const hookId of hookIds) {
      await world.events.create(runId, {
        eventType: 'hook_created',
        correlationId: hookId,
        eventData: { token: `token_${hookId}` },
      });
    }

    const result = await world.events.create(runId, {
      eventType: 'run_completed',
      eventData: { output: new Uint8Array([1]) },
    });
    expect(result.run?.status).toBe('completed');

    for (const hookId of hookIds) {
      expect(await rawItem(hookKey(runId, hookId))).toBeUndefined();
      expect(await rawItem(hookIdKey(hookId))).toBeUndefined();
      expect(await rawItem(hookTokenKey(`token_${hookId}`))).toBeUndefined();
    }
    expect((await world.hooks.list({ runId })).data).toEqual([]);
    // Every token is free again.
    const next = await runningRun();
    const reused = await world.events.create(next, {
      eventType: 'hook_created',
      correlationId: 'hook_next',
      eventData: { token: 'token_hook_39' },
    });
    expect(reused.event?.eventType).toBe('hook_created');
  });

  it('refuses a payload over the item limit with a typed error, writing nothing', async () => {
    const runId = await runningRun();
    await expect(
      world.events.create(runId, {
        eventType: 'step_created',
        correlationId: 'step_big',
        eventData: { stepName: 's', input: new Uint8Array(360 * 1024) },
      })
    ).rejects.toMatchObject({
      name: 'PayloadTooLargeError',
      status: 413,
      code: 'PAYLOAD_TOO_LARGE',
    });
    const log = await world.events.list({ runId });
    expect(log.data.map((event) => event.eventType)).toEqual([
      'run_created',
      'run_started',
    ]);
    await expect(world.steps.get(runId, 'step_big')).rejects.toThrow(
      'Step not found'
    );
  });

  it('pages runs of one workflow with opaque cursors, through GSI2', async () => {
    const wanted: string[] = [];
    for (let i = 0; i < 5; i++) {
      wanted.push(await runningRun('wanted'));
      await runningRun('other');
    }
    const seen: string[] = [];
    let cursor: string | undefined;
    for (let page = 0; page < 10; page++) {
      const result = await world.runs.list({
        workflowName: 'wanted',
        pagination: { limit: 2, cursor, sortOrder: 'asc' },
      });
      seen.push(...result.data.map((run) => run.runId));
      if (!result.hasMore) break;
      expect(result.cursor).not.toContain('wrun_');
      cursor = result.cursor ?? undefined;
    }
    expect(seen).toEqual(wanted);
  });

  it('keeps exactly the runs that have not ended in the active-runs index', async () => {
    const pending = await world.events.create(null, {
      eventType: 'run_created',
      eventData: { deploymentId: 'dpl', workflowName: 'wf', input: [] },
    });
    const running = await runningRun();
    const completed = await runningRun();
    await world.events.create(completed, {
      eventType: 'run_completed',
      eventData: { output: new Uint8Array([1]) },
    });
    const cancelled = await runningRun();
    await world.events.create(cancelled, { eventType: 'run_cancelled' });

    const active = await world.runs.list({
      status: ['pending', 'running'],
      resolveData: 'none',
      pagination: { sortOrder: 'asc' },
    });
    expect(active.data.map((run) => [run.runId, run.status])).toEqual([
      [pending.run.runId, 'pending'],
      [running, 'running'],
    ]);
    const onlyRunning = await world.runs.list({ status: 'running' });
    expect(onlyRunning.data.map((run) => run.runId)).toEqual([running]);

    expect((await rawItem(runMetaKey(running)))?.GSI3PK).toEqual({
      S: 'ACTIVE',
    });
    expect((await rawItem(runMetaKey(completed)))?.GSI3PK).toBeUndefined();
    expect((await rawItem(runMetaKey(cancelled)))?.GSI3SK).toBeUndefined();
    // The terminal ones are still listed by status from every run.
    const ended = await world.runs.list({ status: ['completed', 'cancelled'] });
    expect(ended.data.map((run) => run.runId).sort()).toEqual(
      [completed, cancelled].sort()
    );
  });

  it('answers a disposal that lost to a concurrent one with a conflict, as world-postgres', async () => {
    const runId = await runningRun();
    await world.events.create(runId, {
      eventType: 'hook_created',
      correlationId: 'hook_race',
      eventData: { token: 'token_race' },
    });
    // The first disposer has read the live hook; the second commits in the
    // gap before the first one's transaction.
    context.beforeNextTransaction(
      (input) =>
        appendsEvent(input, (event) => event.eventType === 'hook_disposed'),
      async () => {
        await context.secondWorld().events.create(runId, {
          eventType: 'hook_disposed',
          correlationId: 'hook_race',
        });
      }
    );
    await expect(
      world.events.create(runId, {
        eventType: 'hook_disposed',
        correlationId: 'hook_race',
      })
    ).rejects.toMatchObject({
      name: 'EntityConflictError',
      message: 'Hook "hook_race" already disposed',
    });
    const log = await world.events.list({ runId });
    expect(
      log.data.filter((event) => event.eventType === 'hook_disposed')
    ).toHaveLength(1);
    // Disposing a hook that is simply gone is still a missing hook.
    await expect(
      world.events.create(runId, {
        eventType: 'hook_disposed',
        correlationId: 'hook_race',
      })
    ).rejects.toMatchObject({ name: 'HookNotFoundError' });
  });

  it('refuses a cursor it did not mint', async () => {
    await expect(
      world.runs.list({ pagination: { cursor: 'not-a-cursor' } })
    ).rejects.toMatchObject({ name: 'WorkflowWorldError', status: 400 });
  });
});
