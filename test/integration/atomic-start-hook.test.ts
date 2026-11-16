/**
 * `start({ experimental_hook })` admission: the start Hook's token, the run,
 * its Hook and its run_created commit in one transaction, and a refusal is
 * recorded per candidate.
 *
 * Runs `@workflow/world`'s atomic start Hook conformance suite, then the
 * World's own cases, ported from vercel/workflow,
 * packages/world-local/src/storage/atomic-start-hook.test.ts (main at
 * b1f92691bcc3bc18b9df094d45379c6464cf4d65). Copyright Vercel, Inc.
 * Licensed under the Apache License, Version 2.0
 * (http://www.apache.org/licenses/LICENSE-2.0). Modified: a DynamoDB table
 * per suite instead of a temporary data directory, and the `[world-aws]`
 * prefix on the DEBUG lines.
 */
import {
  HookConflictError,
  START_HOOK_ADMISSION_REJECTED,
} from '@workflow/errors';
import {
  type AnyEventRequest,
  SPEC_VERSION_CURRENT,
  startHookId,
  type World,
} from '@workflow/world';
import { atomicStartHookConformance } from '@workflow/world/test-support/atomic-start-hook-conformance.js';
import { monotonicFactory, ulid } from 'ulid';
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import {
  createStorageTestContext,
  type StorageTestContext,
} from './storage-context.js';

const SPEC = SPEC_VERSION_CURRENT;

let context: StorageTestContext;

beforeAll(async () => {
  context = await createStorageTestContext('start-hook');
}, 120_000);

afterAll(async () => {
  await context?.close();
});

atomicStartHookConformance({
  name: 'world-aws',
  storage: () => context.world,
  // Another World over the same table: a second process.
  secondEvents: () => context.secondWorld().events,
  capabilities: () => context.world.capabilities,
  newRunId: () => `wrun_${ulid()}`,
  advanceClock: (ms) => {
    if (ms === 0) {
      vi.useRealTimers();
      return;
    }
    const target = Date.now() + ms;
    vi.useFakeTimers({ toFake: ['Date'], shouldAdvanceTime: true });
    vi.setSystemTime(target);
  },
  // Hook retention is compared against `Date.now()`.
  retentionReadsAdvanceClock: true,
});

describe('atomic start Hook (world-aws)', () => {
  let storage: World;

  beforeEach(async () => {
    await context.truncate();
    storage = context.world;
  });

  const create = (runId: string, data: AnyEventRequest) =>
    (
      storage.events.create as (
        runId: string,
        data: AnyEventRequest
      ) => ReturnType<typeof storage.events.create>
    )(runId, data);

  const creating = (eventType: 'run_created' | 'run_started', token: string) =>
    ({
      eventType,
      specVersion: SPEC,
      eventData: {
        deploymentId: 'dpl_aws',
        workflowName: 'wf',
        input: new Uint8Array([1]),
        startHook: { token },
      },
    }) as AnyEventRequest;

  it('writes the start Hook with its run, so it resolves at once', async () => {
    const runId = `wrun_${ulid()}`;
    const created = await create(runId, creating('run_created', 'order:1'));
    expect(created.run?.startHook).toMatchObject({
      token: 'order:1',
      hookId: startHookId(runId),
    });
    expect(await storage.hooks.getByToken('order:1')).toMatchObject({
      runId,
      hookId: startHookId(runId),
    });
    // Its creation is the run's run_created, at the first position.
    const page = await storage.events.list({ runId });
    expect(page.data.map((event) => event.eventType)).toEqual(['run_created']);
  });

  it('records a refusal, and answers it again once the token is free', async () => {
    const owner = `wrun_${ulid()}`;
    const refused = `wrun_${ulid()}`;
    await create(owner, creating('run_created', 'order:2'));
    const first = await create(
      refused,
      creating('run_created', 'order:2')
    ).catch((e: unknown) => e);
    expect(HookConflictError.is(first)).toBe(true);
    await create(owner, {
      eventType: 'run_cancelled',
      specVersion: SPEC,
    } as AnyEventRequest);
    // The queued copy decides the same way, though the token is free now.
    const again = await create(
      refused,
      creating('run_started', 'order:2')
    ).catch((e: unknown) => e);
    expect(HookConflictError.is(again)).toBe(true);
    expect((again as HookConflictError).conflictingRunId).toBe(owner);
    await expect(storage.runs.get(refused)).rejects.toThrow();
  });

  it('refuses a queued candidate older than the orphan bound with a permanent code', async () => {
    const dayAgo = monotonicFactory()(Date.now() - 25 * 60 * 60 * 1000);
    const error = await create(
      `wrun_${dayAgo}`,
      creating('run_started', 'order:stale')
    ).catch((e: unknown) => e);
    expect(error).toMatchObject({
      code: START_HOOK_ADMISSION_REJECTED,
      reason: 'expired',
    });
  });

  it('refuses a candidate that already exists without the start Hook', async () => {
    const runId = `wrun_${ulid()}`;
    await create(runId, {
      eventType: 'run_created',
      specVersion: SPEC,
      eventData: {
        deploymentId: 'dpl_aws',
        workflowName: 'wf',
        input: new Uint8Array([1]),
      },
    } as AnyEventRequest);
    const error = await create(runId, creating('run_started', 'order:x')).then(
      (): unknown => undefined,
      (e: unknown) => e
    );
    // The run exists, so run_started takes the ordinary path and starts it;
    // a run_created for it with a start Hook is the contract violation.
    expect(error).toBeUndefined();
    const again = await create(runId, creating('run_created', 'order:x')).catch(
      (e: unknown) => e
    );
    expect(again).toMatchObject({
      code: START_HOOK_ADMISSION_REJECTED,
      reason: 'ordinary_run',
    });
  });

  it('logs decisions under DEBUG, naming runs but never the token', async () => {
    vi.stubEnv('DEBUG', 'workflow:*');
    const debug = vi.spyOn(console, 'debug').mockImplementation(() => {});
    try {
      const token = 'order:secret-key';
      const owner = `wrun_${ulid()}`;
      const refused = `wrun_${ulid()}`;
      const next = `wrun_${ulid()}`;
      await create(owner, creating('run_created', token));
      await create(refused, creating('run_created', token)).catch(() => {});
      await create(owner, {
        eventType: 'run_cancelled',
        specVersion: SPEC,
      } as AnyEventRequest);
      await create(next, creating('run_created', token));

      const lines = debug.mock.calls.map((call) => JSON.stringify(call));
      const said = (message: string, detail: Record<string, string>) =>
        expect(debug).toHaveBeenCalledWith(
          `[world-aws] start Hook: ${message}`,
          detail
        );
      said('accepted a candidate', { runId: owner });
      said('refused a candidate', { runId: refused, conflictingRunId: owner });
      said('accepted a candidate', { runId: next });
      expect(lines.some((line) => line.includes(token))).toBe(false);
    } finally {
      debug.mockRestore();
      vi.unstubAllEnvs();
    }
  });
});
