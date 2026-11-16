/**
 * `runs.waitForTerminalStatus`, and a run_started racing the run's end.
 *
 * Ported from vercel/workflow, packages/world-postgres/test/
 * run-status-wait.test.ts (main at b1f92691bcc3bc18b9df094d45379c6464cf4d65).
 * Copyright Vercel, Inc. Licensed under the Apache License, Version 2.0
 * (http://www.apache.org/licenses/LICENSE-2.0). Modified: one DynamoDB table
 * per suite. world-postgres wakes the wait with a run-terminal NOTIFY and
 * re-reads on a 10s backstop; DynamoDB has no notification, so this World's
 * wait re-reads the run every `RUN_STATUS_POLL_INTERVAL_MS` and the
 * NOTIFY-specific cases assert that interval instead. The row-lock race is
 * reproduced by ending the run between run_started's reads and its write.
 */
import { RunExpiredError, WorkflowRunNotFoundError } from '@workflow/errors';
import type {
  RunCreatedEventRequest,
  WorkflowRun,
  World,
} from '@workflow/world';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { RUN_STATUS_POLL_INTERVAL_MS } from '../../src/storage/runs.js';
import {
  appendsEvent,
  createStorageTestContext,
  type StorageTestContext,
} from './storage-context.js';

describe('runs.waitForTerminalStatus (DynamoDB integration)', () => {
  let context: StorageTestContext;
  let runs: World['runs'];
  let events: World['events'];

  beforeAll(async () => {
    context = await createStorageTestContext('wait');
    ({ runs, events } = context.world);
  }, 120_000);

  beforeEach(async () => {
    await context.truncate();
  });

  afterAll(async () => {
    await context?.close();
  });

  async function startRun(): Promise<WorkflowRun> {
    const created = await events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'dpl_test',
        workflowName: 'test-workflow',
        input: new Uint8Array([1]),
      },
    } as RunCreatedEventRequest);
    if (!created.run) throw new Error('Expected run to be created');
    await events.create(created.run.runId, {
      eventType: 'run_started',
    } as never);
    return created.run;
  }

  const finish = (
    runId: string,
    eventType: 'run_completed' | 'run_failed' | 'run_cancelled',
    eventData?: Record<string, unknown>
  ) => events.create(runId, { eventType, eventData } as never);

  const waitForTerminalStatus = () => {
    const wait = runs.waitForTerminalStatus;
    if (!wait) throw new Error('world-aws should implement the long poll');
    return wait;
  };

  it('a run_started racing the run ending does not move it back to running', async () => {
    const created = await events.create(null, {
      eventType: 'run_created',
      eventData: {
        deploymentId: 'dpl_test',
        workflowName: 'test-workflow',
        input: new Uint8Array([1]),
      },
    } as RunCreatedEventRequest);
    const runId = created.run!.runId;

    // Another writer ends the run after run_started has validated it as
    // `pending` and before its write commits.
    const ender = context.secondWorld();
    context.beforeNextTransaction(
      (input) => appendsEvent(input, (e) => e.eventType === 'run_started'),
      async () => {
        await ender.events.create(runId, {
          eventType: 'run_cancelled',
        } as never);
      }
    );
    const outcome = await events
      .create(runId, { eventType: 'run_started' } as never)
      .then(
        () => 'started',
        (error: unknown) => error
      );
    expect(RunExpiredError.is(outcome)).toBe(true);

    const run = await runs.get(runId);
    expect(run.status).toBe('cancelled');
    const { data } = await events.list({ runId, resolveData: 'none' } as never);
    expect(data.map((event) => event.eventType)).not.toContain('run_started');
  });

  it('returns an already-terminal run immediately', async () => {
    const run = await startRun();
    await finish(run.runId, 'run_completed', { output: new Uint8Array([2]) });

    const startedAt = Date.now();
    const waited = await waitForTerminalStatus()(run.runId, {
      timeoutMs: 30_000,
    });

    expect(waited.status).toBe('completed');
    expect(Date.now() - startedAt).toBeLessThan(1_000);
  });

  it('notices the run ending within one poll interval', async () => {
    // world-postgres: "is woken by the run-terminal NOTIFY, not by its
    // backstop". Here the poll is the only wake-up, and it must be prompt.
    expect(RUN_STATUS_POLL_INTERVAL_MS).toBeLessThanOrEqual(1_000);
    const run = await startRun();

    const pending = waitForTerminalStatus()(run.runId, { timeoutMs: 30_000 });
    await new Promise((resolve) => setTimeout(resolve, 250));
    const startedAt = Date.now();
    await finish(run.runId, 'run_completed', { output: new Uint8Array([2]) });

    const waited = await pending;

    expect(waited.status).toBe('completed');
    expect(Date.now() - startedAt).toBeLessThan(2_000);
  });

  it('wakes on a failed run too', async () => {
    const run = await startRun();

    const pending = waitForTerminalStatus()(run.runId, { timeoutMs: 30_000 });
    await new Promise((resolve) => setTimeout(resolve, 250));
    await finish(run.runId, 'run_failed', {
      error: new Uint8Array([3]),
      errorCode: 'USER_ERROR',
    });

    const waited = await pending;
    expect(waited.status).toBe('failed');
  });

  it('wakes on a cancelled run', async () => {
    const run = await startRun();

    const pending = waitForTerminalStatus()(run.runId, { timeoutMs: 30_000 });
    await new Promise((resolve) => setTimeout(resolve, 250));
    await finish(run.runId, 'run_cancelled');

    const waited = await pending;
    expect(waited.status).toBe('cancelled');
  });

  it('returns the latest non-terminal snapshot when the budget expires', async () => {
    const run = await startRun();

    const startedAt = Date.now();
    const waited = await waitForTerminalStatus()(run.runId, { timeoutMs: 200 });

    expect(waited.status).toBe('running');
    expect(Date.now() - startedAt).toBeGreaterThanOrEqual(190);
  });

  // Skipped: there is no listener to fall back from. The poll this test
  // falls back to is the only mechanism here, covered by every case above.
  it.skip('falls back to the backstop re-read without a listener', () => {});

  it('stops early when the caller aborts', async () => {
    const run = await startRun();
    const controller = new AbortController();

    const pending = waitForTerminalStatus()(run.runId, {
      timeoutMs: 30_000,
      signal: controller.signal,
    });
    setTimeout(() => controller.abort(), 100);

    expect((await pending).status).toBe('running');
  });

  it('fails like get for an unknown run', async () => {
    await expect(
      waitForTerminalStatus()('wrun_01JB0000000000000000000000', {
        timeoutMs: 30_000,
      })
    ).rejects.toBeInstanceOf(WorkflowRunNotFoundError);
  });
});
