/**
 * The in-band writer fence for single-orchestrator runs: an in-band write is
 * accepted only at the run's current in-band count, a refusal allocates
 * nothing, out-of-band writes leave the count alone, and of concurrent
 * in-band writers holding the same count exactly one wins. `list` reports
 * the count, read before listing, as `snapshot`.
 *
 * Runs `@workflow/world`'s in-band fence conformance suite, then the cases
 * ported from vercel/workflow, packages/world-postgres/test/
 * in-band-fence.test.ts (main at b1f92691bcc3bc18b9df094d45379c6464cf4d65).
 * Copyright Vercel, Inc. Licensed under the Apache License, Version 2.0
 * (http://www.apache.org/licenses/LICENSE-2.0). Modified: one DynamoDB table
 * per suite; SQL reads became item reads; where the Postgres suite parks
 * writers on a row lock (an open transaction holding the next slot, or the
 * run's slots row), the writers are held between their reads and their
 * transaction instead (`storage-context.ts`), which is the window a lock
 * closes in Postgres and a conditional write closes here. The NOTIFY case is
 * skipped (see below).
 */
import { QueryCommand } from '@aws-sdk/lib-dynamodb';
import { EntityConflictError, InBandSupersededError } from '@workflow/errors';
import {
  type AnyEventRequest,
  type CreateEventParams,
  type EventResult,
  eventIdToSlot,
  IN_BAND_SEQ_AT_RUN_CREATION,
  SPEC_VERSION_SINGLE_ORCHESTRATOR,
  type World,
} from '@workflow/world';
import { inBandFenceConformance } from '@workflow/world/test-support/in-band-fence-conformance.js';
import { ulid } from 'ulid';
import { afterAll, beforeAll, describe, expect, test } from 'vitest';
import { runMetaKey, runPk } from '../../src/keys.js';
import {
  appendsEvent,
  createStorageTestContext,
  type StorageTestContext,
} from './storage-context.js';

let context: StorageTestContext;

beforeAll(async () => {
  context = await createStorageTestContext('fence');
}, 120_000);

afterAll(async () => {
  await context?.close();
});

// The fence behavior every World shares.
inBandFenceConformance({
  name: 'world-aws',
  events: () => context.world.events,
  secondEvents: () => context.secondWorld().events,
  capabilities: () => context.world.capabilities,
  newRunId: () => `wrun_${ulid()}`,
  atRunCreation: IN_BAND_SEQ_AT_RUN_CREATION,
  concurrentWriters: 12,
});

describe('in-band fence (world-aws)', () => {
  const SPEC = SPEC_VERSION_SINGLE_ORCHESTRATOR;
  /** `create` without its overloads, so a test can pass any request. */
  type CreateAny = (
    runId: string | null,
    data: AnyEventRequest,
    params?: CreateEventParams
  ) => Promise<EventResult>;
  let events: Omit<World['events'], 'create'> & { create: CreateAny };

  beforeAll(() => {
    events = context.world.events as typeof events;
  });

  async function createRun(): Promise<string> {
    const runId = `wrun_${ulid()}`;
    await events.create(runId, {
      eventType: 'run_created',
      specVersion: SPEC,
      eventData: {
        deploymentId: 'dpl_fence',
        workflowName: 'fence',
        input: new Uint8Array([1]),
      },
    } as AnyEventRequest);
    return runId;
  }

  const runStarted = { eventType: 'run_started', specVersion: SPEC } as const;

  const waitCreated = (correlationId: string) =>
    ({
      eventType: 'wait_created',
      correlationId,
      specVersion: SPEC,
      eventData: { resumeAt: new Date(Date.now() + 60_000) },
    }) as AnyEventRequest;

  const attrSet = (value: string) =>
    ({
      eventType: 'attr_set',
      specVersion: SPEC,
      eventData: {
        changes: [{ key: 'k', value }],
        writer: { type: 'workflow' },
      },
    }) as AnyEventRequest;

  async function load(runId: string) {
    const page = await events.list({ runId });
    return {
      snapshot: page.snapshot,
      slots: page.data.map((event) => eventIdToSlot(event.eventId)),
    };
  }

  /** Items of the run's partition whose sort key starts with `prefix`. */
  async function itemsOf(runId: string, prefix: string) {
    const page = await context.clients.documents.send(
      new QueryCommand({
        TableName: context.tableName,
        KeyConditionExpression: 'PK = :pk AND begins_with(SK, :prefix)',
        ExpressionAttributeValues: { ':pk': runPk(runId), ':prefix': prefix },
        ConsistentRead: true,
      })
    );
    return page.Items ?? [];
  }

  test('a stale writer touches no entity row: its refused wait_created leaves the wait free', async () => {
    const runId = await createRun();
    await events.create(runId, runStarted as AnyEventRequest, {
      inBand: true,
      expectedSeqInBand: IN_BAND_SEQ_AT_RUN_CREATION,
    });
    await expect(
      events.create(runId, waitCreated('wait_x'), {
        inBand: true,
        expectedSeqInBand: IN_BAND_SEQ_AT_RUN_CREATION,
      })
    ).rejects.toSatisfy((err: unknown) => InBandSupersededError.is(err));
    // The current writer can still create that wait.
    await expect(
      events.create(runId, waitCreated('wait_x'), {
        inBand: true,
        expectedSeqInBand: IN_BAND_SEQ_AT_RUN_CREATION + 1,
      })
    ).resolves.toBeDefined();
  });

  test('a stale run_completed leaves the run running', async () => {
    const runId = await createRun();
    await events.create(runId, runStarted as AnyEventRequest, {
      inBand: true,
      expectedSeqInBand: IN_BAND_SEQ_AT_RUN_CREATION,
    });
    await expect(
      events.create(
        runId,
        {
          eventType: 'run_completed',
          specVersion: SPEC,
          eventData: { output: new Uint8Array([1]) },
        } as AnyEventRequest,
        { inBand: true, expectedSeqInBand: IN_BAND_SEQ_AT_RUN_CREATION }
      )
    ).rejects.toSatisfy((err: unknown) => InBandSupersededError.is(err));
    const [meta] = (await itemsOf(runId, 'META')).filter(
      (item) => item.SK === runMetaKey(runId).SK
    );
    expect(meta?.status).toBe('running');
  });

  test('an in-band writer racing out-of-band writers for slots keeps its count exact', async () => {
    // Out-of-band writes compete for the same next slot; a fenced write that
    // loses a slot race must leave the count where it was, so the writer's
    // next in-band write at count + 1 is still accepted.
    for (let round = 0; round < 5; round++) {
      const runId = await createRun();
      const { snapshot } = await load(runId);
      const outOfBand = Array.from({ length: 10 }, (_, i) =>
        events.create(runId, attrSet(`v${i}`), { inBand: false })
      );
      const inBand = events.create(runId, waitCreated('wait_1'), {
        inBand: true,
        expectedSeqInBand: snapshot?.seqInBand,
      });
      await Promise.all([...outOfBand, inBand]);
      const after = await load(runId);
      expect(after.snapshot?.seqInBand).toBe((snapshot?.seqInBand ?? 0) + 1);
      // Dense: every slot from 1 to seq is an event.
      expect(after.slots).toEqual(
        Array.from({ length: after.snapshot?.seq ?? 0 }, (_, i) => i + 1)
      );
      await expect(
        events.create(runId, waitCreated('wait_2'), {
          inBand: true,
          expectedSeqInBand: (snapshot?.seqInBand ?? 0) + 1,
        })
      ).resolves.toBeDefined();
    }
  });

  test('a fenced insert that loses its slot to another writer puts its count back and retries', async () => {
    const runId = await createRun();
    const { snapshot } = await load(runId);
    const nextSlot = (snapshot?.seq ?? 0) + 1;
    // An out-of-band writer takes the next slot after the fenced write has
    // decided on it and before its write commits (the Postgres suite holds
    // that slot in an open transaction).
    const other = context.secondWorld();
    context.beforeNextTransaction(
      (input) =>
        appendsEvent(
          input,
          (e) => e.eventType === 'wait_created' && e.correlationId === 'wait_1'
        ),
      async () => {
        await (other.events.create as CreateAny)(runId, attrSet('other'), {
          inBand: false,
        });
      }
    );
    const created = await events.create(runId, waitCreated('wait_1'), {
      inBand: true,
      expectedSeqInBand: snapshot?.seqInBand,
    });
    expect(eventIdToSlot(created.event?.eventId ?? '')).toBe(nextSlot + 1);
    const after = await load(runId);
    expect(after.snapshot?.seqInBand).toBe((snapshot?.seqInBand ?? 0) + 1);
    expect(after.slots).toEqual(
      Array.from({ length: nextSlot + 1 }, (_, i) => i + 1)
    );
  });

  test('a refusal by the run or step checks leaves the count alone', async () => {
    const runId = await createRun();
    const stepCreated = {
      eventType: 'step_created',
      correlationId: `step_${ulid()}`,
      specVersion: SPEC,
      eventData: { stepName: 'add', input: new Uint8Array([1]) },
    } as AnyEventRequest;
    await events.create(runId, stepCreated, {
      inBand: true,
      expectedSeqInBand: IN_BAND_SEQ_AT_RUN_CREATION,
    });
    const before = await load(runId);
    const error = await events
      .create(runId, stepCreated, {
        inBand: true,
        expectedSeqInBand: before.snapshot?.seqInBand,
      })
      .catch((err: unknown) => err);
    expect(EntityConflictError.is(error)).toBe(true);
    expect(await load(runId)).toEqual(before);
  });

  /**
   * Lets every writer make its reads and then holds each one before its
   * transaction until all of them got there, so they all decide on the same
   * state (the Postgres suite parks them on the run's slots row lock).
   */
  async function raceBehindBarrier<T>(
    writers: Array<() => Promise<T>>
  ): Promise<PromiseSettledResult<T>[]> {
    let arrived = 0;
    let release!: () => void;
    const all = new Promise<void>((resolve) => {
      release = resolve;
    });
    for (let i = 0; i < writers.length; i++) {
      context.beforeNextTransaction(
        () => true,
        async () => {
          arrived++;
          if (arrived === writers.length) release();
          await Promise.race([
            all,
            new Promise((resolve) => setTimeout(resolve, 5_000)),
          ]);
        }
      );
    }
    return Promise.allSettled(writers.map((write) => write()));
  }

  test('of concurrent writers holding the same count, a refused wait_created leaves no wait row behind', async () => {
    const runId = await createRun();
    const { snapshot } = await load(runId);
    const outcomes = await raceBehindBarrier([
      () =>
        events.create(runId, waitCreated('wait_a'), {
          inBand: true,
          expectedSeqInBand: snapshot?.seqInBand,
        }),
      () =>
        events.create(runId, waitCreated('wait_b'), {
          inBand: true,
          expectedSeqInBand: snapshot?.seqInBand,
        }),
    ]);
    const accepted = outcomes.filter((o) => o.status === 'fulfilled');
    expect(accepted).toHaveLength(1);
    expect(
      outcomes.filter(
        (o) => o.status === 'rejected' && InBandSupersededError.is(o.reason)
      )
    ).toHaveLength(1);
    const winner = (accepted[0] as PromiseFulfilledResult<{ event?: unknown }>)
      .value.event as { correlationId: string };
    const waits = await itemsOf(runId, 'WAIT#');
    expect(waits.map((row) => row.waitId)).toEqual([
      `${runId}-${winner.correlationId}`,
    ]);
  });

  test('of concurrent writers holding the same count, a refused attr_set leaves the run attributes alone', async () => {
    const runId = await createRun();
    const { snapshot } = await load(runId);
    const attrWrite = (key: string) => () =>
      events.create(
        runId,
        {
          eventType: 'attr_set',
          specVersion: SPEC,
          eventData: {
            changes: [{ key, value: 'v' }],
            writer: { type: 'workflow' },
          },
        } as AnyEventRequest,
        { inBand: true, expectedSeqInBand: snapshot?.seqInBand }
      );
    const outcomes = await raceBehindBarrier([
      attrWrite('ka'),
      attrWrite('kb'),
    ]);
    const accepted = outcomes.filter((o) => o.status === 'fulfilled');
    expect(accepted).toHaveLength(1);
    const winner = (
      accepted[0] as PromiseFulfilledResult<{
        event?: { eventData?: { changes?: { key: string }[] } };
      }>
    ).value.event?.eventData?.changes?.[0]?.key;
    const run = await context.world.runs.get(runId);
    expect(Object.keys(run.attributes ?? {})).toEqual([winner]);
  });

  test.each([
    ['wait_created', () => waitCreated('wait_same')],
    [
      'step_created',
      () =>
        ({
          eventType: 'step_created',
          correlationId: `step_${ulid()}`,
          specVersion: SPEC,
          eventData: { stepName: 'add', input: new Uint8Array([1]) },
        }) as AnyEventRequest,
    ],
  ])(
    'two same-count writers of one %s: one commits, the other is superseded, not in conflict',
    async (_type, request) => {
      // Two overlapping orchestrator deliveries replay to the same decision.
      // The stale one must hear 412 and redeliver, never a 409 for the entity
      // the current writer just created.
      const write = request();
      for (const race of [
        (writers: Array<() => Promise<unknown>>) => raceBehindBarrier(writers),
        (writers: Array<() => Promise<unknown>>) =>
          Promise.allSettled(writers.map((writer) => writer())),
      ]) {
        const runId = await createRun();
        const { snapshot } = await load(runId);
        const sameWrite = {
          ...write,
          ...(write.eventType === 'step_created'
            ? { correlationId: `step_${ulid()}` }
            : {}),
        } as AnyEventRequest;
        const writer = () =>
          events.create(runId, sameWrite, {
            inBand: true,
            expectedSeqInBand: snapshot?.seqInBand,
          });
        const outcomes = await race([writer, writer]);
        expect(outcomes.filter((o) => o.status === 'fulfilled')).toHaveLength(
          1
        );
        const [refusal] = outcomes.flatMap((o) =>
          o.status === 'rejected' ? [o.reason] : []
        );
        expect(EntityConflictError.is(refusal)).toBe(false);
        expect(InBandSupersededError.is(refusal)).toBe(true);
        expect((await load(runId)).snapshot?.seqInBand).toBe(
          (snapshot?.seqInBand ?? 0) + 1
        );
      }
    }
  );

  // Skipped: Postgres-specific. It checks the run-terminal NOTIFY that wakes
  // `runs.waitForTerminalStatus`; DynamoDB has no notification channel, and
  // this World's wait is a poll of the run item (run-status-wait.test.ts).
  test.skip('a fenced run_completed announces the terminal run after its commit', () => {});
});
