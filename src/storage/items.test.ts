import { describe, expect, it } from 'vitest';
import {
  eventFromItem,
  filterRunData,
  hookFromItem,
  runFromItem,
  stepFromItem,
  waitFromItem,
} from './items.js';
import { fromStorable, sizeOf, toStorable } from './values.js';

const createdAt = '2026-11-15T20:00:00.000Z';

describe('item mapping', () => {
  it('maps a run item to a run, dropping keys and counters', () => {
    const run = runFromItem({
      PK: 'RUN#wrun_A',
      SK: 'META',
      type: 'run',
      GSI1PK: 'RUNS',
      GSI1SK: 'wrun_A',
      GSI2PK: 'WF#wf',
      GSI2SK: 'wrun_A',
      seq: 4,
      seqInBand: 2,
      rev: 7,
      runId: 'wrun_A',
      status: 'completed',
      deploymentId: 'dpl_1',
      workflowName: 'wf',
      specVersion: 8,
      input: Buffer.from([1, 2]),
      output: new Uint8Array([3]),
      attributes: { tenant: 't1' },
      createdAt,
      updatedAt: createdAt,
      completedAt: createdAt,
    });
    expect(run).toEqual({
      runId: 'wrun_A',
      status: 'completed',
      deploymentId: 'dpl_1',
      workflowName: 'wf',
      specVersion: 8,
      input: new Uint8Array([1, 2]),
      output: new Uint8Array([3]),
      attributes: { tenant: 't1' },
      createdAt: new Date(createdAt),
      updatedAt: new Date(createdAt),
      completedAt: new Date(createdAt),
    });
    expect(run.input).not.toBeInstanceOf(Buffer);
    expect(run).not.toHaveProperty('seq');
    expect(filterRunData(run, 'none')).toMatchObject({
      input: undefined,
      output: undefined,
    });
  });

  it('defaults a run without attributes to {}', () => {
    expect(
      runFromItem({
        runId: 'wrun_A',
        status: 'pending',
        deploymentId: 'd',
        workflowName: 'w',
        createdAt,
        updatedAt: createdAt,
      }).attributes
    ).toEqual({});
  });

  it('maps step, hook and wait items', () => {
    expect(
      stepFromItem({
        PK: 'RUN#wrun_A',
        SK: 'STEP#step_1',
        type: 'step',
        runId: 'wrun_A',
        stepId: 'step_1',
        stepName: 's',
        status: 'pending',
        attempt: 0,
        input: new Uint8Array(),
        retryAfter: createdAt,
        createdAt,
        updatedAt: createdAt,
      })
    ).toMatchObject({
      stepId: 'step_1',
      input: new Uint8Array(),
      retryAfter: new Date(createdAt),
    });
    expect(
      hookFromItem({
        PK: 'RUN#wrun_A',
        SK: 'HOOK#hook_1',
        GSI1PK: 'HOOKS',
        GSI1SK: 'hook_1',
        runId: 'wrun_A',
        hookId: 'hook_1',
        token: 't',
        ownerId: '',
        projectId: '',
        environment: '',
        tokenRetentionUntil: createdAt,
        createdAt,
      })
    ).toEqual({
      runId: 'wrun_A',
      hookId: 'hook_1',
      token: 't',
      ownerId: '',
      projectId: '',
      environment: '',
      tokenRetentionUntil: new Date(createdAt),
      createdAt: new Date(createdAt),
    });
    expect(
      waitFromItem({
        waitId: 'wrun_A-wait_1',
        runId: 'wrun_A',
        status: 'waiting',
        resumeAt: createdAt,
        createdAt,
        updatedAt: createdAt,
      }).resumeAt
    ).toEqual(new Date(createdAt));
  });

  it('maps an event item, coercing dates inside its data', () => {
    const event = eventFromItem({
      PK: 'RUN#wrun_A',
      SK: 'EVT#evnt_00000000000000000000000002',
      type: 'event',
      runId: 'wrun_A',
      eventId: 'evnt_00000000000000000000000002',
      eventType: 'wait_created',
      correlationId: 'wait_1',
      eventData: { resumeAt: createdAt },
      createdAt,
      resumePayloadDigest: 'abc',
    });
    expect(event).toEqual({
      runId: 'wrun_A',
      eventId: 'evnt_00000000000000000000000002',
      eventType: 'wait_created',
      correlationId: 'wait_1',
      eventData: { resumeAt: new Date(createdAt) },
      createdAt: new Date(createdAt),
    });
  });
});

describe('stored values', () => {
  it('stores dates as ISO strings, keeps binaries, drops undefined', () => {
    expect(
      toStorable({
        at: new Date(createdAt),
        bytes: new Uint8Array([1]),
        nested: [{ at: new Date(0), gone: undefined }],
        gone: undefined,
        none: null,
      })
    ).toEqual({
      at: createdAt,
      bytes: new Uint8Array([1]),
      nested: [{ at: '1970-01-01T00:00:00.000Z' }],
      none: null,
    });
  });

  it('returns binaries as plain Uint8Array views', () => {
    const value = fromStorable({ data: Buffer.from([5, 6]) }) as {
      data: Uint8Array;
    };
    expect(value.data).toEqual(new Uint8Array([5, 6]));
    expect(value.data.constructor).toBe(Uint8Array);
  });

  it('sizes binaries by their bytes and strings by their UTF-8 bytes', () => {
    expect(sizeOf(new Uint8Array(1000))).toBe(1000);
    expect(sizeOf('ñ')).toBe(2);
    expect(sizeOf({ a: new Uint8Array(10) })).toBeGreaterThan(10);
  });
});
