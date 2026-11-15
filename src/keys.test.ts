import { describe, expect, it } from 'vitest';
import {
  chunkKey,
  chunkSeqFromSk,
  chunkSk,
  eventKey,
  eventSk,
  eventSlotFromSk,
  hookKey,
  hookTokenKey,
  idempotencyKey,
  MAX_SORT_NUMBER,
  padSortNumber,
  runMetaKey,
  runsByDeploymentGsi1,
  runsByWorkflowGsi2,
  snapshotKey,
  stepKey,
  streamClosedKey,
  waitKey,
} from './keys.js';

const runId = 'wrun_01JD000000000000000000000';

describe('padSortNumber', () => {
  it('pads to ten digits', () => {
    expect(padSortNumber(0)).toBe('0000000000');
    expect(padSortNumber(42)).toBe('0000000042');
    expect(padSortNumber(MAX_SORT_NUMBER)).toBe('9999999999');
  });

  it.each([-1, 1.5, Number.NaN, MAX_SORT_NUMBER + 1])('rejects %d', (n) => {
    expect(() => padSortNumber(n)).toThrow(RangeError);
  });
});

describe('event keys', () => {
  it('lives in the run partition', () => {
    expect(eventKey(runId, 1)).toEqual({
      PK: `RUN#${runId}`,
      SK: 'EVT#0000000001',
    });
  });

  it('rejects slot 0, since slots are 1-based', () => {
    expect(() => eventSk(0)).toThrow(RangeError);
  });

  it('sorts lexicographically in numeric order', () => {
    const slots = [1, 2, 9, 10, 11, 99, 100, 1_000, 12_345, 1_000_000_000];
    const keys = slots.map(eventSk);
    const shuffled = [...keys].reverse();
    expect(shuffled.sort()).toEqual(keys);
  });

  it('round-trips the slot', () => {
    for (const slot of [1, 7, 10, 123_456_789, MAX_SORT_NUMBER]) {
      expect(eventSlotFromSk(eventSk(slot))).toBe(slot);
    }
  });

  it.each(['EVT#1', 'STEP#0000000001', 'EVT#00000000x1'])(
    'refuses to parse %s',
    (sk) => {
      expect(() => eventSlotFromSk(sk)).toThrow(TypeError);
    }
  );
});

describe('chunk keys', () => {
  it('starts at sequence 0 and sorts in numeric order', () => {
    expect(chunkSk(0)).toBe('CHUNK#0000000000');
    const keys = [0, 1, 2, 10, 100, 2_000].map(chunkSk);
    expect([...keys].reverse().sort()).toEqual(keys);
    expect(chunkSeqFromSk(chunkSk(2_000))).toBe(2_000);
  });

  it('uses one partition per run and stream name', () => {
    expect(chunkKey(runId, 'default', 3)).toEqual({
      PK: `STREAM#${runId}#default`,
      SK: 'CHUNK#0000000003',
    });
    expect(streamClosedKey(runId, 'default')).toEqual({
      PK: `STREAM#${runId}#default`,
      SK: 'CLOSED',
    });
  });

  it('sorts the end marker after every chunk', () => {
    expect(chunkSk(MAX_SORT_NUMBER) < streamClosedKey(runId, 'x').SK).toBe(
      true
    );
  });
});

describe('entity keys', () => {
  it('builds the run partition keys', () => {
    expect(runMetaKey(runId)).toEqual({ PK: `RUN#${runId}`, SK: 'META' });
    expect(stepKey(runId, 'step_1').SK).toBe('STEP#step_1');
    expect(hookKey(runId, 'hook_1').SK).toBe('HOOK#hook_1');
    expect(waitKey(runId, 'wait_1').SK).toBe('WAIT#wait_1');
    expect(snapshotKey(runId, 'latest').SK).toBe('SNAP#latest');
  });

  it('builds the global token and idempotency keys', () => {
    expect(hookTokenKey('tok')).toEqual({ PK: 'HOOKTOKEN#tok', SK: 'TOKEN' });
    expect(idempotencyKey('k-1')).toEqual({ PK: 'IDEMP#k-1', SK: 'Q' });
  });

  it('rejects empty components and "#" in run ids', () => {
    expect(() => runMetaKey('')).toThrow(TypeError);
    expect(() => runMetaKey('a#b')).toThrow(TypeError);
    expect(() => stepKey(runId, '')).toThrow(TypeError);
    expect(() => hookTokenKey('')).toThrow(TypeError);
  });
});

describe('GSI keys', () => {
  it('orders runs by creation time', () => {
    const earlier = runsByDeploymentGsi1(
      'dpl_1',
      new Date('2026-01-02T03:04:05.006Z'),
      'wrun_B'
    );
    const later = runsByDeploymentGsi1(
      'dpl_1',
      new Date('2026-11-15T00:00:00.000Z'),
      'wrun_A'
    );
    expect(earlier).toEqual({
      GSI1PK: 'RUNS#dpl_1',
      GSI1SK: '2026-01-02T03:04:05.006Z#wrun_B',
    });
    expect(earlier.GSI1SK < later.GSI1SK).toBe(true);
  });

  it('indexes runs by workflow name', () => {
    expect(runsByWorkflowGsi2('wf//add', new Date(0), 'wrun_A')).toEqual({
      GSI2PK: 'WF#wf//add',
      GSI2SK: '1970-01-01T00:00:00.000Z#wrun_A',
    });
  });

  it('rejects an invalid date', () => {
    expect(() => runsByWorkflowGsi2('wf', new Date('nope'), 'wrun_A')).toThrow(
      RangeError
    );
  });
});
