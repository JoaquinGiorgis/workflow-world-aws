import { slotToEventId } from '@workflow/world';
import { describe, expect, it } from 'vitest';
import {
  admissionKey,
  attrSetKey,
  chunkKey,
  chunkSeqFromSk,
  chunkSk,
  eventIdFromSk,
  eventKey,
  eventSk,
  hookIdKey,
  hookIndexKeys,
  hookKey,
  hookTokenKey,
  idempotencyKey,
  MAX_SORT_NUMBER,
  padSortNumber,
  resumeKey,
  runIndexKeys,
  runMetaKey,
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
  it('lives in the run partition, keyed by the event id', () => {
    expect(eventKey(runId, slotToEventId(1))).toEqual({
      PK: `RUN#${runId}`,
      SK: 'EVT#evnt_00000000000000000000000001',
    });
  });

  it('rejects an empty event id', () => {
    expect(() => eventSk('')).toThrow(TypeError);
  });

  it('sorts slot ids lexicographically in numeric order', () => {
    const slots = [1, 2, 9, 10, 11, 99, 100, 1_000, 12_345, 1_000_000_000];
    const keys = slots.map((slot) => eventSk(slotToEventId(slot)));
    const shuffled = [...keys].reverse();
    expect(shuffled.sort()).toEqual(keys);
  });

  it('sorts legacy wevt_ ids after slot ids, as world-postgres orders them', () => {
    expect(
      eventSk(slotToEventId(1)) < eventSk('wevt_01JD0000000000000000000000')
    ).toBe(true);
  });

  it('round-trips the event id', () => {
    const eventId = slotToEventId(123_456_789);
    expect(eventIdFromSk(eventSk(eventId))).toBe(eventId);
  });

  it.each(['EVT#', 'STEP#evnt_1'])('refuses to parse %s', (sk) => {
    expect(() => eventIdFromSk(sk)).toThrow(TypeError);
  });
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
    expect(snapshotKey(runId).SK).toBe('SNAPSHOT');
    expect(resumeKey(runId, 'res_1').SK).toBe('RESUME#res_1');
    expect(attrSetKey(runId, 'attr_1').SK).toBe('ATTR#attr_1');
    expect(admissionKey(runId).SK).toBe('ADMISSION');
  });

  it('builds the global token and idempotency keys', () => {
    expect(hookTokenKey('tok')).toEqual({ PK: 'HOOKTOKEN#tok', SK: 'TOKEN' });
    expect(hookIdKey('hook_1')).toEqual({ PK: 'HOOKID#hook_1', SK: 'HOOK' });
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
  it('indexes every run by id, runs per workflow by id, and a new run as active', () => {
    expect(runIndexKeys('wrun_A', 'wf//add')).toEqual({
      GSI1PK: 'RUNS',
      GSI1SK: 'wrun_A',
      GSI2PK: 'WF#wf//add',
      GSI2SK: 'wrun_A',
      GSI3PK: 'ACTIVE',
      GSI3SK: 'wrun_A',
    });
  });

  it('indexes every hook by id', () => {
    expect(hookIndexKeys('hook_1')).toEqual({
      GSI1PK: 'HOOKS',
      GSI1SK: 'hook_1',
    });
  });

  it('rejects an empty workflow name', () => {
    expect(() => runIndexKeys('wrun_A', '')).toThrow(TypeError);
  });
});
