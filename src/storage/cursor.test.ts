import { describe, expect, it } from 'vitest';
import { cursorOf, decodeCursor, encodeCursor } from './cursor.js';

describe('pagination cursors', () => {
  it('round-trips a LastEvaluatedKey', () => {
    const key = {
      PK: 'RUN#wrun_01JD0000000000000000000000',
      SK: 'META',
      GSI1PK: 'RUNS',
      GSI1SK: 'wrun_01JD0000000000000000000000',
    };
    const cursor = encodeCursor(key);
    expect(cursor).toMatch(/^[A-Za-z0-9_-]+$/);
    expect(decodeCursor(cursor)).toEqual(key);
  });

  it('keeps keys with any characters, such as a step id with "#"', () => {
    const key = { PK: 'RUN#wrun_A', SK: 'STEP#a#b/ü' };
    expect(decodeCursor(encodeCursor(key))).toEqual(key);
  });

  it('builds the cursor of an item from its key and index attributes', () => {
    const item = {
      PK: 'RUN#wrun_A',
      SK: 'META',
      GSI2PK: 'WF#wf',
      GSI2SK: 'wrun_A',
      status: 'pending',
      seq: 3,
    };
    expect(decodeCursor(cursorOf(item, ['GSI2PK', 'GSI2SK']))).toEqual({
      PK: 'RUN#wrun_A',
      SK: 'META',
      GSI2PK: 'WF#wf',
      GSI2SK: 'wrun_A',
    });
    expect(decodeCursor(cursorOf(item))).toEqual({
      PK: 'RUN#wrun_A',
      SK: 'META',
    });
  });

  it.each([
    'not a cursor',
    encodeCursor({ PK: 'RUN#x' } as never),
    Buffer.from('[1,2]').toString('base64url'),
    Buffer.from('{"PK":"a","SK":1}').toString('base64url'),
  ])('refuses %s with a 400', (cursor) => {
    expect(() => decodeCursor(cursor)).toThrowError(
      expect.objectContaining({ name: 'WorkflowWorldError', status: 400 })
    );
  });
});
