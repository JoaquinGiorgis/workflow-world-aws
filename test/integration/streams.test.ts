/**
 * Stream suites of world-local and world-postgres, run against the AWS
 * World's streams on the emulator.
 *
 * Ported from vercel/workflow (main at
 * b1f92691bcc3bc18b9df094d45379c6464cf4d65):
 * packages/world-local/src/streamer.test.ts,
 * packages/world-postgres/src/streamer.test.ts,
 * packages/world-postgres/test/stream-offset.test.ts and
 * packages/world-postgres/test/stream-post-eof-duplicates.test.ts.
 * Copyright Vercel, Inc. Licensed under the Apache License, Version 2.0
 * (http://www.apache.org/licenses/LICENSE-2.0). Modified: a streamer on a
 * data directory (world-local) or a Postgres pool (world-postgres) became
 * the World's `streams` on one DynamoDB table, and "another instance on the
 * same data directory" a second World on the same table. world-local's
 * checks on its signal emitter and poll interval became checks that no
 * poll timer is left; world-postgres' checks on its LISTEN emitter, which
 * mocked drizzle and pg, became the same behaviours against the emulator
 * (NOTIFY replays, which this World does not have, became re-reads of the
 * same chunks). Rows a producer appended after the first EOF are written as
 * raw items after `CLOSED`, since this World drops a write to a closed
 * stream. Tests that only apply to a data directory shared across OS
 * processes are left out (the `@workflow/world-testing` suite runs app
 * servers in their own processes). Assertions are unchanged unless a
 * comment says otherwise.
 */
import { randomUUID } from 'node:crypto';
import { HeadObjectCommand } from '@aws-sdk/client-s3';
import { GetCommand, PutCommand } from '@aws-sdk/lib-dynamodb';
import type { World } from '@workflow/world';
import {
  afterAll,
  beforeAll,
  beforeEach,
  describe,
  expect,
  inject,
  it,
  onTestFinished,
  vi,
} from 'vitest';
import { chunkSk, streamPk } from '../../src/keys.js';
import {
  createStorageTestContext,
  type StorageTestContext,
  s3Location,
  TEST_STREAM_POLL_INTERVAL_MS,
} from './storage-context.js';

const aws = inject('aws');

let ctx: StorageTestContext;
let streamer: World;
let TEST_RUN_ID: string;

beforeAll(async () => {
  ctx = await createStorageTestContext('streams');
  streamer = ctx.world;
});

afterAll(async () => {
  await ctx.close();
});

beforeEach(() => {
  // A run of its own per test, as world-local's fresh data directory.
  TEST_RUN_ID = `wrun_${randomUUID().replaceAll('-', '').slice(0, 26)}`;
});

/** A second World on the same table, closed after the test. */
function otherInstance(): World {
  const other = ctx.secondWorld();
  onTestFinished(async () => {
    await other.close?.();
  });
  return other;
}

async function readAll(stream: ReadableStream<Uint8Array>) {
  const chunks: Uint8Array[] = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return chunks;
}

async function readText(stream: ReadableStream<Uint8Array>) {
  return (await readAll(stream)).map((chunk) => Buffer.from(chunk).toString());
}

/** Reads `stream` in the background, collecting chunks as they arrive. */
function collect(stream: ReadableStream<Uint8Array>) {
  const received: string[] = [];
  const done = (async () => {
    for await (const chunk of stream) {
      received.push(Buffer.from(chunk).toString());
    }
  })();
  return { received, done };
}

function deferred<T>() {
  let resolve: (value: T) => void = () => {};
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

async function waitFor(condition: () => boolean, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error('timed out waiting');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

// Long enough for an unordered write or delivery to land first.
const settle = () => new Promise((resolve) => setTimeout(resolve, 50));

/**
 * Tracks the live readers' poll timers (`setTimeout` with the poll
 * interval) created from here on, and whether each was cleared or fired.
 * Stands in for world-local's poll interval tracking: this World's readers
 * sleep between polls with one timer at a time.
 */
function trackPollTimers() {
  const created = new Set<unknown>();
  const settled = new Set<unknown>();
  const original = globalThis.setTimeout;
  const originalClear = globalThis.clearTimeout;
  const set = vi.spyOn(globalThis, 'setTimeout').mockImplementation(((
    callback: (...args: unknown[]) => void,
    ms?: number,
    ...args: unknown[]
  ) => {
    if (ms !== TEST_STREAM_POLL_INTERVAL_MS) {
      return original(callback, ms, ...args);
    }
    const handle = original(
      (...inner: unknown[]) => {
        settled.add(handle);
        callback(...inner);
      },
      ms,
      ...args
    );
    created.add(handle);
    return handle;
  }) as typeof setTimeout);
  const clear = vi.spyOn(globalThis, 'clearTimeout').mockImplementation(((
    handle: Parameters<typeof clearTimeout>[0]
  ) => {
    settled.add(handle);
    originalClear(handle);
  }) as typeof clearTimeout);
  return {
    created: () => created.size,
    open: () => [...created].filter((handle) => !settled.has(handle)).length,
    restore() {
      set.mockRestore();
      clear.mockRestore();
    },
  };
}

describe('streamer', () => {
  describe('write and read', () => {
    it('round-trips string chunks', async () => {
      const streamName = 'string-stream';

      await streamer.streams.write(TEST_RUN_ID, streamName, 'hello ');
      await streamer.streams.write(TEST_RUN_ID, streamName, 'world');
      await streamer.streams.close(TEST_RUN_ID, streamName);

      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, streamName))
      ).toEqual(['hello ', 'world']);
    });

    it('round-trips Buffer chunks', async () => {
      const streamName = 'buffer-stream';

      await streamer.streams.write(
        TEST_RUN_ID,
        streamName,
        Buffer.from('chunk1')
      );
      await streamer.streams.write(
        TEST_RUN_ID,
        streamName,
        Buffer.from('chunk2')
      );
      await streamer.streams.close(TEST_RUN_ID, streamName);

      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, streamName))
      ).toEqual(['chunk1', 'chunk2']);
    });

    it('round-trips binary data byte for byte', async () => {
      const streamName = 'binary-stream';

      await streamer.streams.write(
        TEST_RUN_ID,
        streamName,
        new Uint8Array([0, 1, 2, 255])
      );
      await streamer.streams.write(
        TEST_RUN_ID,
        streamName,
        new Uint8Array([254, 253, 0, 6])
      );
      await streamer.streams.close(TEST_RUN_ID, streamName);

      const chunks = await readAll(
        await streamer.streams.get(TEST_RUN_ID, streamName)
      );
      expect(chunks.map((chunk) => Array.from(chunk))).toEqual([
        [0, 1, 2, 255],
        [254, 253, 0, 6],
      ]);
    });

    it('preserves multi-byte UTF-8 strings', async () => {
      const streamName = 'utf8-stream';
      const text = 'special chars: ñáéíóú ✓ 😀';

      await streamer.streams.write(TEST_RUN_ID, streamName, text);
      await streamer.streams.close(TEST_RUN_ID, streamName);

      const [chunk] = await readAll(
        await streamer.streams.get(TEST_RUN_ID, streamName)
      );
      expect(new TextDecoder().decode(chunk)).toBe(text);
    });

    it('keeps streams independent', async () => {
      await streamer.streams.write(TEST_RUN_ID, 'stream1', 'data1');
      await streamer.streams.write(TEST_RUN_ID, 'stream2', 'data2');
      await streamer.streams.write(TEST_RUN_ID, 'stream1', 'data3');
      await streamer.streams.close(TEST_RUN_ID, 'stream1');
      await streamer.streams.close(TEST_RUN_ID, 'stream2');

      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, 'stream1'))
      ).toEqual(['data1', 'data3']);
      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, 'stream2'))
      ).toEqual(['data2']);
    });

    it('resumes with startIndex after a cancelled read (reproduces vibe platform bug)', async () => {
      const streamName = 'resumption-stream';

      for (let i = 0; i < 4; i++) {
        await streamer.streams.write(TEST_RUN_ID, streamName, `chunk${i}`);
      }

      // First read, interrupted after 2 chunks while the stream is open.
      const reader1 = (
        await streamer.streams.get(TEST_RUN_ID, streamName, 0)
      ).getReader();
      const result1 = await reader1.read();
      const result2 = await reader1.read();
      expect(Buffer.from(result1.value ?? []).toString()).toBe('chunk0');
      expect(Buffer.from(result2.value ?? []).toString()).toBe('chunk1');
      await reader1.cancel();

      await streamer.streams.close(TEST_RUN_ID, streamName);

      // Every delivered chunk must be usable (not a detached ArrayBuffer).
      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, streamName, 2))
      ).toEqual(['chunk2', 'chunk3']);
    });

    it('reads the same chunks from every instance on the table', async () => {
      const other = otherInstance();
      const streamName = 'cross-instance';

      // A live reader on the second instance, opened before any write.
      const live = collect(await other.streams.get(TEST_RUN_ID, streamName));
      await streamer.streams.write(TEST_RUN_ID, streamName, 'a');
      await streamer.streams.writeMulti?.(TEST_RUN_ID, streamName, ['b', 'c']);
      await streamer.streams.close(TEST_RUN_ID, streamName);
      await live.done;
      expect(live.received).toEqual(['a', 'b', 'c']);

      // And a fresh instance sees the finished stream.
      const fresh = otherInstance();
      expect(
        await readText(await fresh.streams.get(TEST_RUN_ID, streamName))
      ).toEqual(['a', 'b', 'c']);
      const page = await fresh.streams.getChunks(TEST_RUN_ID, streamName);
      expect(page.data.map((c) => Buffer.from(c.data).toString())).toEqual([
        'a',
        'b',
        'c',
      ]);
      expect(page.done).toBe(true);
      expect(await fresh.streams.getInfo(TEST_RUN_ID, streamName)).toEqual({
        tailIndex: 2,
        done: true,
      });
      expect(await fresh.streams.list(TEST_RUN_ID)).toEqual([streamName]);
    });
  });

  describe('writeMulti', () => {
    it('preserves chunk ordering', async () => {
      const streamName = 'ordered-multi-stream';

      await streamer.streams.writeMulti?.(TEST_RUN_ID, streamName, [
        'first',
        'second',
        'third',
      ]);
      await streamer.streams.close(TEST_RUN_ID, streamName);

      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, streamName))
      ).toEqual(['first', 'second', 'third']);
    });

    it('writes nothing for an empty chunks array', async () => {
      const streamName = 'empty-multi-stream';

      await streamer.streams.writeMulti?.(TEST_RUN_ID, streamName, []);

      expect(await streamer.streams.getInfo(TEST_RUN_ID, streamName)).toEqual({
        tailIndex: -1,
        done: false,
      });
      expect(await streamer.streams.list(TEST_RUN_ID)).toEqual([]);
    });

    it('handles mixed string and Uint8Array chunks', async () => {
      const streamName = 'mixed-multi-stream';

      await streamer.streams.writeMulti?.(TEST_RUN_ID, streamName, [
        'string-chunk',
        new Uint8Array([1, 2, 3, 4]),
        Buffer.from('buffer-chunk'),
      ]);
      await streamer.streams.close(TEST_RUN_ID, streamName);

      const chunks = await readAll(
        await streamer.streams.get(TEST_RUN_ID, streamName)
      );
      expect(chunks).toHaveLength(3);
      expect(new TextDecoder().decode(chunks[0])).toBe('string-chunk');
      expect(Array.from(chunks[1] ?? [])).toEqual([1, 2, 3, 4]);
      expect(new TextDecoder().decode(chunks[2])).toBe('buffer-chunk');
    });

    it('lands a group whole and in order for a reader that opens mid-write', async () => {
      const streamName = 'write-multi-order';
      await streamer.streams.write(TEST_RUN_ID, streamName, 'zero\n');
      const runId = deferred<string>();

      const writing = streamer.streams.writeMulti?.(
        runId.promise as unknown as string,
        streamName,
        ['first\n', 'second\n', 'third\n']
      );
      await settle();
      // The reader opens while the group is still waiting to be written.
      const stream = await streamer.streams.get(TEST_RUN_ID, streamName);
      runId.resolve(TEST_RUN_ID);
      await writing;
      await streamer.streams.close(TEST_RUN_ID, streamName);

      expect(await readText(stream)).toEqual([
        'zero\n',
        'first\n',
        'second\n',
        'third\n',
      ]);
    });

    it('keeps each group contiguous when instances write at once', async () => {
      const other = otherInstance();
      const streamName = 'write-multi-groups';
      const stream = await streamer.streams.get(TEST_RUN_ID, streamName);

      await Promise.all([
        streamer.streams.writeMulti?.(TEST_RUN_ID, streamName, [
          'a0',
          'a1',
          'a2',
        ]),
        other.streams.writeMulti?.(TEST_RUN_ID, streamName, ['b0', 'b1', 'b2']),
        streamer.streams.writeMulti?.(TEST_RUN_ID, streamName, [
          'c0',
          'c1',
          'c2',
        ]),
      ]);
      await streamer.streams.close(TEST_RUN_ID, streamName);

      const received = await readText(stream);
      expect(received).toHaveLength(9);
      // Groups never interleave, and one instance's groups keep call order.
      const groups = [0, 3, 6].map((i) => received.slice(i, i + 3));
      for (const group of groups) {
        const prefix = group[0]?.[0];
        expect(group).toEqual([`${prefix}0`, `${prefix}1`, `${prefix}2`]);
      }
      const prefixes = groups.map((group) => group[0]?.[0]);
      expect(prefixes.indexOf('a')).toBeLessThan(prefixes.indexOf('c'));
      expect([...prefixes].sort()).toEqual(['a', 'b', 'c']);
    });
  });

  describe('close', () => {
    it('closes a stream with no chunks', async () => {
      const streamName = 'empty-stream';

      await streamer.streams.close(TEST_RUN_ID, streamName);

      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, streamName))
      ).toEqual([]);
      expect(await streamer.streams.getInfo(TEST_RUN_ID, streamName)).toEqual({
        tailIndex: -1,
        done: true,
      });
      const page = await streamer.streams.getChunks(TEST_RUN_ID, streamName);
      expect(page.data).toEqual([]);
      expect(page.done).toBe(true);
    });

    it('closes a stream with existing chunks', async () => {
      const streamName = 'existing-stream';

      await streamer.streams.write(TEST_RUN_ID, streamName, 'chunk1');
      await streamer.streams.write(TEST_RUN_ID, streamName, 'chunk2');
      await streamer.streams.close(TEST_RUN_ID, streamName);

      expect(await streamer.streams.getInfo(TEST_RUN_ID, streamName)).toEqual({
        tailIndex: 1,
        done: true,
      });
      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, streamName))
      ).toEqual(['chunk1', 'chunk2']);
    });

    it('closing twice is harmless', async () => {
      const other = otherInstance();
      const streamName = 'double-close';

      await streamer.streams.write(TEST_RUN_ID, streamName, 'data');
      await streamer.streams.close(TEST_RUN_ID, streamName);
      await expect(
        streamer.streams.close(TEST_RUN_ID, streamName)
      ).resolves.toBeUndefined();
      await expect(
        other.streams.close(TEST_RUN_ID, streamName)
      ).resolves.toBeUndefined();

      expect(await streamer.streams.getInfo(TEST_RUN_ID, streamName)).toEqual({
        tailIndex: 0,
        done: true,
      });
      const page = await streamer.streams.getChunks(TEST_RUN_ID, streamName);
      expect(page.data.map((c) => Buffer.from(c.data).toString())).toEqual([
        'data',
      ]);
      expect(page.done).toBe(true);
      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, streamName))
      ).toEqual(['data']);
    });

    it('ends a live reader when the stream closes', async () => {
      const streamName = 'live-close';
      const live = collect(await streamer.streams.get(TEST_RUN_ID, streamName));

      await streamer.streams.write(TEST_RUN_ID, streamName, 'one');
      await waitFor(() => live.received.length === 1);
      await streamer.streams.close(TEST_RUN_ID, streamName);
      await live.done;

      expect(live.received).toEqual(['one']);
    });

    it('closes a stream behind a write still waiting for its run id', async () => {
      const streamName = 'close-behind-write';
      const stream = await streamer.streams.get(TEST_RUN_ID, streamName);
      const runId = deferred<string>();

      const writing = streamer.streams.write(
        runId.promise as unknown as string,
        streamName,
        'data\n'
      );
      const closing = streamer.streams.close(TEST_RUN_ID, streamName);
      await Promise.race([closing, settle()]);
      runId.resolve(TEST_RUN_ID);
      await Promise.all([writing, closing]);

      expect(await readText(stream)).toEqual(['data\n']);
    });
  });

  describe('get with startIndex', () => {
    async function writeChunks(
      streamName: string,
      count: number,
      { close = true } = {}
    ) {
      for (let i = 0; i < count; i++) {
        await streamer.streams.write(TEST_RUN_ID, streamName, `chunk${i}`);
      }
      if (close) await streamer.streams.close(TEST_RUN_ID, streamName);
    }

    it('skips that many chunks from the start', async () => {
      await writeChunks('positive', 4);

      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, 'positive', 1))
      ).toEqual(['chunk1', 'chunk2', 'chunk3']);
    });

    it('ends at once when it starts past the tail of a finished stream', async () => {
      await writeChunks('past-tail', 2);

      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, 'past-tail', 5))
      ).toEqual([]);
    });

    it('supports a negative startIndex to read from the end', async () => {
      await writeChunks('negative', 4);

      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, 'negative', -2))
      ).toEqual(['chunk2', 'chunk3']);
    });

    it('resolves a negative startIndex against an open stream, then reads live', async () => {
      await writeChunks('negative-open', 3, { close: false });

      const stream = await streamer.streams.get(
        TEST_RUN_ID,
        'negative-open',
        -1
      );
      await streamer.streams.write(TEST_RUN_ID, 'negative-open', 'chunk3');
      await streamer.streams.close(TEST_RUN_ID, 'negative-open');

      expect(await readText(stream)).toEqual(['chunk2', 'chunk3']);
    });

    it('clamps a negative startIndex beyond the chunk count to 0', async () => {
      await writeChunks('negative-clamped', 2);

      expect(
        await readText(
          await streamer.streams.get(TEST_RUN_ID, 'negative-clamped', -100)
        )
      ).toEqual(['chunk0', 'chunk1']);
    });

    it('never delivers a chunk before startIndex, even one written live', async () => {
      const streamName = 'start-index-live';

      const stream = await streamer.streams.get(TEST_RUN_ID, streamName, 1);
      await streamer.streams.write(TEST_RUN_ID, streamName, 'c0\n');
      await streamer.streams.write(TEST_RUN_ID, streamName, 'c1\n');
      await settle();
      await streamer.streams.close(TEST_RUN_ID, streamName);

      expect(await readText(stream)).toEqual(['c1\n']);
    });

    // world-postgres/test/stream-offset.test.ts and its unit counterpart: a
    // chunk the reader already skipped and sees again (there a repeated
    // NOTIFY; here the same chunk read on every poll until the next one
    // comes) must not count against the offset twice.
    it('a repeated read of a skipped chunk does not consume the offset twice', async () => {
      const name = `stream_${randomUUID()}`;
      const stream = await streamer.streams.get(TEST_RUN_ID, name, 2);
      const reader = stream.getReader();
      try {
        await streamer.streams.write(TEST_RUN_ID, name, 'first');
        // Several polls see 'first' before the next write.
        await new Promise((resolve) =>
          setTimeout(resolve, TEST_STREAM_POLL_INTERVAL_MS * 5)
        );
        await streamer.streams.write(TEST_RUN_ID, name, 'second');
        await streamer.streams.write(TEST_RUN_ID, name, 'third');
        const result = await reader.read();
        expect(result.done).toBe(false);
        expect(Buffer.from(result.value as Uint8Array).toString()).toBe(
          'third'
        );
      } finally {
        await reader.cancel();
      }
    });
  });

  describe('chunk order', () => {
    it('lands concurrent writes in the order they were called', async () => {
      const streamName = 'concurrent-write-order';
      const stream = await streamer.streams.get(TEST_RUN_ID, streamName);
      const runId = deferred<string>();

      const first = streamer.streams.write(
        runId.promise as unknown as string,
        streamName,
        'first\n'
      );
      const second = streamer.streams.write(
        TEST_RUN_ID,
        streamName,
        'second\n'
      );
      await Promise.race([second, settle()]);
      runId.resolve(TEST_RUN_ID);
      await Promise.all([first, second]);
      await streamer.streams.close(TEST_RUN_ID, streamName);

      expect(await readText(stream)).toEqual(['first\n', 'second\n']);
    });

    it('lands many writes issued at once in call order', async () => {
      const streamName = 'burst-order';
      const expected = Array.from({ length: 50 }, (_, i) => `${i}\n`);

      await Promise.all(
        expected.map((chunk) =>
          streamer.streams.write(TEST_RUN_ID, streamName, chunk)
        )
      );
      await streamer.streams.close(TEST_RUN_ID, streamName);

      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, streamName))
      ).toEqual(expected);
    });

    it('does not lose or duplicate chunks written while a reader opens', async () => {
      for (let iteration = 0; iteration < 3; iteration++) {
        const streamName = `race-${iteration}`;

        await streamer.streams.write(TEST_RUN_ID, streamName, '0\n');
        await streamer.streams.write(TEST_RUN_ID, streamName, '1\n');

        // Keep writing while the reader opens.
        const writeTask = (async () => {
          for (let i = 2; i < 10; i++) {
            await streamer.streams.write(TEST_RUN_ID, streamName, `${i}\n`);
          }
          await streamer.streams.close(TEST_RUN_ID, streamName);
        })();

        const stream = await streamer.streams.get(TEST_RUN_ID, streamName);
        const chunks = await readText(stream);
        await writeTask;

        expect(chunks).toEqual(Array.from({ length: 10 }, (_, i) => `${i}\n`));
      }
    });

    it('keeps order when chunks arrive while a reader is catching up', async () => {
      const streamName = 'ordering-test';

      for (let i = 0; i < 5; i++) {
        await streamer.streams.write(TEST_RUN_ID, streamName, `${i}\n`);
      }

      const live = collect(await streamer.streams.get(TEST_RUN_ID, streamName));
      for (let i = 5; i < 10; i++) {
        await streamer.streams.write(TEST_RUN_ID, streamName, `${i}\n`);
      }
      await streamer.streams.close(TEST_RUN_ID, streamName);
      await live.done;

      expect(live.received.join('')).toBe('0\n1\n2\n3\n4\n5\n6\n7\n8\n9\n');
    });

    it('delivers writes from two instances in commit order', async () => {
      const other = otherInstance();
      const streamName = 'two-writers';
      const live = collect(await streamer.streams.get(TEST_RUN_ID, streamName));

      await streamer.streams.write(TEST_RUN_ID, streamName, '0\n');
      await other.streams.write(TEST_RUN_ID, streamName, '1\n');
      await settle();
      await other.streams.write(TEST_RUN_ID, streamName, '2\n');
      await streamer.streams.write(TEST_RUN_ID, streamName, '3\n');
      await other.streams.close(TEST_RUN_ID, streamName);
      await live.done;

      expect(live.received).toEqual(['0\n', '1\n', '2\n', '3\n']);
    });
  });

  describe('reader lifecycle (vercel/workflow#2795, #2797)', () => {
    it('tears down the poll timer on cancel', async () => {
      const streamName = 'teardown-stream';
      const timers = trackPollTimers();
      onTestFinished(() => timers.restore());

      // A reader on a stream with no chunks (the abort-stream shape) waits
      // on the poll.
      const reader = (
        await streamer.streams.get(TEST_RUN_ID, streamName)
      ).getReader();
      const pending = reader.read();
      await waitFor(() => timers.open() === 1);

      await reader.cancel();
      await pending.catch(() => {});

      expect(timers.open()).toBe(0);
      // No poll after the cancel.
      const created = timers.created();
      await new Promise((resolve) =>
        setTimeout(resolve, TEST_STREAM_POLL_INTERVAL_MS * 4)
      );
      expect(timers.created()).toBe(created);
      // A later write must not reach the cancelled reader.
      await expect(
        streamer.streams.write(TEST_RUN_ID, streamName, 'after-cancel')
      ).resolves.toBeUndefined();
    });

    it('tears down the poll timer at the end of the stream', async () => {
      const streamName = 'teardown-at-end';
      const timers = trackPollTimers();
      onTestFinished(() => timers.restore());

      const live = collect(await streamer.streams.get(TEST_RUN_ID, streamName));
      await streamer.streams.write(TEST_RUN_ID, streamName, 'data');
      await streamer.streams.close(TEST_RUN_ID, streamName);
      await live.done;

      expect(live.received).toEqual(['data']);
      expect(timers.open()).toBe(0);
    });

    // world-postgres: 'removes the listener when a persisted EOF closes the
    // stream', with 12 readers of the same finished stream.
    it('leaves nothing behind after repeated reads of a closed stream', async () => {
      const streamName = 'stream-eof';
      await streamer.streams.write(TEST_RUN_ID, streamName, 'hello');
      await streamer.streams.close(TEST_RUN_ID, streamName);
      const timers = trackPollTimers();
      onTestFinished(() => timers.restore());

      for (let i = 0; i < 12; i++) {
        const stream = await streamer.streams.get(TEST_RUN_ID, streamName);
        const reader = stream.getReader();
        let done = false;
        while (!done) {
          ({ done } = await reader.read());
        }
        expect(timers.open()).toBe(0);
      }
      // A finished stream is read without waiting on a poll at all.
      expect(timers.created()).toBe(0);
    });

    // world-postgres: 'detaches and fails pending readers when the
    // streamer is closed', and 'rejects streams.get() after the streamer is
    // closed'.
    it('fails pending readers when the World is closed, and refuses new ones', async () => {
      const world = ctx.secondWorld();
      const streamName = 'stream-open';
      await world.streams.write(TEST_RUN_ID, streamName, 'hello');
      const timers = trackPollTimers();
      onTestFinished(() => timers.restore());

      const readers: ReadableStreamDefaultReader<Uint8Array>[] = [];
      for (let i = 0; i < 10; i++) {
        const stream = await world.streams.get(TEST_RUN_ID, streamName);
        readers.push(stream.getReader());
        // Consume the persisted chunk; the reader keeps waiting for more.
        await readers[i]?.read();
      }
      // An outstanding read() must settle once the World shuts down.
      const pending = readers[0]?.read();

      await world.close?.();
      await expect(pending).rejects.toThrow('the AWS World has been closed');
      for (const reader of readers) {
        await expect(reader.read()).rejects.toThrow(
          'the AWS World has been closed'
        );
      }
      expect(timers.open()).toBe(0);
      await expect(
        world.streams.get(TEST_RUN_ID, 'stream-after-close')
      ).rejects.toThrow('the AWS World has been closed');
    });

    it('reads only the stream asked for, however busy the others are', async () => {
      await streamer.streams.write(TEST_RUN_ID, 'target', 'hi');
      await streamer.streams.writeMulti?.(
        TEST_RUN_ID,
        'noise',
        Array.from({ length: 2000 }, (_, i) => `noise${i}`)
      );

      expect(await streamer.streams.getInfo(TEST_RUN_ID, 'target')).toEqual({
        tailIndex: 0,
        done: false,
      });
      const { data } = await streamer.streams.getChunks(TEST_RUN_ID, 'target');
      expect(data.map((c) => Buffer.from(c.data).toString())).toEqual(['hi']);
      // And the 2000 chunks landed dense and in order.
      expect(await streamer.streams.getInfo(TEST_RUN_ID, 'noise')).toEqual({
        tailIndex: 1999,
        done: false,
      });
      const page = await streamer.streams.getChunks(TEST_RUN_ID, 'noise', {
        limit: 1000,
        cursor: undefined,
      });
      expect(page.data.map((c) => c.index)).toEqual(
        Array.from({ length: 1000 }, (_, i) => i)
      );
      expect(Buffer.from(page.data[999]?.data ?? []).toString()).toBe(
        'noise999'
      );
    }, 120_000);
  });

  describe('runId as a promise', () => {
    it('holds writes until the run id resolves, then writes them in order', async () => {
      const streamName = 'promise-runid-test';
      const runId = deferred<string>();

      let writes1Complete = false;
      let writes2Complete = false;
      const writePromise1 = streamer.streams
        .write(runId.promise as unknown as string, streamName, 'chunk1\n')
        .then(() => {
          writes1Complete = true;
        });
      const writePromise2 = streamer.streams
        .write(runId.promise as unknown as string, streamName, 'chunk2\n')
        .then(() => {
          writes2Complete = true;
        });

      await new Promise((resolve) => setTimeout(resolve, 10));
      expect(writes1Complete).toBe(false);
      expect(writes2Complete).toBe(false);
      expect(await streamer.streams.getInfo(TEST_RUN_ID, streamName)).toEqual({
        tailIndex: -1,
        done: false,
      });

      runId.resolve(TEST_RUN_ID);
      await writePromise1;
      await writePromise2;

      const closeRunId = deferred<string>();
      const closePromise = streamer.streams.close(
        closeRunId.promise as unknown as string,
        streamName
      );
      closeRunId.resolve(TEST_RUN_ID);
      await closePromise;

      expect(
        await readText(await streamer.streams.get(TEST_RUN_ID, streamName))
      ).toEqual(['chunk1\n', 'chunk2\n']);
      // The resolved run id is the one the stream is listed under.
      expect(await streamer.streams.list(TEST_RUN_ID)).toEqual([streamName]);
    });
  });

  describe('list', () => {
    it('returns an empty array when no streams exist', async () => {
      expect(await streamer.streams.list(TEST_RUN_ID)).toEqual([]);
    });

    it('returns the streams of the run', async () => {
      const streamName1 = 'my-stdout-stream';
      const streamName2 = 'my-stderr-stream';

      await streamer.streams.write(TEST_RUN_ID, streamName1, 'stdout output');
      await streamer.streams.write(TEST_RUN_ID, streamName2, 'stderr output');
      await streamer.streams.close(TEST_RUN_ID, streamName1);
      await streamer.streams.close(TEST_RUN_ID, streamName2);

      expect((await streamer.streams.list(TEST_RUN_ID)).sort()).toEqual(
        [streamName1, streamName2].sort()
      );
    });

    it('does not return streams of other runs', async () => {
      const otherRunId = `wrun_${randomUUID().replaceAll('-', '').slice(0, 26)}`;

      await streamer.streams.write(TEST_RUN_ID, 'target-stdout', 'target');
      await streamer.streams.write(otherRunId, 'other-stdout', 'other');

      expect(await streamer.streams.list(TEST_RUN_ID)).toEqual([
        'target-stdout',
      ]);
      expect(await streamer.streams.list(otherRunId)).toEqual(['other-stdout']);
    });

    it('returns each stream name once, however many chunks it has', async () => {
      const streamName = 'chunked-output';

      await streamer.streams.write(TEST_RUN_ID, streamName, 'chunk1');
      await streamer.streams.writeMulti?.(TEST_RUN_ID, streamName, [
        'chunk2',
        'chunk3',
      ]);
      await streamer.streams.close(TEST_RUN_ID, streamName);

      expect(await streamer.streams.list(TEST_RUN_ID)).toEqual([streamName]);
    });

    it('handles stream names with dashes', async () => {
      const streamName = 'my-complex-stream-name';

      await streamer.streams.write(TEST_RUN_ID, streamName, 'data');
      await streamer.streams.close(TEST_RUN_ID, streamName);

      expect(await streamer.streams.list(TEST_RUN_ID)).toEqual([streamName]);
    });

    it('lists a stream that was only closed', async () => {
      await streamer.streams.close(TEST_RUN_ID, 'close-only-stream');

      expect(await streamer.streams.list(TEST_RUN_ID)).toEqual([
        'close-only-stream',
      ]);
    });

    describe('concurrent first writes', () => {
      const names = (prefix: string, count: number) =>
        Array.from({ length: count }, (_, i) => `${prefix}-${i}`);

      it('keeps every name when many streams of a run first write at once', async () => {
        const streamNames = names('stream', 20);

        await Promise.all(
          streamNames.map((name) =>
            streamer.streams.write(TEST_RUN_ID, name, 'data')
          )
        );

        expect((await streamer.streams.list(TEST_RUN_ID)).sort()).toEqual(
          [...streamNames].sort()
        );
      });

      it('keeps every name across write, writeMulti and close', async () => {
        await Promise.all([
          streamer.streams.write(TEST_RUN_ID, 'written-0', 'a'),
          streamer.streams.writeMulti?.(TEST_RUN_ID, 'multi-0', ['a', 'b']),
          streamer.streams.close(TEST_RUN_ID, 'closed-0'),
          streamer.streams.write(TEST_RUN_ID, 'written-1', 'a'),
          streamer.streams.writeMulti?.(TEST_RUN_ID, 'multi-1', ['a', 'b']),
          streamer.streams.close(TEST_RUN_ID, 'closed-1'),
        ]);

        expect((await streamer.streams.list(TEST_RUN_ID)).sort()).toEqual([
          'closed-0',
          'closed-1',
          'multi-0',
          'multi-1',
          'written-0',
          'written-1',
        ]);
      });

      // world-local runs this untagged and tagged (its data directory's
      // per-tag files); a table has no tags, so it runs once.
      it('keeps every name when two Worlds share the table', async () => {
        const first = otherInstance();
        const second = otherInstance();

        for (let i = 0; i < 10; i++) {
          const runId = `${TEST_RUN_ID.slice(0, -2)}s${i}`;
          const firstNames = names('first', 3);
          const secondNames = names('second', 3);
          await Promise.all([
            ...firstNames.map((name) => first.streams.write(runId, name, 'a')),
            ...secondNames.map((name) =>
              second.streams.write(runId, name, 'b')
            ),
          ]);

          expect((await first.streams.list(runId)).sort()).toEqual(
            [...firstNames, ...secondNames].sort()
          );
        }
      });
    });
  });

  describe('getChunks', () => {
    it('paginates through all chunks', async () => {
      const streamName = 'paginated-stream';

      await streamer.streams.write(TEST_RUN_ID, streamName, 'a');
      await streamer.streams.write(TEST_RUN_ID, streamName, 'b');
      await streamer.streams.write(TEST_RUN_ID, streamName, 'c');
      await streamer.streams.close(TEST_RUN_ID, streamName);

      const page1 = await streamer.streams.getChunks(TEST_RUN_ID, streamName, {
        limit: 2,
      });
      expect(page1.data.map((c) => c.index)).toEqual([0, 1]);
      expect(page1.data.map((c) => Buffer.from(c.data).toString())).toEqual([
        'a',
        'b',
      ]);
      expect(page1.hasMore).toBe(true);
      expect(page1.done).toBe(false);
      expect(page1.cursor).not.toBeNull();

      const page2 = await streamer.streams.getChunks(TEST_RUN_ID, streamName, {
        limit: 2,
        cursor: page1.cursor ?? undefined,
      });
      expect(page2.data.map((c) => c.index)).toEqual([2]);
      expect(Buffer.from(page2.data[0]?.data ?? []).toString()).toBe('c');
      expect(page2.hasMore).toBe(false);
      expect(page2.cursor).toBeNull();
      expect(page2.done).toBe(true);
    });

    it('reports done on a page that ends exactly at the end of the stream', async () => {
      const streamName = 'exact-page';

      await streamer.streams.writeMulti?.(TEST_RUN_ID, streamName, ['a', 'b']);
      await streamer.streams.close(TEST_RUN_ID, streamName);

      const page = await streamer.streams.getChunks(TEST_RUN_ID, streamName, {
        limit: 2,
      });
      expect(page.data).toHaveLength(2);
      expect(page.hasMore).toBe(false);
      expect(page.cursor).toBeNull();
      expect(page.done).toBe(true);
    });

    it('continues from a cursor as an open stream grows', async () => {
      const streamName = 'growing';

      await streamer.streams.writeMulti?.(TEST_RUN_ID, streamName, ['a', 'b']);
      const page1 = await streamer.streams.getChunks(TEST_RUN_ID, streamName, {
        limit: 1,
      });
      expect(page1.hasMore).toBe(true);

      await streamer.streams.write(TEST_RUN_ID, streamName, 'c');
      await streamer.streams.close(TEST_RUN_ID, streamName);

      const page2 = await streamer.streams.getChunks(TEST_RUN_ID, streamName, {
        cursor: page1.cursor ?? undefined,
      });
      expect(page2.data.map((c) => c.index)).toEqual([1, 2]);
      expect(page2.data.map((c) => Buffer.from(c.data).toString())).toEqual([
        'b',
        'c',
      ]);
      expect(page2.done).toBe(true);
    });

    it('returns done=false for an in-progress stream', async () => {
      const streamName = 'in-progress';

      await streamer.streams.write(TEST_RUN_ID, streamName, 'data');

      const result = await streamer.streams.getChunks(TEST_RUN_ID, streamName);
      expect(result.data).toHaveLength(1);
      expect(result.hasMore).toBe(false);
      expect(result.done).toBe(false);
    });

    it('returns empty data for a nonexistent stream', async () => {
      const result = await streamer.streams.getChunks(
        TEST_RUN_ID,
        'nonexistent'
      );
      expect(result.data).toEqual([]);
      expect(result.hasMore).toBe(false);
      expect(result.cursor).toBeNull();
      expect(result.done).toBe(false);
    });

    it('restarts from the beginning on an invalid cursor', async () => {
      const streamName = 'bad-cursor';

      await streamer.streams.write(TEST_RUN_ID, streamName, 'data');
      await streamer.streams.close(TEST_RUN_ID, streamName);

      const result = await streamer.streams.getChunks(TEST_RUN_ID, streamName, {
        cursor: 'not-valid-base64-json',
      });
      expect(result.data).toHaveLength(1);
      expect(result.data[0]?.index).toBe(0);
    });
  });

  describe('getInfo', () => {
    it('returns tailIndex and done for a completed stream', async () => {
      const streamName = 'info-completed';

      await streamer.streams.write(TEST_RUN_ID, streamName, 'a');
      await streamer.streams.write(TEST_RUN_ID, streamName, 'b');
      await streamer.streams.close(TEST_RUN_ID, streamName);

      expect(await streamer.streams.getInfo(TEST_RUN_ID, streamName)).toEqual({
        tailIndex: 1,
        done: true,
      });
    });

    it('returns tailIndex for an in-progress stream', async () => {
      const streamName = 'info-progress';

      await streamer.streams.write(TEST_RUN_ID, streamName, 'a');
      await streamer.streams.write(TEST_RUN_ID, streamName, 'b');

      expect(await streamer.streams.getInfo(TEST_RUN_ID, streamName)).toEqual({
        tailIndex: 1,
        done: false,
      });
    });

    it('returns -1 for a nonexistent stream', async () => {
      expect(
        await streamer.streams.getInfo(TEST_RUN_ID, 'nonexistent')
      ).toEqual({ tailIndex: -1, done: false });
    });

    it('returns tailIndex 0 for a single-chunk stream', async () => {
      const streamName = 'single-chunk';

      await streamer.streams.write(TEST_RUN_ID, streamName, 'only');
      await streamer.streams.close(TEST_RUN_ID, streamName);

      expect(await streamer.streams.getInfo(TEST_RUN_ID, streamName)).toEqual({
        tailIndex: 0,
        done: true,
      });
    });
  });
});

/**
 * world-postgres/test/stream-post-eof-duplicates.test.ts: a producer that
 * retries a terminal write (lost ACK, overlapping attempts) appends data
 * and EOF after the stream's first EOF through the ordinary write/close
 * API. Here those writes are dropped; reads must show the stream as it was
 * at its first close.
 */
describe('stream reads after a retried terminal write', () => {
  async function writeStreamWithRetriedClose(runId: string, name: string) {
    for (const text of ['a', 'b', 'c', 'd', 'e']) {
      await streamer.streams.write(runId, name, text);
    }
    await streamer.streams.close(runId, name);
    // The retried terminal write: a lost ACK or overlapping attempt appends
    // another data row and EOF after the first EOF.
    await streamer.streams.write(runId, name, 'e');
    await streamer.streams.close(runId, name);
  }

  it('getChunks() excludes rows written after the first EOF', async () => {
    const name = `stream_${randomUUID()}`;
    await writeStreamWithRetriedClose(TEST_RUN_ID, name);

    const result = await streamer.streams.getChunks(TEST_RUN_ID, name);

    expect(result.data.map((c) => Buffer.from(c.data).toString())).toEqual([
      'a',
      'b',
      'c',
      'd',
      'e',
    ]);
    expect(result.hasMore).toBe(false);
    expect(result.done).toBe(true);
  });

  it('getChunks() paginates only up to the first EOF, not past it', async () => {
    const name = `stream_${randomUUID()}`;
    await writeStreamWithRetriedClose(TEST_RUN_ID, name);

    const page = await streamer.streams.getChunks(TEST_RUN_ID, name, {
      limit: 3,
    });
    expect(page.data.map((c) => Buffer.from(c.data).toString())).toEqual([
      'a',
      'b',
      'c',
    ]);
    expect(page.hasMore).toBe(true);
    expect(page.cursor).not.toBeNull();

    const rest = await streamer.streams.getChunks(TEST_RUN_ID, name, {
      cursor: page.cursor ?? undefined,
    });
    expect(rest.data.map((c) => Buffer.from(c.data).toString())).toEqual([
      'd',
      'e',
    ]);
    expect(rest.hasMore).toBe(false);
  });

  it('getInfo() does not count rows written after the first EOF', async () => {
    const name = `stream_${randomUUID()}`;
    await writeStreamWithRetriedClose(TEST_RUN_ID, name);

    const info = await streamer.streams.getInfo(TEST_RUN_ID, name);

    expect(info.tailIndex).toBe(4);
    expect(info.done).toBe(true);
  });

  it('get() delivers the stream as of its first close', async () => {
    const name = `stream_${randomUUID()}`;
    await writeStreamWithRetriedClose(TEST_RUN_ID, name);

    expect(
      await readText(await streamer.streams.get(TEST_RUN_ID, name))
    ).toEqual(['a', 'b', 'c', 'd', 'e']);
  });
});

/**
 * world-postgres/src/streamer.test.ts, "streams.get() rows written after
 * the first EOF": rows past the end, which this World never writes itself,
 * are put as raw items after `CLOSED` (as a writer that lost a race with
 * an older close would have left them).
 */
describe('streams.get() rows written after the first EOF', () => {
  async function streamWithRowsAfterEof(name: string) {
    for (const text of ['a', 'b', 'c', 'd', 'e']) {
      await streamer.streams.write(TEST_RUN_ID, name, `${text}\n`);
    }
    await streamer.streams.close(TEST_RUN_ID, name);
    await ctx.clients.documents.send(
      new PutCommand({
        TableName: ctx.tableName,
        Item: {
          PK: streamPk(TEST_RUN_ID, name),
          SK: chunkSk(5),
          type: 'chunk',
          runId: TEST_RUN_ID,
          data: new TextEncoder().encode('e\n'),
        },
      })
    );
  }

  async function drain(stream: ReadableStream<Uint8Array>): Promise<string> {
    const reader = stream.getReader();
    const decoder = new TextDecoder();
    let out = '';
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      out += decoder.decode(value, { stream: true });
    }
    return out;
  }

  it('ignores rows written after the first EOF instead of erroring the stream', async () => {
    await streamWithRowsAfterEof('stream-1');
    const stream = await streamer.streams.get(TEST_RUN_ID, 'stream-1', 0);
    await expect(drain(stream)).resolves.toBe('a\nb\nc\nd\ne\n');
  });

  it.each([5, 6])(
    'a start index at or past the data count (%i) closes at the first EOF instead of consuming it',
    async (startIndex) => {
      await streamWithRowsAfterEof('stream-1');
      const stream = await streamer.streams.get(
        TEST_RUN_ID,
        'stream-1',
        startIndex
      );
      await expect(drain(stream)).resolves.toBe('');
    },
    5_000
  );

  it.each([
    [-1, 'e\n'],
    [-2, 'd\ne\n'],
  ])(
    'a negative start index (%i) counts data chunks up to the first EOF only',
    async (startIndex, expected) => {
      await streamWithRowsAfterEof('stream-1');
      const stream = await streamer.streams.get(
        TEST_RUN_ID,
        'stream-1',
        startIndex
      );
      await expect(drain(stream)).resolves.toBe(expected);
    },
    5_000
  );
});

describe('chunks too large for an item', () => {
  it('round-trips a chunk over the item limit through S3', async () => {
    const streamName = 'large-chunks';
    const large = new Uint8Array(600 * 1024).map((_, i) => i % 251);

    await streamer.streams.write(TEST_RUN_ID, streamName, 'small');
    await streamer.streams.writeMulti?.(TEST_RUN_ID, streamName, [
      large,
      'after',
    ]);
    await streamer.streams.close(TEST_RUN_ID, streamName);

    const chunks = await readAll(
      await streamer.streams.get(TEST_RUN_ID, streamName)
    );
    expect(chunks.map((chunk) => chunk.byteLength)).toEqual([
      5,
      large.byteLength,
      5,
    ]);
    expect(
      Buffer.compare(Buffer.from(chunks[1] ?? []), Buffer.from(large))
    ).toBe(0);
    const page = await streamer.streams.getChunks(TEST_RUN_ID, streamName);
    expect(page.data[1]?.data).toEqual(large);

    // The item holds a reference to an object under the stream's prefix,
    // one segment per chunk position.
    const { Item } = await ctx.clients.documents.send(
      new GetCommand({
        TableName: ctx.tableName,
        Key: { PK: streamPk(TEST_RUN_ID, streamName), SK: chunkSk(1) },
      })
    );
    const ref = Item?.data as { s3Ref: string };
    expect(ref.s3Ref).toMatch(
      new RegExp(
        `^s3://${aws.bucket}/${TEST_RUN_ID}/stream/${encodeURIComponent(streamName)}/0000000001/data-`
      )
    );
    const { bucket, key } = s3Location(ref.s3Ref);
    await expect(
      ctx.clients.s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }))
    ).resolves.toMatchObject({ ContentLength: large.byteLength });
  });

  it('refuses a chunk over the item limit without a bucket, naming the setting', async () => {
    const world = ctx.secondWorld({ bucket: undefined });
    onTestFinished(async () => {
      await world.close?.();
    });

    await expect(
      world.streams.write(TEST_RUN_ID, 'no-bucket', new Uint8Array(400 * 1024))
    ).rejects.toMatchObject({
      name: 'PayloadTooLargeError',
      status: 413,
      message: expect.stringContaining('WORKFLOW_AWS_BUCKET'),
    });
    expect(await world.streams.getInfo(TEST_RUN_ID, 'no-bucket')).toEqual({
      tailIndex: -1,
      done: false,
    });
  });
});
