/**
 * Streams on the World's table.
 *
 * A stream is a partition, `STREAM#<runId>#<name>`, of numbered chunks
 * (`CHUNK#<seq>`, 0-based, zero-padded) and, once closed, a `CLOSED` item
 * that records how many chunks it has. `CLOSED` sorts after every
 * `CHUNK#`, so one query from a sequence number on returns the chunks that
 * follow and, at the end, whether the stream is closed.
 *
 * - **Writes.** A group of chunks (one `write`, or up to 98 of a
 *   `writeMulti`) is one transaction: each chunk is put at the next free
 *   sequence number on the condition that it is free, next to a condition
 *   that the stream is not closed. Sequence numbers are therefore dense and
 *   unique, as event positions are, and a group lands whole and contiguous.
 *   A writer that loses the race reads the tail again and retries. A write
 *   to a closed stream is dropped, as world-postgres' readers ignore rows
 *   after the first EOF.
 * - **Close.** Puts `CLOSED` with the chunk count, on the condition that the
 *   chunk at that position is still free, so a close and a write never
 *   both commit around each other: a chunk is either before the end or not
 *   written. Closing again does nothing.
 * - **Reads.** `get()` polls (`Query SK >= CHUNK#<next>`) every
 *   `pollIntervalMs` until it has delivered every chunk before `CLOSED`.
 *   Cancelling the reader, or closing the World, stops it with no timer
 *   left behind. `getChunks()` and `getInfo()` read a snapshot.
 * - In one World instance, the writes and closes of a stream run in the
 *   order they were called, each after the run id it was given resolves.
 *
 * Chunks over the item limit go to S3 like any payload (`payloads.ts`).
 * `streams.list` reads `STREAMS#<runId>`, where the first write or close of
 * a stream from each World instance records its name.
 */
import { WorkflowWorldError } from '@workflow/errors';
import type {
  GetChunksOptions,
  StreamChunksResponse,
  Streamer,
  StreamInfoResponse,
} from '@workflow/world';
import {
  chunkSeqFromSk,
  chunkSk,
  MAX_SORT_NUMBER,
  PREFIX,
  STREAM_CLOSED_SK,
  streamClosedKey,
  streamNameKey,
  streamNamesPk,
  streamPk,
} from '../keys.js';
import { MAX_ITEM_BYTES, MAX_TRANSACTION_BYTES } from '../storage/errors.js';
import type { Item } from '../storage/items.js';
import { debugLog } from '../storage/log.js';
import {
  MAX_TRANSACTION_ITEMS,
  sleep,
  type Table,
  type TransactItem,
} from '../storage/table.js';

export interface StreamerOptions {
  table: Table;
  /** How often a live reader looks for new chunks. */
  pollIntervalMs: number;
  /** Attempts of a write that keeps losing races; default 50. */
  maxAttempts?: number;
}

export type AwsStreamer = Streamer & {
  /**
   * Stops every reader (they fail with a "closed" error) and waits for the
   * writes in progress. Later reads are refused.
   */
  close(): Promise<void>;
};

/** Chunks per transaction: two items are the closed check and the name. */
const MAX_GROUP_CHUNKS = MAX_TRANSACTION_ITEMS - 2;
/** Bytes per transaction, below DynamoDB's 4 MB. */
const MAX_GROUP_BYTES = MAX_TRANSACTION_BYTES - 256 * 1024;
/** What a chunk item adds besides its data. */
const CHUNK_OVERHEAD_BYTES = 256;
/** What a chunk moved to S3 leaves in its item. */
const OFFLOADED_CHUNK_BYTES = 512;
/** Streams whose tail and name this instance remembers, at most. */
const MAX_REMEMBERED_STREAMS = 10_000;
const DEFAULT_PAGE = 100;
const MAX_PAGE = 1_000;
const FIRST_CHUNK_SK = chunkSk(0);
const LAST_CHUNK_SK = chunkSk(MAX_SORT_NUMBER);

const encoder = new TextEncoder();

function toBytes(chunk: string | Uint8Array): Uint8Array {
  return typeof chunk === 'string' ? encoder.encode(chunk) : chunk;
}

/** Whether `error` is a cancelled transaction (a condition failed). */
function isCancelled(error: unknown): boolean {
  const name = (error as { name?: string } | undefined)?.name;
  return (
    name === 'TransactionCanceledException' ||
    name === 'TransactionConflictException'
  );
}

interface Chunk {
  seq: number;
  data: Uint8Array;
}

function chunkOf(item: Item): Chunk {
  const data = item.data;
  return {
    seq: chunkSeqFromSk(String(item.SK)),
    data:
      data instanceof Uint8Array
        ? new Uint8Array(data.buffer, data.byteOffset, data.byteLength)
        : new Uint8Array(),
  };
}

function encodeCursor(index: number): string {
  return Buffer.from(JSON.stringify({ i: index })).toString('base64url');
}

/** The index a cursor continues from; 0 for anything else. */
function decodeCursor(cursor: string | undefined): number {
  if (!cursor) return 0;
  try {
    const { i } = JSON.parse(Buffer.from(cursor, 'base64url').toString());
    return Number.isInteger(i) && i >= 0 ? i : 0;
  } catch {
    return 0;
  }
}

export function createStreamer(options: StreamerOptions): AwsStreamer {
  const { table, pollIntervalMs } = options;
  const maxAttempts = options.maxAttempts ?? 50;
  const offloads = table.payloads?.enabled === true;

  /** Per stream name, the tail of this instance's writes and closes. */
  const chains = new Map<string, Promise<void>>();
  /** Per stream, the next free sequence number as this instance saw it. */
  const tails = new Map<string, number>();
  /** Streams whose name this instance has recorded. */
  const named = new Set<string>();
  /** One stop per open reader. */
  const readers = new Set<(error: Error) => void>();
  let closed = false;

  const closedError = () =>
    new Error('Cannot read stream: the AWS World has been closed');

  /** Runs `task` after every earlier write or close of `name`. */
  const inOrder = (name: string, task: () => Promise<void>) => {
    const previous = chains.get(name) ?? Promise.resolve();
    const run = previous.then(task);
    const settled = run.then(
      () => {},
      () => {}
    );
    chains.set(name, settled);
    void settled.then(() => {
      if (chains.get(name) === settled) chains.delete(name);
    });
    return run;
  };

  /** The sequence number after the last chunk; 0 for no chunk. */
  const readTail = async (pk: string): Promise<number> => {
    const { items } = await table.queryPage({
      KeyConditionExpression: 'PK = :pk AND SK BETWEEN :first AND :last',
      ExpressionAttributeValues: {
        ':pk': pk,
        ':first': FIRST_CHUNK_SK,
        ':last': LAST_CHUNK_SK,
      },
      ProjectionExpression: 'SK',
      ScanIndexForward: false,
      Limit: 1,
      ConsistentRead: true,
    });
    const last = items[0];
    return last ? chunkSeqFromSk(String(last.SK)) + 1 : 0;
  };

  /** The chunk count `CLOSED` recorded, or undefined while open. */
  const readClosed = async (
    runId: string,
    name: string
  ): Promise<number | undefined> => {
    const item = await table.get(streamClosedKey(runId, name));
    return item ? Number(item.count) : undefined;
  };

  const nameItem = (runId: string, name: string): TransactItem => ({
    Put: {
      Item: { ...streamNameKey(runId, name), type: 'stream', runId, name },
    },
  });

  /** Splits chunks into groups that each fit one transaction. */
  const groupsOf = (chunks: Uint8Array[]): Uint8Array[][] => {
    const groups: Uint8Array[][] = [];
    let group: Uint8Array[] = [];
    let bytes = 0;
    for (const chunk of chunks) {
      const size =
        CHUNK_OVERHEAD_BYTES +
        (offloads && chunk.byteLength > MAX_ITEM_BYTES
          ? OFFLOADED_CHUNK_BYTES
          : chunk.byteLength);
      if (
        group.length > 0 &&
        (group.length === MAX_GROUP_CHUNKS || bytes + size > MAX_GROUP_BYTES)
      ) {
        groups.push(group);
        group = [];
        bytes = 0;
      }
      group.push(chunk);
      bytes += size;
    }
    if (group.length > 0) groups.push(group);
    return groups;
  };

  /** Retries `attempt` while it loses races; see the module comment. */
  const withRetries = async (
    what: string,
    attempt: () => Promise<'done' | 'retry'>
  ) => {
    for (let n = 0; n < maxAttempts; n++) {
      if (n >= 2) await sleep(Math.random() * Math.min(250, 5 * 2 ** n));
      if ((await attempt()) === 'done') return;
    }
    throw new WorkflowWorldError(
      `Could not ${what} after ${maxAttempts} attempts: other writers kept taking its position`,
      { status: 503 }
    );
  };

  const writeChunks = async (
    runId: string,
    name: string,
    chunks: Uint8Array[]
  ) => {
    const pk = streamPk(runId, name);
    for (const group of groupsOf(chunks)) {
      let dropped = false;
      await withRetries(`write to stream "${name}"`, async () => {
        const next = tails.get(pk) ?? (await readTail(pk));
        const items: TransactItem[] = [
          {
            ConditionCheck: {
              Key: streamClosedKey(runId, name),
              ConditionExpression: 'attribute_not_exists(PK)',
            },
          },
          ...group.map(
            (data, i): TransactItem => ({
              Put: {
                Item: {
                  PK: pk,
                  SK: chunkSk(next + i),
                  type: 'chunk',
                  runId,
                  data,
                },
                ConditionExpression: 'attribute_not_exists(PK)',
              },
            })
          ),
          ...(named.has(pk) ? [] : [nameItem(runId, name)]),
        ];
        try {
          await table.transact(items);
        } catch (error) {
          if (!isCancelled(error)) throw error;
          tails.delete(pk);
          if ((await readClosed(runId, name)) !== undefined) {
            dropped = true;
            return 'done';
          }
          return 'retry';
        }
        // Hints only: forgetting them costs a read (or a name put) later.
        if (tails.size >= MAX_REMEMBERED_STREAMS) tails.clear();
        if (named.size >= MAX_REMEMBERED_STREAMS) named.clear();
        tails.set(pk, next + group.length);
        named.add(pk);
        return 'done';
      });
      if (dropped) {
        debugLog('dropped a write to a closed stream', { runId });
        return;
      }
    }
  };

  const closeStream = async (runId: string, name: string) => {
    const pk = streamPk(runId, name);
    await withRetries(`close stream "${name}"`, async () => {
      const count = tails.get(pk) ?? (await readTail(pk));
      try {
        await table.transact([
          {
            Put: {
              Item: {
                ...streamClosedKey(runId, name),
                type: 'stream_closed',
                runId,
                count,
                closedAt: new Date().toISOString(),
              },
              ConditionExpression: 'attribute_not_exists(PK)',
            },
          },
          {
            ConditionCheck: {
              Key: { PK: pk, SK: chunkSk(count) },
              ConditionExpression: 'attribute_not_exists(PK)',
            },
          },
          nameItem(runId, name),
        ]);
      } catch (error) {
        if (!isCancelled(error)) throw error;
        tails.delete(pk);
        // Closed already: closing again does nothing.
        return (await readClosed(runId, name)) === undefined ? 'retry' : 'done';
      }
      return 'done';
    });
    tails.delete(pk);
    named.delete(pk);
  };

  const getInfo = async (
    runId: string,
    name: string
  ): Promise<StreamInfoResponse> => {
    const count = await readClosed(runId, name);
    if (count !== undefined) return { tailIndex: count - 1, done: true };
    return {
      tailIndex: (await readTail(streamPk(runId, name))) - 1,
      done: false,
    };
  };

  return {
    streams: {
      write(runId, name, chunk) {
        return inOrder(name, async () =>
          writeChunks(await runId, name, [toBytes(chunk)])
        );
      },

      writeMulti(runId, name, chunks) {
        return inOrder(name, async () => {
          const resolved = await runId;
          if (chunks.length === 0) return;
          await writeChunks(resolved, name, chunks.map(toBytes));
        });
      },

      close(runId, name) {
        return inOrder(name, async () => closeStream(await runId, name));
      },

      async get(runId, name, startIndex) {
        if (closed) throw closedError();
        const pk = streamPk(runId, name);
        let next = Math.min(startIndex ?? 0, MAX_SORT_NUMBER);
        if (next < 0) {
          // Against the stream as it is now, as world-local resolves it.
          const { tailIndex } = await getInfo(runId, name);
          next = Math.max(0, tailIndex + 1 + next);
        }
        const abort = new AbortController();
        let controller!: ReadableStreamDefaultController<Uint8Array>;
        let finished = false;
        const finish = () => {
          finished = true;
          abort.abort();
          readers.delete(stop);
        };
        const stop = (error: Error) => {
          if (finished) return;
          finish();
          controller.error(error);
        };
        readers.add(stop);

        return new ReadableStream<Uint8Array>({
          start(c) {
            controller = c;
          },
          async pull(c) {
            try {
              while (!finished) {
                const { items, lastKey } = await table.queryPage(
                  {
                    KeyConditionExpression:
                      'PK = :pk AND SK BETWEEN :from AND :closed',
                    ExpressionAttributeValues: {
                      ':pk': pk,
                      ':from': chunkSk(next),
                      ':closed': STREAM_CLOSED_SK,
                    },
                    ConsistentRead: true,
                  },
                  { signal: abort.signal }
                );
                if (finished) return;
                const end = items.find((item) => item.SK === STREAM_CLOSED_SK);
                const count = end ? Number(end.count) : undefined;
                let delivered = 0;
                for (const item of items) {
                  if (!String(item.SK).startsWith(PREFIX.chunk)) continue;
                  const chunk = chunkOf(item);
                  // Past the end (a write that raced an old close), or a
                  // gap a writer is still filling: stop here.
                  if (count !== undefined && chunk.seq >= count) break;
                  if (chunk.seq !== next) break;
                  next++;
                  if (chunk.data.byteLength > 0) {
                    c.enqueue(chunk.data);
                    delivered++;
                  }
                }
                if (count !== undefined && next >= count) {
                  finish();
                  c.close();
                  return;
                }
                if (delivered > 0) return;
                if (!lastKey) await sleep(pollIntervalMs, abort.signal);
              }
            } catch (error) {
              if (finished) return;
              finish();
              throw error;
            }
          },
          cancel() {
            finish();
          },
        });
      },

      async list(runId) {
        const items = await table.collect(
          {
            KeyConditionExpression: 'PK = :pk',
            ExpressionAttributeValues: { ':pk': streamNamesPk(runId) },
            ConsistentRead: true,
          },
          Number.POSITIVE_INFINITY
        );
        return items.map((item) => String(item.name));
      },

      async getChunks(
        runId: string,
        name: string,
        options?: GetChunksOptions
      ): Promise<StreamChunksResponse> {
        const limit = Math.min(
          MAX_PAGE,
          Math.max(1, Math.floor(options?.limit ?? DEFAULT_PAGE))
        );
        const start = decodeCursor(options?.cursor);
        const [count, items] = await Promise.all([
          readClosed(runId, name),
          table.collect(
            {
              KeyConditionExpression: 'PK = :pk AND SK BETWEEN :from AND :last',
              ExpressionAttributeValues: {
                ':pk': streamPk(runId, name),
                ':from': chunkSk(Math.min(start, MAX_SORT_NUMBER)),
                ':last': LAST_CHUNK_SK,
              },
              ConsistentRead: true,
            },
            limit + 1
          ),
        ]);
        const chunks: Chunk[] = [];
        for (const item of items) {
          const chunk = chunkOf(item);
          if (count !== undefined && chunk.seq >= count) break;
          if (chunk.seq !== start + chunks.length) break;
          chunks.push(chunk);
        }
        const hasMore = chunks.length > limit;
        const page = chunks.slice(0, limit);
        return {
          data: page.map((chunk) => ({ index: chunk.seq, data: chunk.data })),
          cursor: hasMore ? encodeCursor(start + page.length) : null,
          hasMore,
          done: count !== undefined && !hasMore && start + page.length >= count,
        };
      },

      getInfo,
    },

    async close() {
      closed = true;
      for (const stop of [...readers]) stop(closedError());
      await Promise.allSettled([...chains.values()]);
    },
  };
}
