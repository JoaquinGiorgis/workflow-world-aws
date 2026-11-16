import {
  DynamoDBClient,
  TransactionCanceledException,
} from '@aws-sdk/client-dynamodb';
import {
  DynamoDBDocumentClient,
  GetCommand,
  type GetCommandInput,
  QueryCommand,
  type QueryCommandInput,
  TransactWriteCommand,
  type TransactWriteCommandInput,
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import {
  afterAll,
  afterEach,
  beforeEach,
  describe,
  expect,
  it,
  vi,
} from 'vitest';
import { chunkSk } from '../keys.js';
import { Table } from '../storage/table.js';
import { type AwsStreamer, createStreamer } from './index.js';

const documents = DynamoDBDocumentClient.from(
  new DynamoDBClient({
    region: 'us-east-1',
    credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
  })
);
const ddb = mockClient(documents);
const runId = 'wrun_01JD000000000000000000000A';
const pk = `STREAM#${runId}#out`;
const POLL_MS = 15;

afterAll(() => {
  ddb.restore();
});

let streamer: AwsStreamer;

beforeEach(() => {
  ddb.reset();
  streamer = createStreamer({
    table: new Table(documents, 'wf'),
    pollIntervalMs: POLL_MS,
    maxAttempts: 5,
  });
});

afterEach(async () => {
  await streamer.close();
});

const cancelled = (codes: string[]) =>
  new TransactionCanceledException({
    message: 'Transaction cancelled',
    $metadata: {},
    CancellationReasons: codes.map((Code) => ({ Code })),
  });

function transactions(): TransactWriteCommandInput[] {
  return ddb
    .commandCalls(TransactWriteCommand)
    .map((call) => call.args[0].input);
}

/** The chunk sequence numbers a transaction puts. */
function chunkPuts(input: TransactWriteCommandInput | undefined): string[] {
  return (input?.TransactItems ?? [])
    .map((item) => item.Put?.Item?.SK as string | undefined)
    .filter((sk): sk is string => sk?.startsWith('CHUNK#') === true);
}

/** Serves the tail query (the last chunk) from `tail`. */
function serveTail(tail: () => number | undefined) {
  ddb.on(QueryCommand).callsFake((input: QueryCommandInput) => {
    if (input.ScanIndexForward === false && input.Limit === 1) {
      const last = tail();
      return { Items: last === undefined ? [] : [{ SK: chunkSk(last) }] };
    }
    return { Items: [] };
  });
}

describe('writes', () => {
  it('puts a chunk at the next free sequence number, on conditions', async () => {
    serveTail(() => 4);
    ddb.on(TransactWriteCommand).resolves({});

    await streamer.streams.write(runId, 'out', 'hello');

    const [first] = transactions();
    expect(first?.TransactItems?.[0]?.ConditionCheck).toMatchObject({
      Key: { PK: pk, SK: 'CLOSED' },
      ConditionExpression: 'attribute_not_exists(PK)',
    });
    expect(first?.TransactItems?.[1]?.Put).toMatchObject({
      Item: { PK: pk, SK: chunkSk(5), runId, type: 'chunk' },
      ConditionExpression: 'attribute_not_exists(PK)',
    });
    expect(first?.TransactItems?.[1]?.Put?.Item?.data).toEqual(
      new TextEncoder().encode('hello')
    );
    // The first write records the stream's name for list().
    expect(first?.TransactItems?.[2]?.Put?.Item).toMatchObject({
      PK: `STREAMS#${runId}`,
      SK: 'NAME#out',
      name: 'out',
    });

    // The next write continues from what this instance wrote, without
    // reading the tail again, and does not record the name again.
    await streamer.streams.write(runId, 'out', 'again');
    expect(chunkPuts(transactions()[1])).toEqual([chunkSk(6)]);
    expect(transactions()[1]?.TransactItems).toHaveLength(2);
    expect(
      ddb
        .commandCalls(QueryCommand)
        .filter((call) => call.args[0].input.Limit === 1)
    ).toHaveLength(1);
  });

  it('reads the tail again and retries when another writer took the position', async () => {
    let tail = 4;
    serveTail(() => tail);
    ddb
      .on(TransactWriteCommand)
      .callsFakeOnce(() => {
        // Another writer committed chunks 5 and 6 first.
        tail = 6;
        throw cancelled(['None', 'ConditionalCheckFailed', 'None']);
      })
      .resolves({});
    ddb.on(GetCommand).resolves({});

    await streamer.streams.writeMulti?.(runId, 'out', ['a', 'b']);

    const [lost, won] = transactions();
    expect(chunkPuts(lost)).toEqual([chunkSk(5), chunkSk(6)]);
    expect(chunkPuts(won)).toEqual([chunkSk(7), chunkSk(8)]);
    // It checked whether the stream had been closed meanwhile.
    expect(ddb.commandCalls(GetCommand)[0]?.args[0].input.Key).toEqual({
      PK: pk,
      SK: 'CLOSED',
    });
  });

  it('drops a write to a closed stream, without retrying', async () => {
    serveTail(() => 1);
    ddb
      .on(TransactWriteCommand)
      .rejects(cancelled(['ConditionalCheckFailed', 'None', 'None']));
    ddb.on(GetCommand).resolves({ Item: { PK: pk, SK: 'CLOSED', count: 2 } });

    await expect(
      streamer.streams.write(runId, 'out', 'late')
    ).resolves.toBeUndefined();
    expect(transactions()).toHaveLength(1);
  });

  it('gives up with a 503 after the attempts run out', async () => {
    serveTail(() => 0);
    ddb
      .on(TransactWriteCommand)
      .rejects(cancelled(['None', 'ConditionalCheckFailed', 'None']));
    ddb.on(GetCommand).resolves({});

    await expect(
      streamer.streams.write(runId, 'out', 'x')
    ).rejects.toMatchObject({ name: 'WorkflowWorldError', status: 503 });
    expect(transactions()).toHaveLength(5);
  });

  it('surfaces any other DynamoDB error', async () => {
    serveTail(() => undefined);
    ddb.on(TransactWriteCommand).rejects(new Error('AccessDenied'));

    await expect(streamer.streams.write(runId, 'out', 'x')).rejects.toThrow(
      'AccessDenied'
    );
    expect(transactions()).toHaveLength(1);
  });

  it('splits a large writeMulti into transactions of at most 98 chunks, in order', async () => {
    serveTail(() => undefined);
    ddb.on(TransactWriteCommand).resolves({});

    await streamer.streams.writeMulti?.(
      runId,
      'out',
      Array.from({ length: 200 }, (_, i) => `chunk${i}`)
    );

    const puts = transactions().map(chunkPuts);
    expect(puts.map((each) => each.length)).toEqual([98, 98, 4]);
    expect(puts.flat()).toEqual(
      Array.from({ length: 200 }, (_, i) => chunkSk(i))
    );
    for (const input of transactions()) {
      expect(input.TransactItems?.length).toBeLessThanOrEqual(100);
    }
  });

  it('keeps every transaction under 4 MB', async () => {
    serveTail(() => undefined);
    ddb.on(TransactWriteCommand).resolves({});

    // Fourteen chunks of 300 KB: 4.1 MB of data in all.
    await streamer.streams.writeMulti?.(
      runId,
      'out',
      Array.from({ length: 14 }, () => new Uint8Array(300 * 1024))
    );

    const groups = transactions().map(chunkPuts);
    expect(groups.length).toBeGreaterThan(1);
    expect(groups.flat()).toEqual(
      Array.from({ length: 14 }, (_, i) => chunkSk(i))
    );
    for (const input of transactions()) {
      const bytes = (input.TransactItems ?? []).reduce(
        (sum, item) =>
          sum +
          ((item.Put?.Item?.data as Uint8Array | undefined)?.byteLength ?? 0),
        0
      );
      expect(bytes).toBeLessThan(4 * 1024 * 1024);
    }
  });

  it('runs writes and closes of one stream in call order', async () => {
    serveTail(() => undefined);
    const order: string[] = [];
    ddb
      .on(TransactWriteCommand)
      .callsFake((input: TransactWriteCommandInput) => {
        const first = input.TransactItems?.[0];
        order.push(
          first?.Put?.Item?.SK === 'CLOSED'
            ? 'close'
            : String(
                Buffer.from(
                  input.TransactItems?.[1]?.Put?.Item?.data as Uint8Array
                )
              )
        );
        return {};
      });
    let resolveRunId!: (id: string) => void;
    const late = new Promise<string>((resolve) => (resolveRunId = resolve));

    const writes = [
      streamer.streams.write(late as unknown as string, 'out', 'first'),
      streamer.streams.write(runId, 'out', 'second'),
      streamer.streams.close(runId, 'out'),
    ];
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(order).toEqual([]);
    resolveRunId(runId);
    await Promise.all(writes);

    expect(order).toEqual(['first', 'second', 'close']);
  });
});

describe('close', () => {
  it('records the chunk count, on the condition that the next position is free', async () => {
    serveTail(() => 2);
    ddb.on(TransactWriteCommand).resolves({});

    await streamer.streams.close(runId, 'out');

    const [input] = transactions();
    expect(input?.TransactItems?.[0]?.Put).toMatchObject({
      Item: { PK: pk, SK: 'CLOSED', count: 3, runId },
      ConditionExpression: 'attribute_not_exists(PK)',
    });
    expect(input?.TransactItems?.[1]?.ConditionCheck).toMatchObject({
      Key: { PK: pk, SK: chunkSk(3) },
      ConditionExpression: 'attribute_not_exists(PK)',
    });
  });

  it('does nothing when the stream is closed already', async () => {
    serveTail(() => 2);
    ddb
      .on(TransactWriteCommand)
      .rejects(cancelled(['ConditionalCheckFailed', 'None', 'None']));
    ddb.on(GetCommand).resolves({ Item: { PK: pk, SK: 'CLOSED', count: 3 } });

    await expect(streamer.streams.close(runId, 'out')).resolves.toBeUndefined();
    expect(transactions()).toHaveLength(1);
  });

  it('counts a chunk that landed meanwhile', async () => {
    let tail = 2;
    serveTail(() => tail);
    ddb
      .on(TransactWriteCommand)
      .callsFakeOnce(() => {
        tail = 3;
        throw cancelled(['None', 'ConditionalCheckFailed', 'None']);
      })
      .resolves({});
    ddb.on(GetCommand).resolves({});

    await streamer.streams.close(runId, 'out');

    expect(transactions()[1]?.TransactItems?.[0]?.Put?.Item?.count).toBe(4);
  });
});

describe('readers', () => {
  /** Serves the reader's query from `items` (beyond the given start). */
  function serveReads(items: () => Record<string, unknown>[]) {
    ddb.on(QueryCommand).callsFake((input: QueryCommandInput) => {
      const from = input.ExpressionAttributeValues?.[':from'] as string;
      return {
        Items: items().filter((item) => String(item.SK) >= (from ?? '')),
      };
    });
  }
  const chunk = (seq: number, text: string) => ({
    PK: pk,
    SK: chunkSk(seq),
    data: new TextEncoder().encode(text),
  });

  it('polls until the stream closes, then ends', async () => {
    const items: Record<string, unknown>[] = [chunk(0, 'a')];
    serveReads(() => items);
    const reader = (await streamer.streams.get(runId, 'out')).getReader();

    expect(Buffer.from((await reader.read()).value ?? []).toString()).toBe('a');
    const next = reader.read();
    await new Promise((resolve) => setTimeout(resolve, POLL_MS * 3));
    items.push(chunk(1, 'b'), { PK: pk, SK: 'CLOSED', count: 2 });
    expect(Buffer.from((await next).value ?? []).toString()).toBe('b');
    expect(await reader.read()).toEqual({ done: true, value: undefined });
    const polls = ddb.commandCalls(QueryCommand).length;
    expect(polls).toBeGreaterThan(2);
    await new Promise((resolve) => setTimeout(resolve, POLL_MS * 3));
    expect(ddb.commandCalls(QueryCommand)).toHaveLength(polls);
  });

  it('stops polling and aborts the query in flight when cancelled', async () => {
    ddb
      .on(QueryCommand)
      .callsFake(
        async () =>
          new Promise((resolve) => setTimeout(() => resolve({ Items: [] }), 50))
      );
    const send = vi.spyOn(documents, 'send');
    const reader = (await streamer.streams.get(runId, 'out')).getReader();
    const pending = reader.read();
    await vi.waitFor(() => expect(send).toHaveBeenCalled());
    const { abortSignal: signal } = (send.mock.calls[0]?.[1] ?? {}) as {
      abortSignal?: AbortSignal;
    };

    await reader.cancel();
    await expect(pending).resolves.toEqual({ done: true, value: undefined });
    expect(signal?.aborted).toBe(true);
    const polls = ddb.commandCalls(QueryCommand).length;
    await new Promise((resolve) => setTimeout(resolve, POLL_MS * 5));
    expect(ddb.commandCalls(QueryCommand)).toHaveLength(polls);
    send.mockRestore();
  });

  it('fails the reader when a read fails, and stops', async () => {
    ddb.on(QueryCommand).rejects(new Error('initial query failed'));
    for (let i = 0; i < 12; i++) {
      const reader = (await streamer.streams.get(runId, 'out')).getReader();
      await expect(reader.read()).rejects.toThrow('initial query failed');
    }
    const polls = ddb.commandCalls(QueryCommand).length;
    await new Promise((resolve) => setTimeout(resolve, POLL_MS * 3));
    expect(ddb.commandCalls(QueryCommand)).toHaveLength(polls);
  });

  it('resolves a negative start index against the stream as it is', async () => {
    const items: Record<string, unknown>[] = [
      chunk(0, 'a'),
      chunk(1, 'b'),
      chunk(2, 'c'),
    ];
    ddb.on(GetCommand).resolves({});
    ddb.on(QueryCommand).callsFake((input: QueryCommandInput) => {
      if (input.ScanIndexForward === false) {
        return { Items: [{ SK: chunkSk(2) }] };
      }
      const from = input.ExpressionAttributeValues?.[':from'] as string;
      return { Items: items.filter((item) => String(item.SK) >= from) };
    });

    const reader = (await streamer.streams.get(runId, 'out', -2)).getReader();
    expect(Buffer.from((await reader.read()).value ?? []).toString()).toBe('b');
    await reader.cancel();
  });
});

describe('getChunks', () => {
  it('pages with an opaque cursor and clamps the limit', async () => {
    ddb.on(GetCommand).callsFake((input: GetCommandInput) => ({
      Item: input.Key?.SK === 'CLOSED' ? { count: 3 } : undefined,
    }));
    ddb.on(QueryCommand).callsFake((input: QueryCommandInput) => {
      const from = input.ExpressionAttributeValues?.[':from'] as string;
      return {
        Items: [0, 1, 2]
          .map((seq) => ({
            PK: pk,
            SK: chunkSk(seq),
            data: new Uint8Array([seq]),
          }))
          .filter((item) => item.SK >= from)
          .slice(0, input.Limit),
      };
    });

    const first = await streamer.streams.getChunks(runId, 'out', { limit: 0 });
    expect(first.data.map((chunk) => chunk.index)).toEqual([0]);
    expect(first.hasMore).toBe(true);
    expect(first.done).toBe(false);
    expect(first.cursor).not.toContain('CHUNK');

    const rest = await streamer.streams.getChunks(runId, 'out', {
      cursor: first.cursor ?? undefined,
      limit: 5_000,
    });
    expect(rest.data.map((chunk) => chunk.index)).toEqual([1, 2]);
    expect(rest).toMatchObject({ hasMore: false, cursor: null, done: true });
    expect(
      ddb.commandCalls(QueryCommand).at(-1)?.args[0].input.Limit
    ).toBeLessThanOrEqual(1_001);
  });
});
