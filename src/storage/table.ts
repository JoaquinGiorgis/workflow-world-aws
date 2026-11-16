import {
  BatchGetCommand,
  DeleteCommand,
  type DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  type QueryCommandInput,
  TransactWriteCommand,
  type TransactWriteCommandInput,
} from '@aws-sdk/lib-dynamodb';
import type { TableKey } from '../keys.js';
import {
  MAX_ITEM_BYTES,
  MAX_TRANSACTION_BYTES,
  PayloadTooLargeError,
} from './errors.js';
import type { Item } from './items.js';
import { sizeOf } from './values.js';

type Expression = {
  ConditionExpression?: string;
  ExpressionAttributeNames?: Record<string, string>;
  ExpressionAttributeValues?: Record<string, unknown>;
  ReturnValuesOnConditionCheckFailure?: 'ALL_OLD' | 'NONE';
};

/** One write of a transaction; the table name is added when it is sent. */
export type TransactItem =
  | { Put: Expression & { Item: Item } }
  | { Update: Expression & { Key: TableKey; UpdateExpression: string } }
  | { Delete: Expression & { Key: TableKey } }
  | {
      ConditionCheck: Expression & {
        Key: TableKey;
        ConditionExpression: string;
      };
    };

/** DynamoDB's limit of items in one TransactWriteItems request. */
export const MAX_TRANSACTION_ITEMS = 100;

/** Largest `Limit` DynamoDB accepts (a 32-bit integer). */
const MAX_QUERY_LIMIT = 2 ** 31 - 1;

/** BatchGetItem's limit of keys per request. */
const MAX_BATCH_GET_KEYS = 100;

export type QueryInput = Omit<QueryCommandInput, 'TableName'>;

/** The World's one table, read consistently. */
export class Table {
  constructor(
    readonly documents: DynamoDBDocumentClient,
    readonly name: string
  ) {}

  async get(key: TableKey): Promise<Item | undefined> {
    const result = await this.documents.send(
      new GetCommand({ TableName: this.name, Key: key, ConsistentRead: true })
    );
    return result.Item;
  }

  /** Every item that exists among `keys`, in no particular order. */
  async batchGet(keys: TableKey[]): Promise<Item[]> {
    const items: Item[] = [];
    for (let i = 0; i < keys.length; i += MAX_BATCH_GET_KEYS) {
      let pending: Record<string, unknown>[] = keys
        .slice(i, i + MAX_BATCH_GET_KEYS)
        .map((key) => ({ ...key }));
      for (let attempt = 0; pending.length > 0; attempt++) {
        const result = await this.documents.send(
          new BatchGetCommand({
            RequestItems: {
              [this.name]: { Keys: pending, ConsistentRead: true },
            },
          })
        );
        items.push(...(result.Responses?.[this.name] ?? []));
        pending = result.UnprocessedKeys?.[this.name]?.Keys ?? [];
        if (pending.length > 0) await sleep(Math.min(1_000, 20 * 2 ** attempt));
      }
    }
    return items;
  }

  async put(item: Item): Promise<void> {
    checkItemSize(item);
    await this.documents.send(
      new PutCommand({ TableName: this.name, Item: item })
    );
  }

  async delete(key: TableKey): Promise<void> {
    await this.documents.send(
      new DeleteCommand({ TableName: this.name, Key: key })
    );
  }

  /** One page of a query. */
  async queryPage(
    input: QueryInput
  ): Promise<{ items: Item[]; lastKey?: Record<string, unknown> }> {
    const result = await this.documents.send(
      new QueryCommand({ TableName: this.name, ...input })
    );
    return { items: result.Items ?? [], lastKey: result.LastEvaluatedKey };
  }

  /**
   * Up to `want` items of a query, following DynamoDB's pages. With a
   * filter, DynamoDB applies `Limit` before filtering, so pages are read
   * whole until enough items matched. `Limit` is a 32-bit integer in the
   * DynamoDB API: an unbounded `want` sends none and reads whole pages.
   */
  async collect(input: QueryInput, want: number): Promise<Item[]> {
    const items: Item[] = [];
    let startKey = input.ExclusiveStartKey;
    const filtered = input.FilterExpression !== undefined;
    do {
      const remaining = Math.max(1, want - items.length);
      const page = await this.queryPage({
        ...input,
        ExclusiveStartKey: startKey,
        ...(!filtered && remaining <= MAX_QUERY_LIMIT && { Limit: remaining }),
      });
      items.push(...page.items);
      startKey = page.lastKey;
    } while (startKey && items.length < want);
    return items.slice(0, want);
  }

  async transact(items: TransactItem[]): Promise<void> {
    let total = 0;
    for (const entry of items) {
      if ('Put' in entry) total += checkItemSize(entry.Put.Item);
    }
    if (total > MAX_TRANSACTION_BYTES) {
      throw new PayloadTooLargeError(
        'The transaction',
        total,
        MAX_TRANSACTION_BYTES
      );
    }
    await this.documents.send(
      new TransactWriteCommand({
        TransactItems: items.map((entry) =>
          withTable(entry, this.name)
        ) as TransactWriteCommandInput['TransactItems'],
      })
    );
  }
}

function withTable(entry: TransactItem, tableName: string): object {
  const [op, body] = Object.entries(entry)[0] as [string, object];
  return { [op]: { TableName: tableName, ...body } };
}

/** @returns the item's size. @throws {PayloadTooLargeError} */
function checkItemSize(item: Item): number {
  const size = sizeOf(item);
  if (size > MAX_ITEM_BYTES) {
    const what =
      typeof item.SK === 'string'
        ? `The ${item.SK.split('#')[0]?.toLowerCase()} item for ${String(item.PK)}`
        : 'An item';
    throw new PayloadTooLargeError(what, size);
  }
  return size;
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve();
    const timer = setTimeout(done, ms);
    function done() {
      clearTimeout(timer);
      signal?.removeEventListener('abort', done);
      resolve();
    }
    signal?.addEventListener('abort', done, { once: true });
  });
}
