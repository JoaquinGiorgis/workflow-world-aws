import {
  BatchGetCommand,
  DeleteCommand,
  type DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  type QueryCommandInput,
  type QueryCommandOutput,
  TransactWriteCommand,
  type TransactWriteCommandInput,
} from '@aws-sdk/lib-dynamodb';
import type { TableKey } from '../keys.js';
import {
  MAX_ITEM_BYTES,
  MAX_TRANSACTION_BYTES,
  MissingTableIndexError,
  PayloadTooLargeError,
  TRANSACTION_MARGIN_BYTES,
} from './errors.js';
import type { Item } from './items.js';
import { debugLog } from './log.js';
import {
  isS3Ref,
  objectKeyPrefix,
  type PayloadStore,
  type PutOptions,
  type S3Ref,
  storedForm,
} from './payloads.js';
import { sizeOf } from './values.js';

type Expression = {
  ConditionExpression?: string;
  ExpressionAttributeNames?: Record<string, string>;
  ExpressionAttributeValues?: Record<string, unknown>;
  ReturnValuesOnConditionCheckFailure?: 'ALL_OLD' | 'NONE';
};

/**
 * What an update does to its item, so values that would not fit can be
 * moved to S3: the item as it will be after the update, and the value
 * placeholder each attribute the update sets is written from. Not sent.
 */
export interface UpdateOffload {
  item: Item;
  values: Record<string, string>;
}

/** One write of a transaction; the table name is added when it is sent. */
export type TransactItem =
  | { Put: Expression & { Item: Item } }
  | {
      Update: Expression & {
        Key: TableKey;
        UpdateExpression: string;
        offload?: UpdateOffload;
      };
    }
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

/**
 * Attributes that stay in the item whatever their size: keys, counters and
 * everything a condition, filter or index reads.
 */
const PINNED = new Set([
  'PK',
  'SK',
  'GSI1PK',
  'GSI1SK',
  'GSI2PK',
  'GSI2SK',
  'GSI3PK',
  'GSI3SK',
  'type',
  'seq',
  'seqInBand',
  'rev',
  'expiresAt',
  'runId',
  'stepId',
  'hookId',
  'eventId',
  'eventType',
  'correlationId',
  'status',
  'token',
  'resumeId',
  'resumePayloadDigest',
  'workflowName',
  'deploymentId',
  'specVersion',
  'createdAt',
  'updatedAt',
]);

/** Smaller values are never worth an S3 object. */
const MIN_OFFLOAD_BYTES = 1024;

export type QueryInput = Omit<QueryCommandInput, 'TableName'>;

/**
 * One write being prepared: the stored form of its item, and how to put a
 * reference in place of one of its attributes.
 */
interface Pending {
  item: Item;
  /** Bytes it adds to the transaction. */
  bytes: () => number;
  replace: (attribute: string, ref: S3Ref) => void;
}

/**
 * The World's one table, read consistently. With a {@link PayloadStore},
 * values that do not fit an item (or a transaction) are written to S3 and
 * read back transparently; see `payloads.ts`.
 */
export class Table {
  constructor(
    readonly documents: DynamoDBDocumentClient,
    readonly name: string,
    readonly payloads?: PayloadStore
  ) {}

  async get(key: TableKey): Promise<Item | undefined> {
    const result = await this.documents.send(
      new GetCommand({ TableName: this.name, Key: key, ConsistentRead: true })
    );
    return result.Item && this.resolve(result.Item);
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
    return Promise.all(items.map((item) => this.resolve(item)));
  }

  /**
   * Writes `item` whole. Objects in S3 that the item it replaced (or this
   * one) referenced and this one no longer does are deleted; so the values
   * it moves to S3 get objects of their own (`unique`), which no other
   * write shares.
   */
  async put(item: Item): Promise<void> {
    const [entry] = await this.prepare([{ Put: { Item: item } }], {
      unique: true,
    });
    const stored = (entry as { Put: { Item: Item } }).Put.Item;
    const result = await this.documents.send(
      new PutCommand({
        TableName: this.name,
        Item: stored,
        ...(this.payloads && { ReturnValues: 'ALL_OLD' }),
      })
    );
    await this.deleteUnreferenced(result.Attributes, stored);
  }

  async delete(key: TableKey): Promise<void> {
    const result = await this.documents.send(
      new DeleteCommand({
        TableName: this.name,
        Key: key,
        ...(this.payloads && { ReturnValues: 'ALL_OLD' }),
      })
    );
    await this.deleteUnreferenced(result.Attributes, {});
  }

  /** One page of a query. */
  async queryPage(
    input: QueryInput,
    options: { signal?: AbortSignal } = {}
  ): Promise<{ items: Item[]; lastKey?: Record<string, unknown> }> {
    let result: QueryCommandOutput;
    try {
      result = await this.documents.send(
        new QueryCommand({ TableName: this.name, ...input }),
        options.signal ? { abortSignal: options.signal } : {}
      );
    } catch (error) {
      if (input.IndexName && isMissingIndex(error, input.IndexName)) {
        throw new MissingTableIndexError(this.name, input.IndexName, error);
      }
      throw error;
    }
    const items = await Promise.all(
      (result.Items ?? []).map((item) => this.resolve(item))
    );
    return { items, lastKey: result.LastEvaluatedKey };
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

  /**
   * One transaction. `uploaded` is shared by the attempts of one write, so
   * a retry does not upload its values to S3 again (see `PutOptions`).
   */
  async transact(
    items: TransactItem[],
    options: Pick<PutOptions, 'uploaded'> = {}
  ): Promise<void> {
    const prepared = await this.prepare(items, options);
    await this.documents.send(
      new TransactWriteCommand({
        TransactItems: prepared.map((entry) =>
          withTable(entry, this.name)
        ) as TransactWriteCommandInput['TransactItems'],
      })
    );
  }

  /** `item` with every value kept in S3 read back. */
  async resolve(item: Item): Promise<Item> {
    if (!this.payloads) {
      if (Object.values(item).some(isS3Ref)) {
        throw new Error(
          'An item references a payload in S3, but the table has no payload store'
        );
      }
      return item;
    }
    return this.payloads.resolve(item);
  }

  /**
   * The writes as they are sent: every item in its stored form, with the
   * values that keep an item over {@link MAX_ITEM_BYTES}, or the
   * transaction over {@link MAX_TRANSACTION_BYTES} less
   * {@link TRANSACTION_MARGIN_BYTES}, written to S3 (largest first) and
   * referenced instead. Without a bucket, a transaction within the margin
   * is sent as it is, for DynamoDB to decide.
   *
   * @throws {PayloadTooLargeError} when that is not possible: no bucket, or
   *   what is left does not fit either.
   */
  private async prepare(
    items: TransactItem[],
    options: PutOptions = {}
  ): Promise<object[]> {
    const out: object[] = [];
    const pending: Pending[] = [];
    for (const entry of items) {
      if ('Put' in entry) {
        const put = { ...entry.Put, Item: storedForm(entry.Put.Item) };
        out.push({ Put: put });
        pending.push({
          get item() {
            return put.Item;
          },
          bytes: () => sizeOf(put.Item),
          replace: (attribute, ref) => {
            put.Item = { ...put.Item, [attribute]: ref };
          },
        });
      } else if ('Update' in entry) {
        const { offload, ...update } = entry.Update;
        const body = {
          ...update,
          ExpressionAttributeNames: { ...update.ExpressionAttributeNames },
          ExpressionAttributeValues: { ...update.ExpressionAttributeValues },
        };
        out.push({ Update: body });
        if (!offload) continue;
        const projected = storedForm(offload.item);
        // A value the update sets again unchanged keeps its reference.
        for (const [attribute, placeholder] of Object.entries(offload.values)) {
          if (isS3Ref(projected[attribute])) {
            body.ExpressionAttributeValues[placeholder] = projected[attribute];
          }
        }
        const view = { item: projected };
        let added = 0;
        pending.push({
          get item() {
            return view.item;
          },
          bytes: () => sizeOf(body.ExpressionAttributeValues),
          replace: (attribute, ref) => {
            view.item = { ...view.item, [attribute]: ref };
            const placeholder = offload.values[attribute];
            if (placeholder) {
              body.ExpressionAttributeValues[placeholder] = ref;
              return;
            }
            // An attribute the item already holds: set it to the reference.
            const name = `#o${added}`;
            const value = `:o${added}`;
            added++;
            body.ExpressionAttributeNames[name] = attribute;
            body.ExpressionAttributeValues[value] = ref;
            body.UpdateExpression = body.UpdateExpression.replace(
              /^SET /,
              `SET ${name} = ${value}, `
            );
          },
        });
      } else {
        out.push(entry);
      }
    }

    for (const each of pending) {
      let size = sizeOf(each.item);
      while (size > MAX_ITEM_BYTES) {
        const attribute = this.largestOffloadable([each])?.attribute;
        if (attribute === undefined || !this.payloads?.enabled) {
          throw new PayloadTooLargeError(describeItem(each.item), size, {
            offloaded: this.payloads?.enabled === true,
          });
        }
        await this.offload(each, attribute, options);
        size = sizeOf(each.item);
      }
    }

    const total = () => pending.reduce((sum, each) => sum + each.bytes(), 0);
    let bytes = total();
    while (bytes > MAX_TRANSACTION_BYTES - TRANSACTION_MARGIN_BYTES) {
      const largest = this.largestOffloadable(pending);
      if (largest === undefined || !this.payloads?.enabled) {
        if (bytes <= MAX_TRANSACTION_BYTES) break;
        throw new PayloadTooLargeError('The transaction', bytes, {
          limit: MAX_TRANSACTION_BYTES,
          offloaded: this.payloads?.enabled === true,
        });
      }
      await this.offload(largest.pending, largest.attribute, options);
      bytes = total();
    }
    return out;
  }

  private largestOffloadable(
    pending: Pending[]
  ): { pending: Pending; attribute: string } | undefined {
    let best: { pending: Pending; attribute: string; size: number } | undefined;
    for (const each of pending) {
      for (const [attribute, value] of Object.entries(each.item)) {
        if (PINNED.has(attribute) || isS3Ref(value)) continue;
        const size = sizeOf(value);
        if (size < MIN_OFFLOAD_BYTES) continue;
        if (!best || size > best.size)
          best = { pending: each, attribute, size };
      }
    }
    return best;
  }

  private async offload(
    each: Pending,
    attribute: string,
    options: PutOptions
  ): Promise<void> {
    const payloads = this.payloads as PayloadStore;
    const ref = await payloads.put(
      objectKeyPrefix(each.item, attribute),
      each.item[attribute],
      options
    );
    each.replace(attribute, ref);
  }

  /**
   * Deletes the objects `old` referenced that `current` does not: they
   * belonged to the item's previous version. Best effort; a leftover object
   * is only storage (see the README's bucket lifecycle advice).
   */
  private async deleteUnreferenced(
    old: Record<string, unknown> | undefined,
    current: Item
  ): Promise<void> {
    if (!old || !this.payloads) return;
    const kept = new Set(
      Object.values(current)
        .filter(isS3Ref)
        .map((ref) => ref.s3Ref)
    );
    for (const value of Object.values(old)) {
      if (!isS3Ref(value) || kept.has(value.s3Ref)) continue;
      try {
        await this.payloads.delete(value);
      } catch (error) {
        debugLog('could not delete a replaced payload object', {
          ref: value.s3Ref,
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }
}

/**
 * Whether a query failed because the table has no index `indexName`:
 * DynamoDB answers `ValidationException` ("The table does not have the
 * specified index"), moto and LocalStack `ResourceNotFoundException`.
 */
function isMissingIndex(error: unknown, indexName: string): boolean {
  const { name, message } = (error ?? {}) as {
    name?: string;
    message?: string;
  };
  return (
    (name === 'ValidationException' || name === 'ResourceNotFoundException') &&
    typeof message === 'string' &&
    /index/i.test(message) &&
    message.includes(indexName)
  );
}

function withTable(entry: object, tableName: string): object {
  const [op, body] = Object.entries(entry)[0] as [string, object];
  return { [op]: { TableName: tableName, ...body } };
}

function describeItem(item: Item): string {
  return typeof item.SK === 'string'
    ? `The ${item.SK.split('#')[0]?.toLowerCase()} item for ${String(item.PK)}`
    : 'An item';
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
