/**
 * One table per test file, and the plumbing the storage suites need around
 * it: a World over the table, a second World over the same table (a second
 * process), a truncate between tests, and hooks that run code just before a
 * transaction is sent or make it fail, which is how the suites reproduce
 * races and crashes that world-postgres reproduces with row locks and
 * triggers.
 *
 * moto_server runs every request on its own thread and its
 * TransactWriteItems is not isolated from concurrent requests (it snapshots
 * and restores the whole table around each transaction, without a lock).
 * Against moto, every DynamoDB request of the suite is therefore sent one
 * at a time. Callers still interleave freely between requests, which is the
 * concurrency the World has to handle: DynamoDB itself executes each request
 * atomically. Against LocalStack or DynamoDB the requests run concurrently.
 */
import { randomUUID } from 'node:crypto';
import {
  DeleteTableCommand,
  ScanCommand,
  type ScanCommandOutput,
} from '@aws-sdk/client-dynamodb';
import { BatchWriteCommand } from '@aws-sdk/lib-dynamodb';
import type { World } from '@workflow/world';
import { inject } from 'vitest';
import { createWorkflowTable } from '../../scripts/bootstrap-local.js';
import {
  type AwsClients,
  createClients,
  destroyClients,
} from '../../src/clients.js';
import { createWorld } from '../../src/index.js';

const aws = inject('aws');

/** Plain JS input of a DynamoDB command, before the document marshaller. */
export type CommandInput = Record<string, unknown>;

interface Interceptor {
  match: (commandName: string, input: CommandInput) => boolean;
  run: (input: CommandInput) => Promise<void>;
  once: boolean;
}

// Shared by every client of the process: moto must see one request at a time.
let tail: Promise<unknown> = Promise.resolve();
function serialized<T>(fn: () => Promise<T>): Promise<T> {
  const result = tail.then(fn, fn);
  tail = result.catch(() => {});
  return result;
}

const interceptors = new Set<Interceptor>();

function instrument(clients: AwsClients): AwsClients {
  clients.dynamodb.middlewareStack.add(
    (next, context) => async (args) => {
      const input = args.input as CommandInput;
      for (const interceptor of [...interceptors]) {
        if (interceptor.match(context.commandName ?? '', input)) {
          if (interceptor.once) interceptors.delete(interceptor);
          // Outside the serialized section: the hook may send requests.
          await interceptor.run(input);
        }
      }
      return aws.moto ? serialized(() => next(args)) : next(args);
    },
    { step: 'initialize', name: 'storageTestInterceptor' }
  );
  return clients;
}

/** The items a TransactWriteItems input puts, with their operation. */
export function transactionItems(
  input: CommandInput
): { op: string; item: Record<string, unknown> }[] {
  const items = (input.TransactItems ?? []) as Record<
    string,
    { Item?: Record<string, unknown>; Key?: Record<string, unknown> }
  >[];
  return items.map((entry) => {
    const [op, body] = Object.entries(entry)[0] ?? ['', {}];
    return { op, item: body?.Item ?? body?.Key ?? {} };
  });
}

/** Whether a TransactWriteItems input appends an event matching `match`. */
export function appendsEvent(
  input: CommandInput,
  match: (event: Record<string, unknown>) => boolean
): boolean {
  return transactionItems(input).some(
    ({ op, item }) =>
      op === 'Put' &&
      typeof item.SK === 'string' &&
      item.SK.startsWith('EVT#') &&
      match(item)
  );
}

export interface StorageTestContext {
  world: World;
  clients: AwsClients;
  tableName: string;
  /** A new World over the same table, as another process would have. */
  secondWorld(options?: { bucket?: string }): World;
  /** Deletes every item, like world-postgres' TRUNCATE between tests. */
  truncate(): Promise<void>;
  /**
   * Runs `fn` right before the next TransactWriteItems whose input matches,
   * after the World has made every read the write was decided on.
   */
  beforeNextTransaction(
    match: (input: CommandInput) => boolean,
    fn: () => Promise<void>
  ): void;
  /**
   * Makes every TransactWriteItems whose input matches fail with `message`
   * before it reaches DynamoDB, standing in for a process that dies before
   * its write commits. Returns a function that stops it.
   */
  failTransactions(
    match: (input: CommandInput) => boolean,
    message: string
  ): () => void;
  close(): Promise<void>;
}

/** Bucket and key of an `s3://bucket/key` reference. */
export function s3Location(ref: string): { bucket: string; key: string } {
  const match = /^s3:\/\/([^/]+)\/(.+)$/.exec(ref);
  if (!match) throw new Error(`Not an S3 reference: ${ref}`);
  return { bucket: match[1] as string, key: match[2] as string };
}

/** How often the suites' live stream readers poll: short, for speed. */
export const TEST_STREAM_POLL_INTERVAL_MS = 20;

const baseOptions = () => ({
  region: aws.region,
  endpoint: aws.endpoint,
  workflowQueueUrl: aws.workflowQueueUrl,
  stepQueueUrl: aws.stepQueueUrl,
  bucket: aws.bucket,
  streamPollIntervalMs: TEST_STREAM_POLL_INTERVAL_MS,
});

export async function createStorageTestContext(
  suite: string
): Promise<StorageTestContext> {
  const tableName = `wf-${suite}-${randomUUID().slice(0, 8)}`;
  const owned: AwsClients[] = [];
  const newClients = () => {
    const clients = instrument(
      createClients({ region: aws.region, endpoint: aws.endpoint })
    );
    owned.push(clients);
    return clients;
  };
  const clients = newClients();
  await createWorkflowTable(clients, tableName);
  const newWorld = (
    worldClients: AwsClients,
    overrides: { bucket?: string } = {}
  ) =>
    createWorld({
      ...baseOptions(),
      ...overrides,
      tableName,
      clients: worldClients,
    });
  const world = newWorld(clients);

  return {
    world,
    clients,
    tableName,
    secondWorld: (overrides) => newWorld(newClients(), overrides),
    async truncate() {
      let startKey: ScanCommandOutput['LastEvaluatedKey'];
      do {
        const page = await clients.dynamodb.send(
          new ScanCommand({
            TableName: tableName,
            ProjectionExpression: 'PK, SK',
            ExclusiveStartKey: startKey,
          })
        );
        const keys = (page.Items ?? []).map((item) => ({
          PK: item.PK?.S,
          SK: item.SK?.S,
        }));
        for (let i = 0; i < keys.length; i += 25) {
          await clients.documents.send(
            new BatchWriteCommand({
              RequestItems: {
                [tableName]: keys
                  .slice(i, i + 25)
                  .map((Key) => ({ DeleteRequest: { Key } })),
              },
            })
          );
        }
        startKey = page.LastEvaluatedKey;
      } while (startKey);
    },
    beforeNextTransaction(match, fn) {
      interceptors.add({
        match: (name, input) =>
          name === 'TransactWriteItemsCommand' && match(input),
        run: fn,
        once: true,
      });
    },
    failTransactions(match, message) {
      const interceptor: Interceptor = {
        match: (name, input) =>
          name === 'TransactWriteItemsCommand' && match(input),
        run: async () => {
          throw new Error(message);
        },
        once: false,
      };
      interceptors.add(interceptor);
      return () => interceptors.delete(interceptor);
    },
    async close() {
      interceptors.clear();
      await clients.dynamodb.send(
        new DeleteTableCommand({ TableName: tableName })
      );
      for (const each of owned) destroyClients(each);
    },
  };
}
