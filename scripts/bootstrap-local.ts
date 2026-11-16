/**
 * Creates the AWS resources the World needs on a local emulator (moto_server
 * or LocalStack): the DynamoDB table with its GSIs and TTL, the workflow and
 * step SQS queues with their dead-letter queues, and the payload bucket.
 *
 * Safe to run more than once: resources that already exist are kept.
 *
 *   WORKFLOW_AWS_ENDPOINT=http://localhost:4566 pnpm bootstrap:local
 *
 * It refuses to run without WORKFLOW_AWS_ENDPOINT so that it never creates
 * resources in a real AWS account by accident.
 */
import { setTimeout as sleep } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';
import {
  type AttributeValue,
  ConditionalCheckFailedException,
  CreateTableCommand,
  DescribeTableCommand,
  ResourceInUseException,
  ScanCommand,
  UpdateItemCommand,
  UpdateTableCommand,
  UpdateTimeToLiveCommand,
  waitUntilTableExists,
} from '@aws-sdk/client-dynamodb';
import {
  BucketAlreadyOwnedByYou,
  type BucketLocationConstraint,
  CreateBucketCommand,
  type CreateBucketCommandInput,
} from '@aws-sdk/client-s3';
import {
  CreateQueueCommand,
  GetQueueAttributesCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { TERMINAL_WORKFLOW_RUN_STATUSES } from '@workflow/world';
import {
  type AwsClients,
  createClients,
  destroyClients,
} from '../src/clients.js';
import {
  ACTIVE_RUNS_GSI3PK,
  GSI1_NAME,
  GSI2_NAME,
  GSI3_NAME,
  RUN_META_SK,
} from '../src/keys.js';
import { DEFAULT_MAX_RECEIVE_COUNT } from '../src/queue/index.js';

/** SQS's longest retention, 14 days, for both queues and dead letters. */
const MESSAGE_RETENTION_SECONDS = '1209600';

/** Attribute the table's TTL reads (epoch seconds). */
export const TTL_ATTRIBUTE = 'expiresAt';

export interface BootstrapOptions {
  region: string;
  endpoint: string;
  tableName: string;
  workflowQueueName: string;
  stepQueueName: string;
  bucket: string;
  /**
   * Receives before SQS moves a message to the dead-letter queue. Default
   * {@link DEFAULT_MAX_RECEIVE_COUNT} (73): a failed delivery is retried
   * with a backoff of up to an hour, for days, as world-postgres retries a
   * job, so the runtime's own delivery cap (49) is reached first.
   */
  maxReceiveCount?: number;
  visibilityTimeoutSeconds?: number;
  /** Clients to use instead of creating (and destroying) new ones. */
  clients?: AwsClients;
}

export interface BootstrapResult {
  tableName: string;
  workflowQueueUrl: string;
  workflowDeadLetterQueueUrl: string;
  stepQueueUrl: string;
  stepDeadLetterQueueUrl: string;
  bucket: string;
}

export async function bootstrapLocal(
  options: BootstrapOptions
): Promise<BootstrapResult> {
  const clients =
    options.clients ??
    createClients({ region: options.region, endpoint: options.endpoint });
  try {
    await createWorkflowTable(clients, options.tableName);

    const maxReceiveCount =
      options.maxReceiveCount ?? DEFAULT_MAX_RECEIVE_COUNT;
    const visibilityTimeoutSeconds = options.visibilityTimeoutSeconds ?? 60;
    const workflow = await createQueueWithDeadLetter(
      clients.sqs,
      options.workflowQueueName,
      maxReceiveCount,
      visibilityTimeoutSeconds
    );
    const step = await createQueueWithDeadLetter(
      clients.sqs,
      options.stepQueueName,
      maxReceiveCount,
      visibilityTimeoutSeconds
    );

    await createBucket(clients, options.bucket, options.region);

    return {
      tableName: options.tableName,
      workflowQueueUrl: workflow.queueUrl,
      workflowDeadLetterQueueUrl: workflow.deadLetterQueueUrl,
      stepQueueUrl: step.queueUrl,
      stepDeadLetterQueueUrl: step.deadLetterQueueUrl,
      bucket: options.bucket,
    };
  } finally {
    if (!options.clients) destroyClients(clients);
  }
}

/**
 * How long the bootstrap waits for a new GSI to become ACTIVE. On a table
 * that already holds many items the backfill can take longer; the script
 * then fails after `UpdateTable` was accepted, and running it again is the
 * way on: it finds the index (CREATING or ACTIVE), waits for it again and
 * backfills. It only runs against an emulator, so in practice this is
 * about large local tables.
 */
const INDEX_WAIT_MS = 5 * 60_000;

/**
 * Creates the World's table: PK/SK, GSI1 (every run and every hook, by id),
 * GSI2 (runs per workflow name) and the sparse GSI3 (runs that have not
 * ended), on-demand billing, and TTL on {@link TTL_ATTRIBUTE}. Keeps a table
 * that already exists, adding GSI3 to one created before it
 * ({@link addActiveRunIndex}).
 */
export async function createWorkflowTable(
  clients: AwsClients,
  tableName: string
): Promise<void> {
  try {
    await clients.dynamodb.send(
      new CreateTableCommand({
        TableName: tableName,
        BillingMode: 'PAY_PER_REQUEST',
        AttributeDefinitions: [
          { AttributeName: 'PK', AttributeType: 'S' },
          { AttributeName: 'SK', AttributeType: 'S' },
          { AttributeName: 'GSI1PK', AttributeType: 'S' },
          { AttributeName: 'GSI1SK', AttributeType: 'S' },
          { AttributeName: 'GSI2PK', AttributeType: 'S' },
          { AttributeName: 'GSI2SK', AttributeType: 'S' },
          { AttributeName: 'GSI3PK', AttributeType: 'S' },
          { AttributeName: 'GSI3SK', AttributeType: 'S' },
        ],
        KeySchema: [
          { AttributeName: 'PK', KeyType: 'HASH' },
          { AttributeName: 'SK', KeyType: 'RANGE' },
        ],
        GlobalSecondaryIndexes: [
          {
            IndexName: GSI1_NAME,
            KeySchema: [
              { AttributeName: 'GSI1PK', KeyType: 'HASH' },
              { AttributeName: 'GSI1SK', KeyType: 'RANGE' },
            ],
            Projection: { ProjectionType: 'ALL' },
          },
          {
            IndexName: GSI2_NAME,
            KeySchema: [
              { AttributeName: 'GSI2PK', KeyType: 'HASH' },
              { AttributeName: 'GSI2SK', KeyType: 'RANGE' },
            ],
            Projection: { ProjectionType: 'ALL' },
          },
          {
            IndexName: GSI3_NAME,
            KeySchema: [
              { AttributeName: 'GSI3PK', KeyType: 'HASH' },
              { AttributeName: 'GSI3SK', KeyType: 'RANGE' },
            ],
            Projection: { ProjectionType: 'ALL' },
          },
        ],
      })
    );
  } catch (error) {
    if (!(error instanceof ResourceInUseException)) throw error;
  }

  await waitUntilTableExists(
    { client: clients.dynamodb, maxWaitTime: 60, minDelay: 1, maxDelay: 2 },
    { TableName: tableName }
  );
  await addActiveRunIndex(clients, tableName);

  try {
    await clients.dynamodb.send(
      new UpdateTimeToLiveCommand({
        TableName: tableName,
        TimeToLiveSpecification: {
          AttributeName: TTL_ATTRIBUTE,
          Enabled: true,
        },
      })
    );
  } catch (error) {
    // DynamoDB refuses to enable TTL twice; that is the state we want.
    if (!/already enabled/i.test((error as Error).message ?? '')) throw error;
  }
}

/**
 * Adds GSI3 (the runs that have not ended) to a table created without it,
 * by an older bootstrap: `UpdateTable`, a wait until the index is ACTIVE,
 * and {@link backfillActiveRunIndex}. Does nothing when the index exists.
 *
 * @returns whether it added the index.
 */
export async function addActiveRunIndex(
  clients: AwsClients,
  tableName: string
): Promise<boolean> {
  const indexStatus = async () => {
    const { Table } = await clients.dynamodb.send(
      new DescribeTableCommand({ TableName: tableName })
    );
    return Table?.GlobalSecondaryIndexes?.find(
      (index) => index.IndexName === GSI3_NAME
    )?.IndexStatus;
  };
  if ((await indexStatus()) !== undefined) return false;

  await clients.dynamodb.send(
    new UpdateTableCommand({
      TableName: tableName,
      AttributeDefinitions: [
        { AttributeName: 'GSI3PK', AttributeType: 'S' },
        { AttributeName: 'GSI3SK', AttributeType: 'S' },
      ],
      GlobalSecondaryIndexUpdates: [
        {
          Create: {
            IndexName: GSI3_NAME,
            KeySchema: [
              { AttributeName: 'GSI3PK', KeyType: 'HASH' },
              { AttributeName: 'GSI3SK', KeyType: 'RANGE' },
            ],
            Projection: { ProjectionType: 'ALL' },
          },
        },
      ],
    })
  );
  const deadline = Date.now() + INDEX_WAIT_MS;
  while ((await indexStatus()) !== 'ACTIVE') {
    if (Date.now() > deadline) {
      throw new Error(
        `${GSI3_NAME} of table "${tableName}" did not become ACTIVE within ${INDEX_WAIT_MS / 1000} s`
      );
    }
    await sleep(1_000);
  }
  await backfillActiveRunIndex(clients, tableName);
  return true;
}

/**
 * Puts every run that has not ended in GSI3: the one-off step after adding
 * the index to a table that already holds runs (new runs carry the
 * attributes from their creation). A run that ends meanwhile, or got the
 * attributes some other way, is left alone. Safe to run more than once.
 *
 * @returns how many runs it added to the index.
 */
export async function backfillActiveRunIndex(
  clients: AwsClients,
  tableName: string
): Promise<number> {
  const terminal: Record<string, AttributeValue> = {};
  TERMINAL_WORKFLOW_RUN_STATUSES.forEach((status, i) => {
    terminal[`:t${i}`] = { S: status };
  });
  const notEnded = `attribute_not_exists(GSI3PK) AND NOT (#status IN (${Object.keys(terminal).join(', ')}))`;
  let added = 0;
  let startKey: Record<string, AttributeValue> | undefined;
  do {
    const page = await clients.dynamodb.send(
      new ScanCommand({
        TableName: tableName,
        FilterExpression: `SK = :meta AND ${notEnded}`,
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':meta': { S: RUN_META_SK }, ...terminal },
        ProjectionExpression: 'PK, SK, runId',
        ...(startKey && { ExclusiveStartKey: startKey }),
      })
    );
    for (const item of page.Items ?? []) {
      const runId = item.runId?.S;
      if (!item.PK || !item.SK || !runId) continue;
      try {
        await clients.dynamodb.send(
          new UpdateItemCommand({
            TableName: tableName,
            Key: { PK: item.PK, SK: item.SK },
            UpdateExpression: 'SET GSI3PK = :pk, GSI3SK = :runId',
            ConditionExpression: `attribute_exists(PK) AND ${notEnded}`,
            ExpressionAttributeNames: { '#status': 'status' },
            ExpressionAttributeValues: {
              ':pk': { S: ACTIVE_RUNS_GSI3PK },
              ':runId': { S: runId },
              ...terminal,
            },
          })
        );
        added++;
      } catch (error) {
        if (!(error instanceof ConditionalCheckFailedException)) throw error;
      }
    }
    startKey = page.LastEvaluatedKey;
  } while (startKey);
  return added;
}

/**
 * Creates a standard queue and its dead-letter queue (`<name>-dlq`), with a
 * redrive policy after `maxReceiveCount` receives. Keeps queues that exist.
 */
export async function createQueueWithDeadLetter(
  sqs: SQSClient,
  name: string,
  maxReceiveCount: number,
  visibilityTimeoutSeconds: number
): Promise<{ queueUrl: string; deadLetterQueueUrl: string }> {
  const deadLetter = await sqs.send(
    new CreateQueueCommand({
      QueueName: `${name}-dlq`,
      Attributes: { MessageRetentionPeriod: MESSAGE_RETENTION_SECONDS },
    })
  );
  const deadLetterQueueUrl = requireValue(
    deadLetter.QueueUrl,
    `QueueUrl of ${name}-dlq`
  );

  const attributes = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: deadLetterQueueUrl,
      AttributeNames: ['QueueArn'],
    })
  );
  const deadLetterArn = requireValue(
    attributes.Attributes?.QueueArn,
    `QueueArn of ${name}-dlq`
  );

  const queue = await sqs.send(
    new CreateQueueCommand({
      QueueName: name,
      Attributes: {
        VisibilityTimeout: String(visibilityTimeoutSeconds),
        // Retries back off for days; the default four days could drop a
        // message before its retries (or its dead-letter move) are done.
        MessageRetentionPeriod: MESSAGE_RETENTION_SECONDS,
        RedrivePolicy: JSON.stringify({
          deadLetterTargetArn: deadLetterArn,
          maxReceiveCount: String(maxReceiveCount),
        }),
      },
    })
  );

  return {
    queueUrl: requireValue(queue.QueueUrl, `QueueUrl of ${name}`),
    deadLetterQueueUrl,
  };
}

async function createBucket(
  clients: AwsClients,
  bucket: string,
  region: string
): Promise<void> {
  const input: CreateBucketCommandInput = { Bucket: bucket };
  // us-east-1 is the default location and refuses an explicit constraint.
  if (region !== 'us-east-1') {
    input.CreateBucketConfiguration = {
      LocationConstraint: region as BucketLocationConstraint,
    };
  }
  try {
    await clients.s3.send(new CreateBucketCommand(input));
  } catch (error) {
    if (!(error instanceof BucketAlreadyOwnedByYou)) throw error;
  }
}

function requireValue<T>(value: T | undefined, what: string): T {
  if (value === undefined) {
    throw new Error(`The emulator did not return ${what}`);
  }
  return value;
}

/** Options for the CLI, from the same variables the World reads. */
export function optionsFromEnv(
  env: Record<string, string | undefined>
): BootstrapOptions {
  const endpoint = env.WORKFLOW_AWS_ENDPOINT?.trim();
  if (!endpoint) {
    throw new Error(
      'WORKFLOW_AWS_ENDPOINT is required: this script only bootstraps a local emulator such as moto_server or LocalStack (for example http://localhost:4566)'
    );
  }
  return {
    endpoint,
    region: env.WORKFLOW_AWS_REGION?.trim() || 'us-east-1',
    tableName: env.WORKFLOW_AWS_TABLE?.trim() || 'workflow',
    workflowQueueName: 'workflow',
    stepQueueName: 'step',
    bucket: env.WORKFLOW_AWS_BUCKET?.trim() || 'workflow-payloads',
  };
}

async function main(): Promise<void> {
  const options = optionsFromEnv(process.env);
  const result = await bootstrapLocal(options);
  console.log(`# Resources ready at ${options.endpoint}`);
  console.log(`WORKFLOW_AWS_ENDPOINT=${options.endpoint}`);
  console.log(`WORKFLOW_AWS_REGION=${options.region}`);
  console.log(`WORKFLOW_AWS_TABLE=${result.tableName}`);
  console.log(`WORKFLOW_AWS_WORKFLOW_QUEUE_URL=${result.workflowQueueUrl}`);
  console.log(`WORKFLOW_AWS_STEP_QUEUE_URL=${result.stepQueueUrl}`);
  console.log(`WORKFLOW_AWS_BUCKET=${result.bucket}`);
}

if (
  process.argv[1] !== undefined &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  main().catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : error);
    process.exitCode = 1;
  });
}
