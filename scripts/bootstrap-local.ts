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
import { pathToFileURL } from 'node:url';
import {
  CreateTableCommand,
  ResourceInUseException,
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
import {
  type AwsClients,
  createClients,
  destroyClients,
} from '../src/clients.js';
import { GSI1_NAME, GSI2_NAME } from '../src/keys.js';

/** Attribute the table's TTL reads (epoch seconds). */
export const TTL_ATTRIBUTE = 'expiresAt';

export interface BootstrapOptions {
  region: string;
  endpoint: string;
  tableName: string;
  workflowQueueName: string;
  stepQueueName: string;
  bucket: string;
  /** Receives before SQS moves a message to the dead-letter queue. */
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

    const maxReceiveCount = options.maxReceiveCount ?? 5;
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
 * Creates the World's table: PK/SK, GSI1 (every run and every hook, by id)
 * and GSI2 (runs per workflow name), on-demand billing, and TTL on
 * {@link TTL_ATTRIBUTE}. Keeps a table that already exists.
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

async function createQueueWithDeadLetter(
  sqs: SQSClient,
  name: string,
  maxReceiveCount: number,
  visibilityTimeoutSeconds: number
): Promise<{ queueUrl: string; deadLetterQueueUrl: string }> {
  const deadLetter = await sqs.send(
    new CreateQueueCommand({
      QueueName: `${name}-dlq`,
      Attributes: { MessageRetentionPeriod: '1209600' },
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
