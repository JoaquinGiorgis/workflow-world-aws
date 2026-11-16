/**
 * Lambda mode against the emulator's SQS: each event is built the way an SQS
 * event source mapping builds it, from a real ReceiveMessage (receipt
 * handle, `ApproximateReceiveCount`, the queue's ARN), and what Lambda does
 * with the answer is done here: the records not reported in
 * `batchItemFailures` are deleted, the others stay in the queue.
 */
import { randomUUID } from 'node:crypto';
import { ListObjectsV2Command } from '@aws-sdk/client-s3';
import {
  DeleteMessageBatchCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import type { MessageId, QueuePayload } from '@workflow/world';
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  inject,
  it,
} from 'vitest';
import {
  createQueueWithDeadLetter,
  createWorkflowTable,
} from '../../scripts/bootstrap-local.js';
import {
  type AwsClients,
  type AwsWorld,
  createClients,
  createSqsLambdaHandler,
  createWorld,
  destroyClients,
  type SqsBatchResponse,
  type SqsLambdaEvent,
} from '../../src/index.js';
import { encodeMessage } from '../../src/queue/message.js';

const aws = inject('aws');
const prefix = '__wkf_workflow_';
const queueName = '__wkf_workflow_wf' as const;

type Meta = { messageId: string; attempt: number; deliveryCount: number };

let clients: AwsClients;
let tableName: string;
const open: AwsWorld[] = [];

beforeAll(async () => {
  clients = createClients({ region: aws.region, endpoint: aws.endpoint });
  tableName = `wf-lambda-${randomUUID().slice(0, 8)}`;
  await createWorkflowTable(clients, tableName);
}, 120_000);

afterEach(async () => {
  await Promise.all(open.splice(0).map((world) => world.close()));
});

afterAll(() => {
  destroyClients(clients);
});

async function queueArn(queueUrl: string): Promise<string> {
  const { Attributes } = await clients.sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: ['QueueArn'],
    })
  );
  return Attributes?.QueueArn ?? '';
}

/** A World over fresh queues, delivering in process to `handler`. */
async function setUp(
  handler: (message: unknown, meta: Meta) => Promise<unknown>
) {
  const name = `lambda-${randomUUID().slice(0, 8)}`;
  const workflow = await createQueueWithDeadLetter(
    clients.sqs,
    `${name}-wf`,
    5,
    30
  );
  const step = await createQueueWithDeadLetter(
    clients.sqs,
    `${name}-step`,
    5,
    30
  );
  const world = createWorld({
    region: aws.region,
    endpoint: aws.endpoint,
    tableName,
    workflowQueueUrl: workflow.queueUrl,
    stepQueueUrl: step.queueUrl,
    bucket: aws.bucket,
    clients,
  });
  open.push(world);
  world.registerHandler(
    prefix,
    world.createQueueHandler(prefix, (message, meta) =>
      handler(message, meta as Meta)
    )
  );
  const lambda = createSqsLambdaHandler({ world });
  const arns = {
    workflow: await queueArn(workflow.queueUrl),
    step: await queueArn(step.queueUrl),
  };
  return { world, lambda, workflow, step, arns };
}

/**
 * What an event source mapping does once: receives a batch, invokes the
 * function with it, and deletes the records the function did not report.
 */
async function invokeLikeLambda(
  lambda: (event: SqsLambdaEvent) => Promise<SqsBatchResponse>,
  queueUrl: string,
  arn: string,
  waitSeconds = 1
): Promise<{ event: SqsLambdaEvent; response: SqsBatchResponse }> {
  const { Messages = [] } = await clients.sqs.send(
    new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: waitSeconds,
      VisibilityTimeout: 30,
      MessageSystemAttributeNames: ['All'],
      MessageAttributeNames: ['All'],
    })
  );
  const event: SqsLambdaEvent = {
    Records: Messages.map((message) => ({
      messageId: message.MessageId ?? '',
      receiptHandle: message.ReceiptHandle ?? '',
      body: message.Body ?? '',
      attributes: {
        ApproximateReceiveCount: '1',
        ...message.Attributes,
      },
      messageAttributes: {},
      md5OfBody: message.MD5OfBody ?? '',
      eventSource: 'aws:sqs',
      eventSourceARN: arn,
      awsRegion: aws.region,
    })),
  };
  if (event.Records.length === 0) {
    return { event, response: { batchItemFailures: [] } };
  }
  const response = await lambda(event);
  const failed = new Set(
    response.batchItemFailures.map((failure) => failure.itemIdentifier)
  );
  const done = event.Records.filter((record) => !failed.has(record.messageId));
  if (done.length > 0) {
    await clients.sqs.send(
      new DeleteMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: done.map((record, i) => ({
          Id: String(i),
          ReceiptHandle: record.receiptHandle,
        })),
      })
    );
  }
  return { event, response };
}

/** Invokes until `until()` holds, as the mapping polls. */
async function pollLikeLambda(
  lambda: (event: SqsLambdaEvent) => Promise<SqsBatchResponse>,
  queueUrl: string,
  arn: string,
  until: () => boolean,
  timeoutMs = 20_000
): Promise<SqsBatchResponse[]> {
  const responses: SqsBatchResponse[] = [];
  const deadline = Date.now() + timeoutMs;
  while (!until()) {
    if (Date.now() > deadline) throw new Error('timed out polling');
    const { event, response } = await invokeLikeLambda(lambda, queueUrl, arn);
    if (event.Records.length > 0) responses.push(response);
  }
  return responses;
}

const payload = (extra: Record<string, unknown> = {}) =>
  ({ runId: `wrun_${randomUUID()}`, ...extra }) as unknown as QueuePayload;

async function visibleCount(queueUrl: string): Promise<number> {
  const { Attributes } = await clients.sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: [
        'ApproximateNumberOfMessages',
        'ApproximateNumberOfMessagesNotVisible',
        'ApproximateNumberOfMessagesDelayed',
      ],
    })
  );
  return (
    Number(Attributes?.ApproximateNumberOfMessages ?? 0) +
    Number(Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0) +
    Number(Attributes?.ApproximateNumberOfMessagesDelayed ?? 0)
  );
}

describe('createSqsLambdaHandler on real SQS events', () => {
  it('delivers a batch with stable ids, and Lambda deletes it', async () => {
    const seen: [unknown, Meta][] = [];
    const { world, lambda, workflow, step, arns } = await setUp(
      async (message, meta) => {
        seen.push([message, meta]);
      }
    );
    const sentIds = await Promise.all(
      [payload(), payload(), payload()].map((each) =>
        world.queue(queueName, each)
      )
    );
    const stepId = await world.queue(queueName, payload({ stepId: 'step_1' }));

    await pollLikeLambda(
      lambda,
      workflow.queueUrl,
      arns.workflow,
      () => seen.length >= 3
    );
    await pollLikeLambda(
      lambda,
      step.queueUrl,
      arns.step,
      () => seen.length >= 4
    );
    expect(seen.map(([, meta]) => meta.messageId).sort()).toEqual(
      [...sentIds, stepId].map((each) => each.messageId).sort()
    );
    expect(seen.every(([, meta]) => meta.deliveryCount === 1)).toBe(true);
    expect(await visibleCount(workflow.queueUrl)).toBe(0);
    expect(await visibleCount(step.queueUrl)).toBe(0);
  });

  it('leaves a failed record in the queue with a backoff, and counts the retry', async () => {
    const seen: Meta[] = [];
    const { world, lambda, workflow, arns } = await setUp(
      async (_message, meta) => {
        seen.push(meta);
        if (seen.length === 1) throw new Error('first delivery fails');
      }
    );
    const { messageId } = await world.queue(queueName, payload());

    const [first] = await pollLikeLambda(
      lambda,
      workflow.queueUrl,
      arns.workflow,
      () => seen.length >= 1
    );
    expect(first?.batchItemFailures).toHaveLength(1);
    // Backed off for e^1 seconds: not visible right away.
    const { event } = await invokeLikeLambda(
      lambda,
      workflow.queueUrl,
      arns.workflow,
      0
    );
    expect(event.Records).toHaveLength(0);

    await pollLikeLambda(
      lambda,
      workflow.queueUrl,
      arns.workflow,
      () => seen.length >= 2
    );
    expect(seen.map((meta) => [meta.messageId, meta.attempt])).toEqual([
      [messageId, 1],
      [messageId, 2],
    ]);
    expect(await visibleCount(workflow.queueUrl)).toBe(0);
  });

  it('redelivers after { timeoutSeconds } through a new message', async () => {
    const seen: Meta[] = [];
    const { world, lambda, workflow, arns } = await setUp(
      async (_message, meta) => {
        seen.push(meta);
        return seen.length === 1 ? { timeoutSeconds: 1 } : undefined;
      }
    );
    const { messageId } = await world.queue(queueName, payload());
    const responses = await pollLikeLambda(
      lambda,
      workflow.queueUrl,
      arns.workflow,
      () => seen.length >= 2
    );
    expect(
      responses.every((response) => response.batchItemFailures.length === 0)
    ).toBe(true);
    expect(seen.map((meta) => [meta.messageId, meta.deliveryCount])).toEqual([
      [messageId, 1],
      [messageId, 2],
    ]);
  });

  it('delivers a message that travelled through S3 and deletes its object', async () => {
    const seen: unknown[] = [];
    const { world, lambda, workflow, arns } = await setUp(async (message) => {
      seen.push(message);
    });
    const large = payload({
      runInput: {
        input: new Uint8Array(300 * 1024).map((_, i) => i % 251),
        deploymentId: 'aws',
        workflowName: 'wf',
        specVersion: 8,
      },
    });
    const { messageId } = await world.queue(queueName, large);
    await pollLikeLambda(
      lambda,
      workflow.queueUrl,
      arns.workflow,
      () => seen.length >= 1
    );
    expect(seen[0]).toEqual(large);
    const left = await clients.s3.send(
      new ListObjectsV2Command({ Bucket: aws.bucket, Prefix: 'queue/' })
    );
    expect(
      (left.Contents ?? []).filter((object) =>
        object.Key?.includes(`/${messageId}/`)
      )
    ).toEqual([]);
  });

  it('sends a hop of a long delay on instead of delivering it early', async () => {
    const seen: Meta[] = [];
    const { lambda, workflow, arns } = await setUp(async (_message, meta) => {
      seen.push(meta);
    });
    // What the last 15-minute hop of a long delay looks like when it
    // arrives with three seconds still to go.
    const notBefore = Date.now() + 3_000;
    await clients.sqs.send(
      new SendMessageCommand({
        QueueUrl: workflow.queueUrl,
        MessageBody: encodeMessage({
          v: 1,
          messageId: 'msg_01JD00000000000000000LHOP1' as MessageId,
          queueName,
          payload: payload(),
          createdAt: new Date().toISOString(),
          notBefore,
        }),
      })
    );
    const { event, response } = await invokeLikeLambda(
      lambda,
      workflow.queueUrl,
      arns.workflow
    );
    // Taken off the queue and sent on, not delivered.
    expect(event.Records).toHaveLength(1);
    expect(response.batchItemFailures).toEqual([]);
    expect(seen).toHaveLength(0);

    await pollLikeLambda(
      lambda,
      workflow.queueUrl,
      arns.workflow,
      () => seen.length >= 1
    );
    expect(Date.now()).toBeGreaterThanOrEqual(notBefore - 1_000);
    // The hop is not a delivery.
    expect(seen[0]).toMatchObject({
      messageId: 'msg_01JD00000000000000000LHOP1',
      attempt: 1,
      deliveryCount: 1,
    });
  });
});
