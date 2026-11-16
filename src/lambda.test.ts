import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
} from '@aws-sdk/client-sqs';
import {
  DeleteCommand,
  PutCommand,
  QueryCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { mockClient } from 'aws-sdk-client-mock';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { sqsMessage } from '../test/unit/sqs-mock.js';
import {
  type AwsClients,
  type AwsWorld,
  createClients,
  createSqsLambdaHandler,
  createWorld,
  destroyClients,
  type SqsLambdaEvent,
  type SqsLambdaRecord,
} from './index.js';
import { handleSqsEvent } from './queue/lambda.js';
import { decodeMessage, type QueueMessage } from './queue/message.js';
import type { RunGate } from './queue/state.js';

const prefix = '__wkf_workflow_';
type Handler = Parameters<AwsWorld['createQueueHandler']>[1];

let clients: AwsClients;
let sqs: ReturnType<typeof mockClient>;
let ddb: ReturnType<typeof mockClient>;
let worlds: AwsWorld[];
let n = 0;
let names: { workflow: string; step: string };

beforeEach(() => {
  n++;
  names = { workflow: `wf-lambda-${n}`, step: `step-lambda-${n}` };
  clients = createClients({
    region: 'us-east-1',
    endpoint: 'http://localhost:4566',
  });
  sqs = mockClient(clients.sqs);
  ddb = mockClient(clients.documents);
  sqs.on(SendMessageCommand).resolves({ MessageId: 'sqs' });
  sqs.on(ChangeMessageVisibilityCommand).resolves({});
  ddb.on(PutCommand).resolves({});
  ddb.on(UpdateCommand).resolves({});
  ddb.on(DeleteCommand).resolves({});
  ddb.on(QueryCommand).resolves({ Items: [] });
  worlds = [];
});

afterEach(async () => {
  await Promise.all(worlds.map((world) => world.close()));
  sqs.restore();
  ddb.restore();
  destroyClients(clients);
});

const queueUrl = (name: string) => `http://localhost:4566/000000000000/${name}`;
const queueArn = (name: string) => `arn:aws:sqs:us-east-1:000000000000:${name}`;

/** A World over this test's queues whose prefix is served by `handler`. */
function world(handler: Handler, extra: Record<string, unknown> = {}) {
  const created = createWorld({
    region: 'us-east-1',
    endpoint: 'http://localhost:4566',
    tableName: 'workflow',
    workflowQueueUrl: queueUrl(names.workflow),
    stepQueueUrl: queueUrl(names.step),
    clients,
    ...extra,
  });
  worlds.push(created);
  created.registerHandler(prefix, created.createQueueHandler(prefix, handler));
  return created;
}

/** The SQS event record Lambda hands over for a message of `queue`. */
function record(
  message: Partial<QueueMessage> & { payload: unknown },
  receiveCount = 1,
  queue: 'workflow' | 'step' = 'workflow'
): SqsLambdaRecord {
  const sqsRecord = sqsMessage(message, receiveCount);
  return {
    messageId: sqsRecord.MessageId ?? '',
    receiptHandle: sqsRecord.ReceiptHandle ?? '',
    body: sqsRecord.Body ?? '',
    attributes: {
      ApproximateReceiveCount: String(receiveCount),
      SentTimestamp: String(Date.now()),
    },
    messageAttributes: {},
    md5OfBody: '',
    eventSource: 'aws:sqs',
    eventSourceARN: queueArn(names[queue]),
    awsRegion: 'us-east-1',
  };
}

const event = (...records: SqsLambdaRecord[]): SqsLambdaEvent => ({
  Records: records,
});

const sentMessages = () =>
  sqs.commandCalls(SendMessageCommand).map((call) => ({
    queueUrl: call.args[0].input.QueueUrl,
    delaySeconds: call.args[0].input.DelaySeconds,
    message: decodeMessage(call.args[0].input.MessageBody),
  }));

describe('createSqsLambdaHandler', () => {
  it('delivers each record with the id in its body and leaves deleting it to Lambda', async () => {
    const handler = vi.fn(async () => undefined);
    const lambda = createSqsLambdaHandler({ world: world(handler) });
    const delivered = record(
      {
        messageId: 'msg_01JD0000000000000000000LMB' as never,
        payload: { runId: 'wrun_A' },
        attemptOffset: 1,
        deliveryOffset: 2,
      },
      3
    );

    expect(await lambda(event(delivered))).toEqual({ batchItemFailures: [] });
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler.mock.calls[0]).toEqual([
      { runId: 'wrun_A' },
      expect.objectContaining({
        messageId: 'msg_01JD0000000000000000000LMB',
        queueName: '__wkf_workflow_wf',
        attempt: 4,
        deliveryCount: 5,
      }),
    ]);
    // No heartbeat and no delete: Lambda owns the message's visibility and
    // deletes the records that are not reported.
    expect(sqs.commandCalls(DeleteMessageCommand)).toHaveLength(0);
    expect(sqs.commandCalls(ChangeMessageVisibilityCommand)).toHaveLength(0);
    expect(sqs.commandCalls(ReceiveMessageCommand)).toHaveLength(0);
  });

  it('reports a failed delivery and backs its message off', async () => {
    const lambda = createSqsLambdaHandler({
      world: world(async () => {
        throw new Error('boom');
      }),
    });
    const failed = record({ payload: { runId: 'wrun_A' } }, 2);
    expect(await lambda(event(failed))).toEqual({
      batchItemFailures: [{ itemIdentifier: failed.messageId }],
    });
    expect(
      sqs.commandCalls(ChangeMessageVisibilityCommand)[0]?.args[0].input
    ).toEqual({
      QueueUrl: queueUrl(names.workflow),
      ReceiptHandle: failed.receiptHandle,
      // e^2 seconds, as the worker backs off.
      VisibilityTimeout: 8,
    });
  });

  it('reports only the records that failed', async () => {
    const lambda = createSqsLambdaHandler({
      world: world(async (message) => {
        if ((message as { runId: string }).runId === 'wrun_bad') {
          throw new Error('boom');
        }
      }),
    });
    const good = record({ payload: { runId: 'wrun_good' } });
    const bad = record({ payload: { runId: 'wrun_bad' } });
    const step = record(
      { payload: { runId: 'wrun_good', stepId: 'step_1' } },
      1,
      'step'
    );
    expect(await lambda(event(good, bad, step))).toEqual({
      batchItemFailures: [{ itemIdentifier: bad.messageId }],
    });
  });

  it('sends a { timeoutSeconds } redelivery as a new message and lets the record go', async () => {
    const lambda = createSqsLambdaHandler({
      world: world(async () => ({ timeoutSeconds: 5 })),
    });
    const rescheduled = record(
      {
        messageId: 'msg_01JD0000000000000000000RSC' as never,
        payload: { runId: 'wrun_A', stepId: 'step_1' },
      },
      1,
      'step'
    );
    expect(await lambda(event(rescheduled))).toEqual({
      batchItemFailures: [],
    });
    const [again] = sentMessages();
    expect(again).toMatchObject({
      queueUrl: queueUrl(names.step),
      delaySeconds: 5,
      message: {
        messageId: 'msg_01JD0000000000000000000RSC',
        deliveryOffset: 1,
      },
    });
  });

  it('sends a hop of a long delay on without delivering it', async () => {
    const handler = vi.fn(async () => undefined);
    const lambda = createSqsLambdaHandler({ world: world(handler) });
    const hop = record({
      payload: { runId: 'wrun_A' },
      notBefore: Date.now() + 120_000,
    });
    expect(await lambda(event(hop))).toEqual({ batchItemFailures: [] });
    expect(handler).not.toHaveBeenCalled();
    const [next] = sentMessages();
    expect(next?.delaySeconds).toBeGreaterThanOrEqual(119);
    expect(next?.delaySeconds).toBeLessThanOrEqual(120);
  });

  it('reports a record that is not a World message', async () => {
    const lambda = createSqsLambdaHandler({
      world: world(async () => undefined),
    });
    const garbage = { ...record({ payload: {} }), body: 'not json' };
    expect(await lambda(event(garbage))).toEqual({
      batchItemFailures: [{ itemIdentifier: garbage.messageId }],
    });
    expect(sqs.commandCalls(ChangeMessageVisibilityCommand)).toHaveLength(1);
  });

  it('reports a record from a queue the World was not given', async () => {
    const handler = vi.fn(async () => undefined);
    const lambda = createSqsLambdaHandler({ world: world(handler) });
    const stranger = {
      ...record({ payload: { runId: 'wrun_A' } }),
      eventSourceARN: queueArn('somebody-else'),
    };
    expect(await lambda(event(stranger))).toEqual({
      batchItemFailures: [{ itemIdentifier: stranger.messageId }],
    });
    expect(handler).not.toHaveBeenCalled();
  });

  it("runs one spec 9 run's records one at a time", async () => {
    let running = 0;
    let most = 0;
    const lambda = createSqsLambdaHandler({
      world: world(async () => {
        running++;
        most = Math.max(most, running);
        await new Promise((resolve) => setTimeout(resolve, 20));
        running--;
      }),
    });
    const spec9 = () =>
      record({ payload: { runId: 'wrun_A', runSpecVersion: 9 } });
    expect(await lambda(event(spec9(), spec9(), spec9()))).toEqual({
      batchItemFailures: [],
    });
    expect(most).toBe(1);
  });

  it('answers only once the idempotency keys its deliveries sent are confirmed', async () => {
    let confirm!: () => void;
    ddb.on(UpdateCommand).callsFake(
      () =>
        new Promise((resolve) => {
          confirm = () => resolve({});
        })
    );
    const created = world(async () => {
      await created.queue('__wkf_workflow_wf', { runId: 'wrun_B' } as never, {
        idempotencyKey: 'step-key',
      });
    });
    const lambda = createSqsLambdaHandler({ world: created });
    let answered = false;
    const answer = lambda(event(record({ payload: { runId: 'wrun_A' } }))).then(
      (response) => {
        answered = true;
        return response;
      }
    );
    await vi.waitFor(() =>
      expect(ddb.commandCalls(UpdateCommand)).toHaveLength(1)
    );
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(answered).toBe(false);
    confirm();
    expect(await answer).toEqual({ batchItemFailures: [] });
  });

  it('recovers the runs that have not ended on its first event, once per interval', async () => {
    const created = world(async () => undefined);
    const lambda = createSqsLambdaHandler({ world: created });
    await lambda(event(record({ payload: { runId: 'wrun_A' } })));
    await lambda(event(record({ payload: { runId: 'wrun_A' } })));
    const recoveries = ddb
      .commandCalls(QueryCommand)
      .filter((call) => call.args[0].input.IndexName === 'GSI3');
    expect(recoveries).toHaveLength(1);
    const guard = ddb
      .commandCalls(PutCommand)
      .map((call) => call.args[0].input)
      .find((input) => input.Item?.PK === 'RECOVERY');
    expect(guard?.ConditionExpression).toBe(
      'attribute_not_exists(PK) OR recoveredAt < :cutoff'
    );
  });

  it('answers even when recovery fails, and tries it again on the next event', async () => {
    ddb
      .on(QueryCommand)
      .rejectsOnce(new Error('throttled'))
      .resolves({ Items: [] });
    const log = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      const lambda = createSqsLambdaHandler({
        world: world(async () => undefined),
      });
      expect(
        await lambda(event(record({ payload: { runId: 'wrun_A' } })))
      ).toEqual({ batchItemFailures: [] });
      expect(log).toHaveBeenCalledWith(
        expect.stringContaining('recover'),
        expect.anything()
      );
      await lambda(event(record({ payload: { runId: 'wrun_A' } })));
      expect(
        ddb
          .commandCalls(QueryCommand)
          .filter((call) => call.args[0].input.IndexName === 'GSI3')
      ).toHaveLength(2);
    } finally {
      log.mockRestore();
    }
  });

  it.each([
    [
      'a missing GSI3',
      Object.assign(
        new Error('The table does not have the specified index: GSI3'),
        {
          name: 'ValidationException',
          $fault: 'client',
          $metadata: { httpStatusCode: 400 },
        }
      ),
    ],
    [
      'access denied',
      Object.assign(new Error('not authorized to perform: dynamodb:Query'), {
        name: 'AccessDeniedException',
        $fault: 'client',
        $metadata: { httpStatusCode: 400 },
      }),
    ],
  ])(
    'does not try recovery again in the process after %s',
    async (_, error) => {
      ddb.on(QueryCommand).rejects(error);
      const log = vi.spyOn(console, 'warn').mockImplementation(() => {});
      try {
        const lambda = createSqsLambdaHandler({
          world: world(async () => undefined),
        });
        for (let i = 0; i < 3; i++) {
          expect(
            await lambda(event(record({ payload: { runId: 'wrun_A' } })))
          ).toEqual({ batchItemFailures: [] });
        }
        expect(
          ddb
            .commandCalls(QueryCommand)
            .filter((call) => call.args[0].input.IndexName === 'GSI3')
        ).toHaveLength(1);
        expect(log).toHaveBeenCalledTimes(1);
        expect(log).toHaveBeenCalledWith(
          expect.stringContaining('not trying again'),
          expect.anything()
        );
      } finally {
        log.mockRestore();
      }
    }
  );

  it.each([
    ['ExpiredTokenException', 400],
    ['ExpiredToken', 400],
    ['RequestExpired', 400],
  ])(
    'tries recovery again on the next event after %s (expired credentials)',
    async (name, status) => {
      ddb
        .on(QueryCommand)
        .rejectsOnce(
          Object.assign(new Error('The security token included has expired'), {
            name,
            $fault: 'client',
            $metadata: { httpStatusCode: status },
          })
        )
        .resolves({ Items: [] });
      const log = vi.fn();
      const lambda = createSqsLambdaHandler({
        world: world(async () => undefined),
        log,
      });
      await lambda(event(record({ payload: { runId: 'wrun_A' } })));
      await lambda(event(record({ payload: { runId: 'wrun_A' } })));
      expect(
        ddb
          .commandCalls(QueryCommand)
          .filter((call) => call.args[0].input.IndexName === 'GSI3')
      ).toHaveLength(2);
      expect(log).toHaveBeenCalledTimes(1);
      expect(log).toHaveBeenCalledWith(
        expect.stringContaining('trying again on the next event'),
        expect.anything()
      );
    }
  );

  it('sends its warnings to the logger it was given', async () => {
    ddb
      .on(QueryCommand)
      .rejectsOnce(new Error('throttled'))
      .resolves({ Items: [] });
    const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
    const log = vi.fn();
    try {
      const lambda = createSqsLambdaHandler({
        world: world(async () => undefined),
        log,
      });
      await lambda(event(record({ payload: { runId: 'wrun_A' } })));
      expect(log).toHaveBeenCalledWith(
        'Could not recover the runs that have not ended; trying again on the next event',
        { error: 'throttled' }
      );
      expect(warn).not.toHaveBeenCalled();
    } finally {
      warn.mockRestore();
    }
  });

  it('tries recovery again on the next event after a throttled query', async () => {
    ddb
      .on(QueryCommand)
      .rejectsOnce(
        Object.assign(new Error('Rate of requests exceeds the allowed'), {
          name: 'ThrottlingException',
          $fault: 'client',
          $metadata: { httpStatusCode: 400 },
          $retryable: { throttling: true },
        })
      )
      .resolves({ Items: [] });
    const log = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      const lambda = createSqsLambdaHandler({
        world: world(async () => undefined),
      });
      await lambda(event(record({ payload: { runId: 'wrun_A' } })));
      await lambda(event(record({ payload: { runId: 'wrun_A' } })));
      expect(
        ddb
          .commandCalls(QueryCommand)
          .filter((call) => call.args[0].input.IndexName === 'GSI3')
      ).toHaveLength(2);
      expect(log).toHaveBeenCalledWith(
        expect.stringContaining('trying again on the next event'),
        expect.anything()
      );
    } finally {
      log.mockRestore();
    }
  });

  it('takes an event typed with @types/aws-lambda', () => {
    // The shapes of SQSEvent in @types/aws-lambda (trigger/sqs.d.ts),
    // without adding the package: they must be assignable to ours.
    interface SQSRecordAttributes {
      AWSTraceHeader?: string;
      ApproximateReceiveCount: string;
      SentTimestamp: string;
      SenderId: string;
      ApproximateFirstReceiveTimestamp: string;
      SequenceNumber?: string;
      MessageGroupId?: string;
      MessageDeduplicationId?: string;
      DeadLetterQueueSourceArn?: string;
    }
    interface SQSMessageAttributes {
      [name: string]: { stringValue?: string; dataType: string };
    }
    interface SQSRecord {
      messageId: string;
      receiptHandle: string;
      body: string;
      attributes: SQSRecordAttributes;
      messageAttributes: SQSMessageAttributes;
      md5OfBody: string;
      md5OfMessageAttributes?: string;
      eventSource: string;
      eventSourceARN: string;
      awsRegion: string;
    }
    interface SQSEvent {
      Records: SQSRecord[];
    }
    const theirs: SQSEvent = { Records: [] };
    const ours: SqsLambdaEvent = theirs;
    expect(ours.Records).toEqual([]);
  });

  it('creates its World once, from a factory', async () => {
    const factory = vi.fn(() => world(async () => undefined));
    const lambda = createSqsLambdaHandler({ world: factory });
    await lambda(event(record({ payload: { runId: 'wrun_A' } })));
    await lambda(event(record({ payload: { runId: 'wrun_A' } })));
    expect(factory).toHaveBeenCalledTimes(1);
  });
});

describe('handleSqsEvent', () => {
  it('backs off a record whose handling throws, and reports it', async () => {
    const log = vi.fn();
    const failed = record(
      { payload: { runId: 'wrun_A', runSpecVersion: 9 } },
      3
    );
    const response = await handleSqsEvent(
      {
        sqs: clients.sqs,
        queueUrls: [queueUrl(names.workflow), queueUrl(names.step)],
        concurrency: 1,
        // Throws where nothing in handleRecord catches it.
        runGate: {
          acquire: () => Promise.reject(new Error('gate')),
        } as unknown as RunGate,
        deliver: async () => ({ type: 'completed' }),
        log,
      },
      event(failed)
    );
    expect(response).toEqual({
      batchItemFailures: [{ itemIdentifier: failed.messageId }],
    });
    expect(log).toHaveBeenCalledWith(
      'A queue record could not be handled',
      expect.anything()
    );
    expect(
      sqs.commandCalls(ChangeMessageVisibilityCommand)[0]?.args[0].input
    ).toEqual({
      QueueUrl: queueUrl(names.workflow),
      ReceiptHandle: failed.receiptHandle,
      // e^3 seconds, the same backoff as a failed delivery.
      VisibilityTimeout: 21,
    });
  });
});
