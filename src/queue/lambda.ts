/**
 * Lambda mode: the World's side of an SQS event source mapping.
 *
 * Lambda receives the messages, keeps them invisible while the function
 * runs, and deletes the records the function does not report in
 * `batchItemFailures` (the mapping needs `ReportBatchItemFailures`). So
 * there is no worker, no heartbeat and no `DeleteMessage` here; each record
 * goes through the same steps as one the worker received (`consumer.ts`):
 *
 * - The message id is the one in the body, and `attempt` / `deliveryCount`
 *   are the record's `ApproximateReceiveCount` plus the body's offsets.
 * - A body pointing to S3 is read from there; its object is deleted once
 *   the record succeeded.
 * - An early hop of a long delay is sent on with the rest of its delay, and
 *   the record succeeds without a delivery.
 * - Acknowledged: the record succeeds.
 * - `{ timeoutSeconds: n }`: the same message is sent again with an `n`
 *   second delay, and the record succeeds. Leaving the record in the queue
 *   with `ChangeMessageVisibility(n)` would also delay it, but would count
 *   a receive towards the dead-letter queue's `maxReceiveCount` on every
 *   redelivery, which the worker avoids too.
 * - Failed: the record is reported, after `ChangeMessageVisibility` sets
 *   the same backoff the worker uses (`e^receiveCount` seconds, at most an
 *   hour). Without it the message would come back after the queue's
 *   visibility timeout. A record whose handling throws is backed off and
 *   reported the same way.
 *
 * A spec 9 run's orchestrator records wait for each other within the
 * process (the same `RunGate` the worker uses), and at most `concurrency`
 * records of an event are delivered at once. Given a deadline, no record
 * is started once less than its margin is left: the records not started
 * are reported without a backoff, so only they come back (after the queue's
 * visibility timeout) and the ones that succeeded are not delivered again
 * when the function would otherwise time out. Before answering, the handler
 * waits for the idempotency confirmations its deliveries started, since
 * Lambda may freeze the process as soon as it answers.
 */
import {
  ChangeMessageVisibilityCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import { debugLog } from '../storage/log.js';
import type { PayloadStore, S3Ref } from '../storage/payloads.js';
import {
  defaultRetryDelaySeconds,
  describe,
  followUp,
  isEarlyHop,
  readReceived,
  receiveCountOf,
  runGateKey,
  sendOnTowardNotBefore,
  sendQueueMessage,
  traceOf,
} from './consumer.js';
import type { Delivery, DeliveryResult } from './delivery.js';
import type { QueueMessage } from './message.js';
import type { RunGate } from './state.js';

/**
 * One record of an SQS event, as Lambda passes it. The same shape as
 * `SQSRecord` in `@types/aws-lambda` with every field the World does not
 * read optional, so a record typed with that package can be passed.
 */
export interface SqsLambdaRecord {
  messageId: string;
  receiptHandle: string;
  body: string;
  attributes: {
    ApproximateReceiveCount?: string;
    SentTimestamp?: string;
    SenderId?: string;
    ApproximateFirstReceiveTimestamp?: string;
    AWSTraceHeader?: string;
    SequenceNumber?: string;
    MessageGroupId?: string;
    MessageDeduplicationId?: string;
    DeadLetterQueueSourceArn?: string;
  };
  /** `arn:aws:sqs:<region>:<account>:<queue name>`. */
  eventSourceARN: string;
  messageAttributes?: Record<string, unknown>;
  md5OfBody?: string;
  eventSource?: string;
  awsRegion?: string;
}

/** The event an SQS event source mapping invokes the function with. */
export interface SqsLambdaEvent {
  Records: SqsLambdaRecord[];
}

/**
 * When an invocation has to answer by: the handler starts no record once
 * fewer than `marginMs` milliseconds are left.
 */
export interface SqsEventDeadline {
  /** Milliseconds the invocation has left (`context.getRemainingTimeInMillis`). */
  remainingTimeMs: () => number;
  marginMs: number;
}

/** What the function answers: the records to leave in the queue. */
export interface SqsBatchResponse {
  batchItemFailures: { itemIdentifier: string }[];
}

export interface LambdaDeliveryOptions {
  sqs: SQSClient;
  /** The World's queues; a record from any other queue is reported. */
  queueUrls: readonly string[];
  /** Records of one event delivered at once, at most. */
  concurrency: number;
  runGate: RunGate;
  deliver: (delivery: Delivery) => Promise<DeliveryResult>;
  payloads?: PayloadStore;
  retryDelaySeconds?: (receiveCount: number) => number;
  log: (message: string, detail?: Record<string, unknown>) => void;
}

/** The URL of the queue an event source ARN names, among `queueUrls`. */
export function queueUrlOfArn(
  arn: string,
  queueUrls: readonly string[]
): string | undefined {
  const name = arn.slice(arn.lastIndexOf(':') + 1);
  if (!name) return undefined;
  return queueUrls.find(
    (url) => url.replace(/\/+$/, '').split('/').pop() === name
  );
}

/** Handles the records of one SQS event; see the module comment. */
export async function handleSqsEvent(
  options: LambdaDeliveryOptions,
  event: SqsLambdaEvent,
  deadline?: SqsEventDeadline
): Promise<SqsBatchResponse> {
  const records = event.Records ?? [];
  const failed: string[] = [];
  let next = 0;
  let remainingMs: number | undefined;
  const timeIsUp = () => {
    if (!deadline) return false;
    remainingMs = deadline.remainingTimeMs();
    return remainingMs < deadline.marginMs;
  };
  const lane = async () => {
    while (next < records.length && !timeIsUp()) {
      const record = records[next++] as SqsLambdaRecord;
      let ok: boolean;
      try {
        ok = await handleRecord(options, record);
      } catch (error) {
        options.log('A queue record could not be handled', {
          messageId: record.messageId,
          error: describe(error),
        });
        // The same backoff as a failed delivery, rather than the queue's
        // visibility timeout.
        const queueUrl = queueUrlOfArn(
          record.eventSourceARN,
          options.queueUrls
        );
        ok = queueUrl ? await backOff(options, record, queueUrl) : false;
      }
      if (!ok) failed.push(record.messageId);
    }
  };
  await Promise.all(
    Array.from(
      { length: Math.max(1, Math.min(options.concurrency, records.length)) },
      lane
    )
  );
  // Not started: left in the queue as they are, without a backoff.
  const notStarted = records.slice(next);
  if (notStarted.length > 0) {
    options.log(
      'Leaving records of the event for redelivery: the invocation is near its deadline',
      {
        records: notStarted.length,
        remainingMs,
        marginMs: deadline?.marginMs,
      }
    );
    failed.push(...notStarted.map((record) => record.messageId));
  }
  // In the order Lambda gave them.
  const order = new Map(records.map((record, i) => [record.messageId, i]));
  failed.sort((a, b) => (order.get(a) ?? 0) - (order.get(b) ?? 0));
  return { batchItemFailures: failed.map((id) => ({ itemIdentifier: id })) };
}

/**
 * Sets the record's visibility to the worker's backoff for its receive
 * count; answers false, the record failed.
 */
async function backOff(
  options: LambdaDeliveryOptions,
  record: SqsLambdaRecord,
  queueUrl: string
): Promise<false> {
  const delay = (options.retryDelaySeconds ?? defaultRetryDelaySeconds)(
    receiveCountOf(record.attributes)
  );
  try {
    await options.sqs.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: record.receiptHandle,
        VisibilityTimeout: Math.max(0, Math.min(43_200, Math.ceil(delay))),
      })
    );
  } catch (error) {
    // It comes back after the queue's visibility timeout instead.
    options.log('ChangeMessageVisibility failed', {
      queueUrl,
      error: describe(error),
    });
  }
  return false;
}

/** Whether the record succeeded (Lambda deletes it) or failed. */
async function handleRecord(
  options: LambdaDeliveryOptions,
  record: SqsLambdaRecord
): Promise<boolean> {
  const { sqs, payloads } = options;
  const queueUrl = queueUrlOfArn(record.eventSourceARN, options.queueUrls);
  if (!queueUrl) {
    options.log(
      "Received a record from a queue that is not one of the World's; is the event source mapping on the right queue?",
      { messageId: record.messageId, eventSourceARN: record.eventSourceARN }
    );
    return false;
  }
  const receiveCount = receiveCountOf(record.attributes);
  const retry = () => backOff(options, record, queueUrl);

  let message: QueueMessage;
  let bodyRef: S3Ref | undefined;
  try {
    ({ message, bodyRef } = await readReceived(record.body, payloads));
  } catch (error) {
    options.log('Received a message that is not a World message', {
      queueUrl,
      messageId: record.messageId,
      error: describe(error),
    });
    return retry();
  }
  /** The record succeeded: its S3 body, if any, is not needed any more. */
  const succeed = async () => {
    if (bodyRef && payloads) {
      try {
        await payloads.delete(bodyRef);
      } catch (error) {
        options.log('Could not delete the S3 body of a handled message', {
          ref: bodyRef.s3Ref,
          error: describe(error),
        });
      }
    }
    return true;
  };

  if (isEarlyHop(message)) {
    try {
      await sendOnTowardNotBefore(sqs, queueUrl, message, payloads);
    } catch (error) {
      options.log('Could not send a delayed message on', {
        messageId: message.messageId,
        error: describe(error),
      });
      return retry();
    }
    return succeed();
  }

  const gateKey = runGateKey(message);
  const release = gateKey ? await options.runGate.acquire(gateKey) : undefined;
  try {
    const attempt = receiveCount + (message.attemptOffset ?? 0);
    const deliveryCount = receiveCount + (message.deliveryOffset ?? 0);
    const trace = { ...traceOf(message), attempt, deliveryCount };
    debugLog('queue delivery started (Lambda)', trace);
    let result: DeliveryResult;
    try {
      result = await options.deliver({
        message,
        attempt,
        deliveryCount,
        invocationStartedAt: Date.now(),
      });
    } catch (error) {
      result = { type: 'error', status: 0, text: describe(error) };
    }
    switch (result.type) {
      case 'completed':
        debugLog('queue delivery completed (Lambda)', trace);
        return succeed();
      case 'reschedule':
        debugLog('queue delivery rescheduled (Lambda)', {
          ...trace,
          timeoutSeconds: result.timeoutSeconds,
        });
        try {
          await sendQueueMessage(
            sqs,
            queueUrl,
            followUp(message, attempt, deliveryCount, result.timeoutSeconds),
            result.timeoutSeconds,
            payloads
          );
        } catch (error) {
          options.log('Could not send a redelivery; retrying the record', {
            messageId: message.messageId,
            error: describe(error),
          });
          return retry();
        }
        return succeed();
      default:
        options.log('Queue delivery failed; retrying after a backoff', {
          queueName: message.queueName,
          messageId: message.messageId,
          attempt,
          status: result.status,
          error: result.text.slice(0, 1_000),
        });
        return retry();
    }
  } finally {
    release?.();
  }
}
