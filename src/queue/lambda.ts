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
 *   visibility timeout.
 *
 * A spec 9 run's orchestrator records wait for each other within the
 * process (the same `RunGate` the worker uses), and at most `concurrency`
 * records of an event are delivered at once. Before answering, the handler
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
 * One record of an SQS event, as Lambda passes it: the fields the World
 * reads, a subset of `SQSRecord` in `@types/aws-lambda`.
 */
export interface SqsLambdaRecord {
  messageId: string;
  receiptHandle: string;
  body: string;
  attributes: {
    ApproximateReceiveCount?: string;
    [name: string]: string | undefined;
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
  event: SqsLambdaEvent
): Promise<SqsBatchResponse> {
  const records = event.Records ?? [];
  const failed: string[] = [];
  let next = 0;
  const lane = async () => {
    while (next < records.length) {
      const record = records[next++] as SqsLambdaRecord;
      let ok: boolean;
      try {
        ok = await handleRecord(options, record);
      } catch (error) {
        options.log('A queue record could not be handled', {
          messageId: record.messageId,
          error: describe(error),
        });
        ok = false;
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
  // In the order Lambda gave them.
  const order = new Map(records.map((record, i) => [record.messageId, i]));
  failed.sort((a, b) => (order.get(a) ?? 0) - (order.get(b) ?? 0));
  return { batchItemFailures: failed.map((id) => ({ itemIdentifier: id })) };
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
  const backOff = async () => {
    const delay = (options.retryDelaySeconds ?? defaultRetryDelaySeconds)(
      receiveCount
    );
    try {
      await sqs.send(
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
  };

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
    return backOff();
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
      return backOff();
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
          return backOff();
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
        return backOff();
    }
  } finally {
    release?.();
  }
}
