/**
 * The body of an SQS message the World sends.
 *
 * The message id the handler sees (`meta.messageId`) travels in the body:
 * SQS mints a new `MessageId` whenever the World sends a message again (a
 * `{ timeoutSeconds }` redelivery, the next hop of a long delay), and the
 * contract wants one id per enqueued message across all of its deliveries.
 * The offsets carry the deliveries an earlier SQS message of the same
 * logical message already made, so `attempt` and `deliveryCount` keep
 * counting across them.
 */
import type { MessageId, ValidQueueName } from '@workflow/world';
import { PayloadTooLargeError } from '../storage/errors.js';
import { isS3Ref, type PayloadStore, type S3Ref } from '../storage/payloads.js';
import { nextUlid } from './state.js';
import { deserializePayload, serializePayload } from './transport.js';

export const MESSAGE_FORMAT_VERSION = 1;

/**
 * The largest body the World sends in one SQS message: 256 KiB, the size
 * every queue accepts (`MaximumMessageSize` goes up to 1 MiB, but many
 * queues keep 256 KiB). A larger message travels in S3: see
 * {@link encodeForQueue}.
 */
export const MAX_SQS_MESSAGE_BYTES = 256 * 1024;

export interface QueueMessage {
  v: typeof MESSAGE_FORMAT_VERSION;
  messageId: MessageId;
  /** The full queue name `queue()` was called with, prefix included. */
  queueName: ValidQueueName;
  payload: unknown;
  /** When the message was first enqueued (ISO 8601). */
  createdAt: string;
  /**
   * Epoch ms before which the message must not be delivered, for delays
   * over SQS's 15 minutes. A delivery that comes early sends the message
   * on with the rest of the delay instead of running it.
   */
  notBefore?: number;
  idempotencyKey?: string;
  /** Caller headers (`QueueOptions.headers`), sent with every delivery. */
  headers?: Record<string, string>;
  /** The delivery route the producer had registered for the prefix. */
  route?: string;
  /** Attempts made by earlier SQS messages of this message. */
  attemptOffset?: number;
  /** Deliveries made by earlier SQS messages of this message. */
  deliveryOffset?: number;
  /**
   * Set on a message too large for SQS: the SQS body only carries the ids
   * and this reference, and the whole message (this format) is the S3
   * object. `payload` is then null.
   */
  bodyRef?: S3Ref;
}

export function encodeMessage(message: QueueMessage): string {
  return serializePayload(message);
}

const encoder = new TextEncoder();

/**
 * The SQS body of `message`. One over {@link MAX_SQS_MESSAGE_BYTES} is
 * written whole to S3, under `queue/<runId>/<messageId>/`, and the body
 * points at it (`bodyRef`). Each send writes its own object; the worker
 * deletes it with the SQS message that carried it.
 *
 * @throws {PayloadTooLargeError} when the message does not fit and no
 *   bucket is configured.
 */
export async function encodeForQueue(
  message: QueueMessage,
  payloads: PayloadStore | undefined,
  maxBytes = MAX_SQS_MESSAGE_BYTES
): Promise<string> {
  const body = encodeMessage(message);
  const bytes = Buffer.byteLength(body);
  if (bytes <= maxBytes) return body;
  if (!payloads?.enabled) {
    throw new PayloadTooLargeError(
      `The queue message ${message.messageId}`,
      bytes,
      {
        limit: maxBytes,
        where: 'the AWS World sends in one SQS message',
      }
    );
  }
  const runId = (message.payload as { runId?: unknown } | null)?.runId;
  const ref = await payloads.put(
    [
      'queue',
      encodeURIComponent(typeof runId === 'string' ? runId : '_'),
      message.messageId,
      nextUlid(),
    ].join('/'),
    encoder.encode(body)
  );
  return encodeMessage({
    v: message.v,
    messageId: message.messageId,
    queueName: message.queueName,
    createdAt: message.createdAt,
    payload: null,
    bodyRef: ref,
  });
}

/**
 * The whole message behind a body that points to S3, or `message` itself.
 *
 * @throws when the object cannot be read or is not a World message.
 */
export async function resolveMessage(
  message: QueueMessage,
  payloads: PayloadStore | undefined
): Promise<QueueMessage> {
  if (message.bodyRef === undefined) return message;
  if (!isS3Ref(message.bodyRef) || !payloads) {
    throw new TypeError(
      'The SQS message points to S3, but no S3 client is set'
    );
  }
  const bytes = await payloads.get(message.bodyRef);
  const whole = decodeMessage(new TextDecoder().decode(bytes as Uint8Array));
  if (whole.messageId !== message.messageId) {
    throw new TypeError('The S3 body belongs to another message');
  }
  return whole;
}

/** @throws {TypeError} when `body` is not a message this World sent. */
export function decodeMessage(body: string | undefined): QueueMessage {
  let value: unknown;
  try {
    value = deserializePayload(body ?? '');
  } catch {
    throw new TypeError('The SQS message body is not JSON');
  }
  const message = value as Partial<QueueMessage> | null;
  if (
    message === null ||
    typeof message !== 'object' ||
    message.v !== MESSAGE_FORMAT_VERSION ||
    typeof message.messageId !== 'string' ||
    typeof message.queueName !== 'string' ||
    typeof message.createdAt !== 'string' ||
    !('payload' in message)
  ) {
    throw new TypeError('The SQS message body is not a World message');
  }
  return message as QueueMessage;
}
