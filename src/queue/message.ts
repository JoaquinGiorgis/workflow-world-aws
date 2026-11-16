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
import { deserializePayload, serializePayload } from './transport.js';

export const MESSAGE_FORMAT_VERSION = 1;

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
}

export function encodeMessage(message: QueueMessage): string {
  return serializePayload(message);
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
