/**
 * Deduplication of `queue()` sends by `idempotencyKey`, on the World's table
 * (`IDEMP#<key>` / `Q`).
 *
 * A send first reserves the key with a conditional put that also records
 * the message id it will send under and when it reserved it. The put that
 * wins sends the message and then marks the reservation sent; a send that
 * loses answers with the stored message id and sends nothing. If the
 * winner's send fails, it deletes its reservation so a retry can send. If it
 * dies between the two, the reservation stays unconfirmed: a later send
 * under the key waits for it, and takes it over (sending under the same
 * message id) once it is older than {@link RESERVATION_TIMEOUT_MS}, which
 * is well past the longest a live sender's SQS client can keep a send going
 * (`SQS_MAX_SEND_MS`). Confirming and releasing are conditioned on the
 * sender's own reservation time, so a sender that was overtaken anyway
 * touches neither the taker's reservation nor its key. A key is kept for
 * {@link IDEMPOTENCY_TTL_SECONDS} whether or not its message was delivered,
 * as on Vercel Queues; DynamoDB's TTL deletes it some time after it expired,
 * and the conditions treat an expired item as absent until then.
 */
import {
  DeleteCommand,
  type DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  type MessageId,
  QUEUE_MIN_STEP_KEY_DEDUPE_WINDOW_MS,
} from '@workflow/world';
import { idempotencyKey } from '../keys.js';
import { sleep } from '../storage/table.js';

/** How long a key deduplicates sends: the contract's minimum, 24 hours. */
export const IDEMPOTENCY_TTL_SECONDS =
  QUEUE_MIN_STEP_KEY_DEDUPE_WINDOW_MS / 1000;

/**
 * An unconfirmed reservation older than this is taken over: 10 minutes,
 * more than four times the longest one send can take (`SQS_MAX_SEND_MS`),
 * so a sender still sending is never overtaken.
 */
export const RESERVATION_TIMEOUT_MS = 10 * 60_000;

/** How often a send polls a reservation another sender has not confirmed. */
const RESERVATION_POLL_MS = 100;

export type Reservation =
  | {
      /** This send holds the reservation and must send the message. */
      owner: true;
      /** The message id to send under. */
      messageId: MessageId;
      /** When this send reserved (or took over) the key. */
      reservedAt: number;
    }
  | {
      /** An earlier send under the key holds it: nothing to send. */
      owner: false;
      /** The message id the earlier send used. */
      messageId: MessageId;
    };

/** Where the store reports a confirmation that lost its reservation. */
export type IdempotencyLog = (
  message: string,
  detail?: Record<string, unknown>
) => void;

function isConditionFailure(error: unknown): boolean {
  return (
    (error as { name?: string } | undefined)?.name ===
    'ConditionalCheckFailedException'
  );
}

export class IdempotencyStore {
  constructor(
    private readonly documents: DynamoDBDocumentClient,
    private readonly tableName: string,
    private readonly log: IdempotencyLog
  ) {}

  async reserve(key: string, messageId: MessageId): Promise<Reservation> {
    for (;;) {
      const now = Date.now();
      const nowSeconds = Math.floor(now / 1000);
      try {
        await this.documents.send(
          new PutCommand({
            TableName: this.tableName,
            Item: {
              ...idempotencyKey(key),
              type: 'idempotency',
              messageId,
              state: 'reserved',
              reservedAt: now,
              expiresAt: nowSeconds + IDEMPOTENCY_TTL_SECONDS,
            },
            ConditionExpression: 'attribute_not_exists(PK) OR expiresAt < :now',
            ExpressionAttributeValues: { ':now': nowSeconds },
          })
        );
        return { owner: true, messageId, reservedAt: now };
      } catch (error) {
        if (!isConditionFailure(error)) throw error;
      }

      const existing = await this.read(key);
      // Released or expired since: try again.
      if (!existing || existing.expiresAt < nowSeconds) continue;
      if (existing.state === 'sent') {
        return { owner: false, messageId: existing.messageId };
      }
      if (Date.now() - existing.reservedAt < RESERVATION_TIMEOUT_MS) {
        await sleep(RESERVATION_POLL_MS);
        continue;
      }
      // The sender that reserved the key never confirmed it: take the
      // reservation over and send under its message id.
      const reservedAt = await this.takeOver(key, existing);
      if (reservedAt !== undefined) {
        return { owner: true, messageId: existing.messageId, reservedAt };
      }
    }
  }

  /**
   * Marks the key's message sent, if the reservation is still the one this
   * send made. Best effort: see the module comment.
   */
  async confirm(
    key: string,
    messageId: MessageId,
    reservedAt: number
  ): Promise<void> {
    try {
      await this.documents.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: idempotencyKey(key),
          UpdateExpression: 'SET #state = :sent',
          ConditionExpression: 'messageId = :id AND reservedAt = :mine',
          ExpressionAttributeNames: { '#state': 'state' },
          ExpressionAttributeValues: {
            ':sent': 'sent',
            ':id': messageId,
            ':mine': reservedAt,
          },
        })
      );
    } catch (error) {
      if (isConditionFailure(error)) {
        // Another sender took the reservation over while this one was
        // sending: the message may now be in the queue twice.
        this.log(
          `Idempotent send was overtaken: another sender took over its reservation after ${RESERVATION_TIMEOUT_MS} ms and may have sent the message again`,
          { idempotencyKey: key, messageId, reservedAt }
        );
      }
      // Otherwise an unconfirmed reservation is taken over later, at worst
      // sending the message twice: the queue is at-least-once anyway.
    }
  }

  /**
   * Gives a reservation back after its send failed, if it is still the one
   * this send made. Best effort.
   */
  async release(
    key: string,
    messageId: MessageId,
    reservedAt: number
  ): Promise<void> {
    try {
      await this.documents.send(
        new DeleteCommand({
          TableName: this.tableName,
          Key: idempotencyKey(key),
          ConditionExpression:
            'messageId = :id AND reservedAt = :mine AND #state = :reserved',
          ExpressionAttributeNames: { '#state': 'state' },
          ExpressionAttributeValues: {
            ':id': messageId,
            ':mine': reservedAt,
            ':reserved': 'reserved',
          },
        })
      );
    } catch {
      // Left reserved, it is taken over once it times out; taken over
      // already, it belongs to the taker.
    }
  }

  private async read(key: string): Promise<
    | {
        messageId: MessageId;
        state: string;
        reservedAt: number;
        expiresAt: number;
      }
    | undefined
  > {
    const result = await this.documents.send(
      new GetCommand({
        TableName: this.tableName,
        Key: idempotencyKey(key),
        ConsistentRead: true,
      })
    );
    const item = result.Item;
    if (!item || typeof item.messageId !== 'string') return undefined;
    return {
      messageId: item.messageId as MessageId,
      state: String(item.state),
      reservedAt: Number(item.reservedAt ?? 0),
      expiresAt: Number(item.expiresAt ?? 0),
    };
  }

  /** Takes a timed-out reservation over; answers its new `reservedAt`. */
  private async takeOver(
    key: string,
    existing: { messageId: MessageId; reservedAt: number }
  ): Promise<number | undefined> {
    const now = Date.now();
    try {
      await this.documents.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: idempotencyKey(key),
          UpdateExpression: 'SET reservedAt = :now',
          ConditionExpression:
            'messageId = :id AND reservedAt = :then AND #state = :reserved',
          ExpressionAttributeNames: { '#state': 'state' },
          ExpressionAttributeValues: {
            ':now': now,
            ':id': existing.messageId,
            ':then': existing.reservedAt,
            ':reserved': 'reserved',
          },
        })
      );
      return now;
    } catch (error) {
      if (isConditionFailure(error)) return undefined;
      throw error;
    }
  }
}
