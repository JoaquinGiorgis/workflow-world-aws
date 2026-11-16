/**
 * Deduplication of `queue()` sends by `idempotencyKey`, on the World's table
 * (`IDEMP#<key>` / `Q`).
 *
 * A send first reserves the key with a conditional put that also records
 * the message id it will send under. The put that wins sends the message
 * and then marks the reservation sent; a send that loses answers with the
 * stored message id and sends nothing. If the winner's send fails, it
 * deletes its reservation so a retry can send. If it dies between the two,
 * the reservation stays unconfirmed: a later send under the key waits for
 * it, and takes it over (sending under the same message id) once it is
 * older than {@link RESERVATION_TIMEOUT_MS}. A key is kept for
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

/** An unconfirmed reservation older than this is taken over. */
export const RESERVATION_TIMEOUT_MS = 30_000;

/** How often a send polls a reservation another sender has not confirmed. */
const RESERVATION_POLL_MS = 100;

export interface Reservation {
  /** Whether this send must send the message (it holds the reservation). */
  owner: boolean;
  /** The message id to send under, or the one an earlier send used. */
  messageId: MessageId;
}

function isConditionFailure(error: unknown): boolean {
  return (
    (error as { name?: string } | undefined)?.name ===
    'ConditionalCheckFailedException'
  );
}

export class IdempotencyStore {
  constructor(
    private readonly documents: DynamoDBDocumentClient,
    private readonly tableName: string
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
        return { owner: true, messageId };
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
      if (await this.takeOver(key, existing)) {
        return { owner: true, messageId: existing.messageId };
      }
    }
  }

  /** Marks the key's message sent. Best effort: see the module comment. */
  async confirm(key: string, messageId: MessageId): Promise<void> {
    try {
      await this.documents.send(
        new UpdateCommand({
          TableName: this.tableName,
          Key: idempotencyKey(key),
          UpdateExpression: 'SET #state = :sent',
          ConditionExpression: 'messageId = :id',
          ExpressionAttributeNames: { '#state': 'state' },
          ExpressionAttributeValues: { ':sent': 'sent', ':id': messageId },
        })
      );
    } catch {
      // An unconfirmed reservation is taken over later, at worst sending
      // the message twice: the queue is at-least-once anyway.
    }
  }

  /** Gives a reservation back after its send failed. Best effort. */
  async release(key: string, messageId: MessageId): Promise<void> {
    try {
      await this.documents.send(
        new DeleteCommand({
          TableName: this.tableName,
          Key: idempotencyKey(key),
          ConditionExpression: 'messageId = :id AND #state = :reserved',
          ExpressionAttributeNames: { '#state': 'state' },
          ExpressionAttributeValues: {
            ':id': messageId,
            ':reserved': 'reserved',
          },
        })
      );
    } catch {
      // Left reserved, it is taken over once it times out.
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

  private async takeOver(
    key: string,
    existing: { messageId: MessageId; reservedAt: number }
  ): Promise<boolean> {
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
            ':now': Date.now(),
            ':id': existing.messageId,
            ':then': existing.reservedAt,
            ':reserved': 'reserved',
          },
        })
      );
      return true;
    } catch (error) {
      if (isConditionFailure(error)) return false;
      throw error;
    }
  }
}
