/**
 * Startup recovery, at most once per interval per table.
 *
 * Recovery (`reenqueueActiveRuns`) sends one message per run that has not
 * ended, in case the message that would continue it was lost. world-postgres
 * runs it on every start, which is cheap with one long-lived worker. With
 * SQS the queue outlives the process, and processes start often: an
 * autoscaled fleet, or a Lambda function's cold starts. So before recovering,
 * a process records it on the table (`RECOVERY` / `LAST`) on the condition
 * that no process did so within the interval, and skips recovery when the
 * condition fails. A recovery that fails gives the record back, so the next
 * start tries again.
 */
import {
  DeleteCommand,
  type DynamoDBDocumentClient,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';
import { RECOVERY_KEY } from './keys.js';

export interface RecoveryClaim {
  /** Gives the record back, after a recovery that failed. Best effort. */
  release(): Promise<void>;
}

function isConditionFailure(error: unknown): boolean {
  return (
    (error as { name?: string } | undefined)?.name ===
    'ConditionalCheckFailedException'
  );
}

export class RecoveryGuard {
  constructor(
    private readonly documents: DynamoDBDocumentClient,
    private readonly tableName: string,
    private readonly intervalSeconds: number
  ) {}

  /**
   * Claims the table's recovery for this process. Undefined when another
   * process recovered it within the interval; with an interval of 0, a
   * claim that writes nothing.
   */
  async claim(): Promise<RecoveryClaim | undefined> {
    if (this.intervalSeconds === 0) return { release: async () => {} };
    const now = Date.now();
    try {
      await this.documents.send(
        new PutCommand({
          TableName: this.tableName,
          Item: {
            ...RECOVERY_KEY,
            type: 'recovery',
            recoveredAt: now,
            // Only tidies the item away; the condition decides.
            expiresAt: Math.floor(now / 1000) + this.intervalSeconds + 60,
          },
          ConditionExpression:
            'attribute_not_exists(PK) OR recoveredAt < :cutoff',
          ExpressionAttributeValues: {
            ':cutoff': now - this.intervalSeconds * 1000,
          },
        })
      );
    } catch (error) {
      if (isConditionFailure(error)) return undefined;
      throw error;
    }
    return {
      release: async () => {
        try {
          await this.documents.send(
            new DeleteCommand({
              TableName: this.tableName,
              Key: RECOVERY_KEY,
              ConditionExpression: 'recoveredAt = :mine',
              ExpressionAttributeValues: { ':mine': now },
            })
          );
        } catch {
          // Kept, the next start skips recovery until the interval is over.
        }
      },
    };
  }
}
