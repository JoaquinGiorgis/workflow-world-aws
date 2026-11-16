import {
  InBandSupersededError,
  START_HOOK_ADMISSION_REJECTED,
  WorkflowWorldError,
} from '@workflow/errors';
import type {
  AnyEventRequest,
  CreateEventParams,
  Hook,
  Step,
  Wait,
  WorkflowRun,
} from '@workflow/world';
import { hookIdKey, hookKey, hookTokenKey, PREFIX, runPk } from '../keys.js';
import type {
  Append,
  Decision,
  NewEvent,
  RunState,
  TaggedItem,
} from './commit.js';
import { pointerDelete, tokenDelete } from './hook-writes.js';
import type { Item } from './items.js';
import type { Table, TransactItem } from './table.js';

const DAY_MS = 24 * 60 * 60 * 1000;

/** What a committed (or converged) create answers with. */
export interface Outcome {
  /** The stored item of the event this create answers with. */
  event?: Item;
  run?: WorkflowRun;
  step?: Step;
  hook?: Hook;
  wait?: Wait;
  stepCreated?: true;
  /** Positions this write appended to its own run (0 or 1 for the fence). */
  allocated: number;
}

/** `WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS`, 30 by default. */
export function hookRetentionLimitMs(): number {
  const days = Number(process.env.WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS ?? 30);
  if (!Number.isFinite(days) || days <= 0) {
    throw new WorkflowWorldError(
      'WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS must be a positive number',
      { status: 400 }
    );
  }
  return days * DAY_MS;
}

export function retentionLimitError(limitMs: number, startHook: boolean) {
  return new WorkflowWorldError(
    `Hook minimum retention cannot exceed ${limitMs / DAY_MS} days in the AWS World.`,
    {
      status: 400,
      ...(startHook && {
        code: START_HOOK_ADMISSION_REJECTED,
        reason: 'invalid',
      }),
    }
  );
}

/**
 * Refuses a stale in-band write before anything is decided: the in-band
 * fence (`WorldCapabilities.inBandFence`). `run_created` opens the count
 * and is never fenced; a run without counters predates slots.
 */
export function checkFence(
  runId: string,
  state: RunState,
  params: CreateEventParams | undefined
): void {
  if (!params?.inBand) return;
  const expected = params.expectedSeqInBand;
  if (
    expected === undefined ||
    !Number.isSafeInteger(expected) ||
    expected < 0
  ) {
    throw new WorkflowWorldError(
      `An in-band write to run ${runId} must carry a nonnegative integer expectedSeqInBand`,
      { status: 400 }
    );
  }
  if (typeof state.item?.seq !== 'number') return;
  if (state.seqInBand !== expected) {
    throw new InBandSupersededError(
      `In-band write on run ${runId} expected seqInBand ${expected}, but the run is at ${state.seqInBand}. Another orchestrator wrote in-band events this one has not seen; stop writing and redeliver.`,
      { seqInBand: state.seqInBand }
    );
  }
}

/** An in-band write's `createdAt` is the `occurredAt` it was sent with. */
export function eventTime(
  params: CreateEventParams | undefined
): Date | undefined {
  if (!params?.inBand || params.occurredAt === undefined) return undefined;
  const occurredAt = new Date(params.occurredAt);
  return Number.isNaN(occurredAt.getTime()) ? undefined : occurredAt;
}

/**
 * Whether the run's log holds an event for `correlationId`: of `eventType`,
 * or of any type without one.
 */
export async function hasEvent(
  table: Table,
  runId: string,
  correlationId: string,
  eventType?: string
): Promise<boolean> {
  const found = await table.collect(
    {
      KeyConditionExpression: 'PK = :pk AND begins_with(SK, :prefix)',
      FilterExpression:
        eventType === undefined
          ? 'correlationId = :c'
          : 'correlationId = :c AND eventType = :t',
      ExpressionAttributeValues: {
        ':pk': runPk(runId),
        ':prefix': PREFIX.event,
        ':c': correlationId,
        ...(eventType !== undefined && { ':t': eventType }),
      },
      ConsistentRead: true,
    },
    1
  );
  return found.length > 0;
}

/** Items of the run's partition whose sort key starts with `prefix`. */
export async function partitionItems(
  table: Table,
  runId: string,
  prefix: string
): Promise<Item[]> {
  return table.collect(
    {
      KeyConditionExpression: 'PK = :pk AND begins_with(SK, :prefix)',
      ExpressionAttributeValues: { ':pk': runPk(runId), ':prefix': prefix },
      ConsistentRead: true,
    },
    Number.MAX_SAFE_INTEGER
  );
}

/**
 * Deletes for a run that ends: its waits, and its hooks unless their
 * retention is still running, with their pointers and token claims.
 */
export async function endOfRunCleanup(
  table: Table,
  runId: string,
  keepRetained: boolean
): Promise<TransactItem[]> {
  const [hooks, waits] = await Promise.all([
    partitionItems(table, runId, PREFIX.hook),
    partitionItems(table, runId, PREFIX.wait),
  ]);
  const released = hooks.filter(
    (hook) =>
      !keepRetained ||
      !(
        typeof hook.tokenRetentionUntil === 'string' &&
        Date.parse(hook.tokenRetentionUntil) > Date.now()
      )
  );
  const claims = new Map(
    (
      await table.batchGet(
        released.map((hook) => hookTokenKey(String(hook.token)))
      )
    ).map((claim) => [String(claim.PK), claim])
  );
  const deletes: TransactItem[] = [];
  for (const hook of released) {
    const hookId = String(hook.hookId);
    deletes.push({ Delete: { Key: hookKey(runId, hookId) } });
    deletes.push(pointerDelete(runId, hookId));
    const claim = claims.get(hookTokenKey(String(hook.token)).PK);
    if (claim?.runId === runId && claim.hookId === hookId) {
      deletes.push(tokenDelete(String(hook.token), runId, hookId));
    }
  }
  for (const wait of waits) {
    deletes.push({
      Delete: { Key: { PK: String(wait.PK), SK: String(wait.SK) } },
    });
  }
  return deletes;
}

export interface Request {
  runId: string;
  data: AnyEventRequest;
  params: CreateEventParams | undefined;
  specVersion: number;
}

export function pointerCheck(hookId: string, runId: string): TransactItem {
  return {
    ConditionCheck: {
      Key: hookIdKey(hookId),
      ConditionExpression: '#runId = :runId',
      ExpressionAttributeNames: { '#runId': 'runId' },
      ExpressionAttributeValues: { ':runId': runId },
    },
  };
}

export type Own = (events: NewEvent[], extra?: Partial<Append>) => Append;

export type Answer = (
  append: Append,
  writes: TaggedItem[],
  result: Omit<Outcome, 'event' | 'allocated'>,
  cleanup?: TransactItem[]
) => Decision<Outcome>;
