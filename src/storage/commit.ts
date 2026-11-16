/**
 * The event commit: read, decide, write in one transaction, retry on
 * conflict.
 *
 * Every write to a run goes through its run item (`RUN#<runId>` / `META`),
 * which carries three counters:
 *
 * - `seq`: the highest slot in the run's log. The events a write appends
 *   take the next slots, `seq + 1`, `seq + 2`, ..., and `seq` moves past
 *   them in the same transaction, so positions are allocated at the commit
 *   and only by a write that commits: dense and unique.
 * - `seqInBand`: the positions taken by in-band writes (the in-band fence).
 * - `rev`: moved by every write to the run's partition. The transaction is
 *   conditioned on the `rev` the decision read, so a decision made on state
 *   another writer has since changed never commits: the transaction is
 *   cancelled and the write is decided again on fresh reads.
 *
 * The decision itself (`decide`) reads what it needs consistently and
 * returns the items to write. Items outside the run's partition (hook
 * tokens, the hook id pointer, another run's items) carry their own
 * conditions. When a transaction is cancelled, the reasons DynamoDB gives
 * (`CancellationReasons`, with the item a failed condition saw when the item
 * asked for `ALL_OLD`) are handed to the next decision by tag.
 */
import { WorkflowWorldError } from '@workflow/errors';
import { IN_BAND_SEQ_AT_RUN_CREATION, slotToEventId } from '@workflow/world';
import { monotonicFactory } from 'ulid';
import { eventKey, runMetaKey } from '../keys.js';
import {
  DYNAMODB_ITEM_BYTES,
  MAX_ITEM_BYTES,
  PayloadTooLargeError,
} from './errors.js';
import type { Item } from './items.js';
import { debugLog } from './log.js';
import {
  MAX_TRANSACTION_ITEMS,
  sleep,
  type Table,
  type TransactItem,
} from './table.js';
import { sizeOf, toStorable } from './values.js';

/** A run's counters as a decision read them. */
export interface RunState {
  /** The run item, or undefined when the run does not exist. */
  item: Item | undefined;
  seq: number;
  seqInBand: number;
  /** Undefined for a run item written without one (legacy rows). */
  rev: number | undefined;
}

export function runState(item: Item | undefined): RunState {
  return {
    item,
    seq: typeof item?.seq === 'number' ? item.seq : 0,
    seqInBand: typeof item?.seqInBand === 'number' ? item.seqInBand : 0,
    rev: typeof item?.rev === 'number' ? item.rev : undefined,
  };
}

export async function readRun(table: Table, runId: string): Promise<RunState> {
  return runState(await table.get(runMetaKey(runId)));
}

/** An event to append; the commit gives it its id and position. */
export interface NewEvent {
  eventType: string;
  correlationId?: string;
  eventData?: unknown;
  specVersion?: number;
  createdAt?: Date;
  resumeId?: string;
  resumePayloadDigest?: string;
}

/** What a write does to one run: the events it appends, and its run item. */
export interface Append {
  runId: string;
  state: RunState;
  events: NewEvent[];
  /** Run item attributes to set. */
  set?: Record<string, unknown>;
  /** Run item attributes to remove. */
  remove?: string[];
  /** An in-band write: takes one in-band position if it appends anything. */
  inBand?: boolean;
  /** Creates the run: its item, written only if it does not exist. */
  create?: Item;
  /**
   * Events of a run created before slot-numbered ids, which keep minting
   * `wevt_<ulid>` ids and never move `seq`.
   */
  legacyIds?: boolean;
}

export interface TaggedItem {
  item: TransactItem;
  /** Names the item in the next decision's {@link Failure}. */
  tag?: string;
}

export interface Plan<R> {
  appends: Append[];
  writes?: TaggedItem[];
  /**
   * Deletes that may be left out of the transaction when it is full (over
   * 100 items) and run right after it instead. Only for items the World
   * already treats as gone (a finished run's hooks and waits).
   */
  cleanup?: TransactItem[];
  done: (committed: Committed) => R | Promise<R>;
}

export interface Committed {
  /** The stored event items, per run, in log order. */
  events: Map<string, Item[]>;
}

export type Decision<R> = { plan: Plan<R> } | { result: R };

/** Why the previous attempt's transaction was cancelled, by item tag. */
export interface Failure {
  reasons: Map<string, { code: string; item?: Item }>;
}

export interface Attempt {
  attempt: number;
  failure?: Failure;
}

export interface RetryOptions {
  /** Attempts before giving up with a 503. */
  maxAttempts: number;
  /** Attempts retried at once, before backing off. */
  immediateAttempts: number;
  baseDelayMs: number;
  maxDelayMs: number;
}

export const DEFAULT_RETRY: RetryOptions = {
  maxAttempts: 40,
  immediateAttempts: 2,
  baseDelayMs: 5,
  maxDelayMs: 250,
};

const legacyEventUlid = monotonicFactory();

/** The DynamoDB reasons that mean "decide again". */
const RETRYABLE_REASONS = new Set([
  'ConditionalCheckFailed',
  'TransactionConflict',
]);

/**
 * Decides and commits a write, retrying while another writer wins the
 * race. `decide` must be repeatable: it runs again, on fresh reads, after
 * every cancelled attempt.
 */
export async function commit<R>(
  table: Table,
  what: string,
  decide: (attempt: Attempt) => Promise<Decision<R>>,
  retry: RetryOptions = DEFAULT_RETRY
): Promise<R> {
  let failure: Failure | undefined;
  for (let attempt = 0; attempt < retry.maxAttempts; attempt++) {
    if (attempt >= retry.immediateAttempts) {
      // Full jitter, so writers that keep colliding spread out.
      const cap = Math.min(
        retry.maxDelayMs,
        retry.baseDelayMs * 2 ** (attempt - retry.immediateAttempts)
      );
      await sleep(Math.random() * cap);
    }
    const decision = await decide({ attempt, failure });
    if ('result' in decision) return decision.result;
    const { plan } = decision;

    const now = new Date();
    const committed: Committed = { events: new Map() };
    const tagged: TaggedItem[] = [];
    for (const append of plan.appends) {
      tagged.push(...appendItems(append, now, committed));
    }
    tagged.push(...(plan.writes ?? []));
    const room = MAX_TRANSACTION_ITEMS - tagged.length;
    const cleanup = plan.cleanup ?? [];
    const inline = cleanup.slice(0, Math.max(0, room));
    const deferred = cleanup.slice(inline.length);
    const items = [...tagged.map((entry) => entry.item), ...inline];

    try {
      await table.transact(items);
    } catch (error) {
      const reasons = cancellationReasons(error);
      if (reasons === undefined) throw error;
      if (itemTooLarge(error)) {
        throw new PayloadTooLargeError(
          `An item of ${what}`,
          undefined,
          DYNAMODB_ITEM_BYTES
        );
      }
      // No reasons at all: an emulator that does not report them. Deciding
      // again is always safe; a permanent error then exhausts the attempts.
      if (
        reasons.size > 0 &&
        ![...reasons].some((reason) => RETRYABLE_REASONS.has(reason))
      ) {
        throw error;
      }
      failure = { reasons: new Map() };
      tagged.forEach((entry, index) => {
        const reason = reasonAt(error, index);
        if (entry.tag && reason) failure?.reasons.set(entry.tag, reason);
      });
      continue;
    }

    await runDeferredCleanup(table, what, deferred);
    return plan.done(committed);
  }
  throw new WorkflowWorldError(
    `Could not commit ${what} after ${retry.maxAttempts} attempts: other writers kept changing the run`,
    { status: 503 }
  );
}

function appendItems(
  append: Append,
  now: Date,
  committed: Committed
): TaggedItem[] {
  const { runId, state, events } = append;
  const items: TaggedItem[] = [];
  const stored: Item[] = [];
  events.forEach((event, i) => {
    const eventId = append.legacyIds
      ? `wevt_${legacyEventUlid()}`
      : slotToEventId(state.seq + i + 1);
    const item = toStorable({
      ...eventKey(runId, eventId),
      type: 'event',
      runId,
      eventId,
      eventType: event.eventType,
      correlationId: event.correlationId,
      eventData: event.eventData,
      specVersion: event.specVersion,
      createdAt: event.createdAt ?? now,
      resumeId: event.resumeId,
      resumePayloadDigest: event.resumePayloadDigest,
    }) as Item;
    stored.push(item);
    items.push({
      item: {
        Put: { Item: item, ConditionExpression: 'attribute_not_exists(PK)' },
      },
    });
  });
  committed.events.set(runId, [
    ...(committed.events.get(runId) ?? []),
    ...stored,
  ]);

  const seq = append.legacyIds ? state.seq : state.seq + events.length;
  const seqInBand =
    append.inBand && events.length > 0 ? state.seqInBand + 1 : state.seqInBand;

  if (append.create) {
    items.unshift({
      tag: `run:${runId}`,
      item: {
        Put: {
          Item: toStorable({
            ...append.create,
            seq,
            seqInBand: IN_BAND_SEQ_AT_RUN_CREATION,
            rev: 1,
          }) as Item,
          ConditionExpression: 'attribute_not_exists(PK)',
          ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
        },
      },
    });
    return items;
  }

  // The run item carries input, output, error, attributes and the start
  // Hook together: an update can grow it over the limit although every
  // value fits on its own.
  if (append.set && state.item) {
    const size = sizeOf({ ...state.item, ...(toStorable(append.set) as Item) });
    if (size > MAX_ITEM_BYTES) {
      throw new PayloadTooLargeError(`The run item of ${runId}`, size);
    }
  }
  const names: Record<string, string> = { '#rev': 'rev' };
  const values: Record<string, unknown> = {
    ':rev1': (state.rev ?? 0) + 1,
  };
  const sets = ['#rev = :rev1'];
  if (!append.legacyIds && state.item && typeof state.item.seq === 'number') {
    names['#seq'] = 'seq';
    names['#seqInBand'] = 'seqInBand';
    values[':seq'] = seq;
    values[':seqInBand'] = seqInBand;
    sets.push('#seq = :seq', '#seqInBand = :seqInBand');
  }
  let n = 0;
  for (const [name, value] of Object.entries(append.set ?? {})) {
    if (value === undefined) continue;
    names[`#a${n}`] = name;
    values[`:a${n}`] = toStorable(value);
    sets.push(`#a${n} = :a${n}`);
    n++;
  }
  const removes = (append.remove ?? []).map((name) => {
    names[`#a${n}`] = name;
    return `#a${n++}`;
  });
  let condition: string;
  if (state.rev === undefined) {
    condition = 'attribute_exists(PK) AND attribute_not_exists(#rev)';
  } else {
    condition = '#rev = :rev';
    values[':rev'] = state.rev;
  }
  items.unshift({
    tag: `run:${runId}`,
    item: {
      Update: {
        Key: runMetaKey(runId),
        UpdateExpression: `SET ${sets.join(', ')}${removes.length ? ` REMOVE ${removes.join(', ')}` : ''}`,
        ConditionExpression: condition,
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
      },
    },
  });
  return items;
}

/**
 * The reason codes of a cancelled transaction, or undefined for any other
 * error. A conflict with another transaction in flight surfaces as
 * `TransactionConflictException` on some emulators; it is retryable too.
 */
function cancellationReasons(error: unknown): Set<string> | undefined {
  const name = (error as { name?: string } | undefined)?.name;
  if (name === 'TransactionConflictException') {
    return new Set(['TransactionConflict']);
  }
  if (name !== 'TransactionCanceledException') return undefined;
  const reasons =
    (error as { CancellationReasons?: { Code?: string }[] })
      .CancellationReasons ?? [];
  return new Set(
    reasons
      .map((reason) => reason.Code)
      .filter((code): code is string => !!code && code !== 'None')
  );
}

function reasonAt(
  error: unknown,
  index: number
): { code: string; item?: Item } | undefined {
  const reason = (
    error as { CancellationReasons?: { Code?: string; Item?: Item }[] }
  ).CancellationReasons?.[index];
  if (!reason?.Code || reason.Code === 'None') return undefined;
  return {
    code: reason.Code,
    ...(reason.Item && { item: unmarshallItem(reason.Item) }),
  };
}

/**
 * Errors are not unmarshalled by the document client, so the item a failed
 * condition saw arrives as raw attribute values.
 */
function unmarshallItem(raw: Record<string, unknown>): Item {
  const item: Item = {};
  for (const [name, value] of Object.entries(raw)) {
    item[name] = unmarshallValue(value);
  }
  return item;
}

function unmarshallValue(value: unknown): unknown {
  if (value === null || typeof value !== 'object') return value;
  const [type, inner] = Object.entries(value)[0] ?? [];
  switch (type) {
    case 'S':
      return inner;
    case 'N':
      return Number(inner);
    case 'BOOL':
      return inner;
    case 'NULL':
      return null;
    case 'B':
      return inner;
    case 'L':
      return (inner as unknown[]).map(unmarshallValue);
    case 'M':
      return unmarshallItem(inner as Record<string, unknown>);
    default:
      return inner;
  }
}

/**
 * Runs the cleanup left out of a committed transaction. The write it
 * belongs to has committed, so a failure here is not the write's: the reads
 * already treat a finished run's hooks and waits as gone, and what is left
 * behind is only storage. It is logged under DEBUG and the write answers
 * normally.
 */
async function runDeferredCleanup(
  table: Table,
  what: string,
  deferred: TransactItem[]
): Promise<void> {
  try {
    for (const entry of deferred) await runCleanup(table, entry);
  } catch (error) {
    debugLog('deferred cleanup failed after the commit', {
      write: what,
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

/**
 * Whether DynamoDB cancelled the transaction because an item (after an
 * update, say) went over its 400 KB limit.
 */
function itemTooLarge(error: unknown): boolean {
  const reasons =
    (error as { CancellationReasons?: { Code?: string; Message?: string }[] })
      .CancellationReasons ?? [];
  return reasons.some(
    (reason) =>
      reason.Code === 'ValidationError' &&
      /item size/i.test(reason.Message ?? '')
  );
}

/** A deferred cleanup delete; one that no longer applies is skipped. */
async function runCleanup(table: Table, entry: TransactItem): Promise<void> {
  if (!('Delete' in entry)) return;
  try {
    await table.transact([entry]);
  } catch (error) {
    if (cancellationReasons(error) === undefined) throw error;
  }
}
