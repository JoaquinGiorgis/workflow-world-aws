/**
 * Key layout of the single DynamoDB table.
 *
 * | Entity                 | PK                       | SK                  |
 * | ---------------------- | ------------------------ | ------------------- |
 * | Run                    | `RUN#<runId>`            | `META`              |
 * | Event                  | `RUN#<runId>`            | `EVT#<eventId>`     |
 * | Step                   | `RUN#<runId>`            | `STEP#<stepId>`     |
 * | Hook                   | `RUN#<runId>`            | `HOOK#<hookId>`     |
 * | Wait                   | `RUN#<runId>`            | `WAIT#<waitId>`     |
 * | Snapshot               | `RUN#<runId>`            | `SNAPSHOT`          |
 * | Hook resume (dedup)    | `RUN#<runId>`            | `RESUME#<resumeId>` |
 * | attr_set (dedup)       | `RUN#<runId>`            | `ATTR#<corrId>`     |
 * | Start Hook admission   | `RUN#<runId>`            | `ADMISSION`         |
 * | Hook id → run          | `HOOKID#<hookId>`        | `HOOK`              |
 * | Hook token claim       | `HOOKTOKEN#<token>`      | `TOKEN`             |
 * | Stream chunk           | `STREAM#<runId>#<name>`  | `CHUNK#<seq>`       |
 * | Stream end marker      | `STREAM#<runId>#<name>`  | `CLOSED`            |
 * | Stream name (per run)  | `STREAMS#<runId>`        | `NAME#<name>`       |
 * | Queue idempotency      | `IDEMP#<key>`            | `Q`                 |
 *
 * Event ids are slots (`evnt_` and a 26-digit zero-padded position), so
 * `EVT#<eventId>` sorts in log order. Chunk sequence numbers are zero-padded
 * to {@link SORT_NUMBER_WIDTH} digits for the same reason.
 *
 * GSI1 lists every run (`RUNS`) and every hook (`HOOKS`) by id; run ids
 * are ULIDs, so id order is creation order. GSI2 lists the runs of one
 * workflow (`WF#<workflowName>`), also by id. GSI3 is sparse: it lists the
 * runs that have not ended (`ACTIVE`), by id. A run item carries its GSI3
 * attributes from its creation until the write that ends the run removes
 * them, in the same transaction, so recovery reads only active runs.
 */

export interface TableKey {
  PK: string;
  SK: string;
}

/** Digits in a padded chunk sequence number. */
export const SORT_NUMBER_WIDTH = 10;
/** Largest number that fits in {@link SORT_NUMBER_WIDTH} digits. */
export const MAX_SORT_NUMBER = 10 ** SORT_NUMBER_WIDTH - 1;

export const RUN_META_SK = 'META';
export const SNAPSHOT_SK = 'SNAPSHOT';
export const ADMISSION_SK = 'ADMISSION';
export const HOOK_ID_SK = 'HOOK';
export const HOOK_TOKEN_SK = 'TOKEN';
export const STREAM_CLOSED_SK = 'CLOSED';
export const IDEMPOTENCY_SK = 'Q';

export const PREFIX = {
  run: 'RUN#',
  event: 'EVT#',
  step: 'STEP#',
  hook: 'HOOK#',
  wait: 'WAIT#',
  resume: 'RESUME#',
  attr: 'ATTR#',
  hookId: 'HOOKID#',
  hookToken: 'HOOKTOKEN#',
  stream: 'STREAM#',
  chunk: 'CHUNK#',
  streamNames: 'STREAMS#',
  streamName: 'NAME#',
  idempotency: 'IDEMP#',
  runsByWorkflow: 'WF#',
} as const;

export const GSI1_NAME = 'GSI1';
export const GSI2_NAME = 'GSI2';
export const GSI3_NAME = 'GSI3';
/** GSI1 partition holding every run. */
export const ALL_RUNS_GSI1PK = 'RUNS';
/** GSI1 partition holding every hook. */
export const ALL_HOOKS_GSI1PK = 'HOOKS';
/** GSI3 partition holding the runs that have not ended. */
export const ACTIVE_RUNS_GSI3PK = 'ACTIVE';
/** The attributes that put a run item in GSI3. */
export const ACTIVE_RUN_INDEX_ATTRIBUTES = ['GSI3PK', 'GSI3SK'] as const;

function requireComponent(value: string, what: string): string {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError(`${what} must be a non-empty string`);
  }
  return value;
}

/** A run id is followed by other components in stream keys, so no `#`. */
function requireRunId(runId: string): string {
  requireComponent(runId, 'runId');
  if (runId.includes('#')) {
    throw new TypeError(`runId must not contain "#": ${runId}`);
  }
  return runId;
}

/** Zero-pads a non-negative integer to {@link SORT_NUMBER_WIDTH} digits. */
export function padSortNumber(value: number): string {
  if (!Number.isInteger(value) || value < 0 || value > MAX_SORT_NUMBER) {
    throw new RangeError(
      `Expected an integer between 0 and ${MAX_SORT_NUMBER}, got ${value}`
    );
  }
  return String(value).padStart(SORT_NUMBER_WIDTH, '0');
}

function parseSortNumber(sk: string, prefix: string): number {
  const digits = sk.slice(prefix.length);
  if (
    !sk.startsWith(prefix) ||
    digits.length !== SORT_NUMBER_WIDTH ||
    !/^\d+$/.test(digits)
  ) {
    throw new TypeError(`Not a ${prefix} sort key: ${sk}`);
  }
  return Number(digits);
}

export function runPk(runId: string): string {
  return `${PREFIX.run}${requireRunId(runId)}`;
}

export function runMetaKey(runId: string): TableKey {
  return { PK: runPk(runId), SK: RUN_META_SK };
}

/** Sort key of an event: its id, which sorts in log order. */
export function eventSk(eventId: string): string {
  return `${PREFIX.event}${requireComponent(eventId, 'eventId')}`;
}

export function eventIdFromSk(sk: string): string {
  if (!sk.startsWith(PREFIX.event) || sk.length === PREFIX.event.length) {
    throw new TypeError(`Not an event sort key: ${sk}`);
  }
  return sk.slice(PREFIX.event.length);
}

export function eventKey(runId: string, eventId: string): TableKey {
  return { PK: runPk(runId), SK: eventSk(eventId) };
}

export function stepKey(runId: string, stepId: string): TableKey {
  return {
    PK: runPk(runId),
    SK: `${PREFIX.step}${requireComponent(stepId, 'stepId')}`,
  };
}

export function hookKey(runId: string, hookId: string): TableKey {
  return {
    PK: runPk(runId),
    SK: `${PREFIX.hook}${requireComponent(hookId, 'hookId')}`,
  };
}

/** A wait is keyed by its correlation id within the run. */
export function waitKey(runId: string, correlationId: string): TableKey {
  return {
    PK: runPk(runId),
    SK: `${PREFIX.wait}${requireComponent(correlationId, 'correlationId')}`,
  };
}

/** The run's VM snapshot; each save replaces the previous one. */
export function snapshotKey(runId: string): TableKey {
  return { PK: runPk(runId), SK: SNAPSHOT_SK };
}

/** Marks a `hook_received` resume id as used, for `hookResumeDedup`. */
export function resumeKey(runId: string, resumeId: string): TableKey {
  return {
    PK: runPk(runId),
    SK: `${PREFIX.resume}${requireComponent(resumeId, 'resumeId')}`,
  };
}

/** Marks a workflow `attr_set` correlation id as used. */
export function attrSetKey(runId: string, correlationId: string): TableKey {
  return {
    PK: runPk(runId),
    SK: `${PREFIX.attr}${requireComponent(correlationId, 'correlationId')}`,
  };
}

/** The start Hook admission decision of a candidate run id. */
export function admissionKey(runId: string): TableKey {
  return { PK: runPk(runId), SK: ADMISSION_SK };
}

/** Points from a hook id to the run that owns the hook. */
export function hookIdKey(hookId: string): TableKey {
  return {
    PK: `${PREFIX.hookId}${requireComponent(hookId, 'hookId')}`,
    SK: HOOK_ID_SK,
  };
}

/** Item that reserves a hook token across all runs. */
export function hookTokenKey(token: string): TableKey {
  return {
    PK: `${PREFIX.hookToken}${requireComponent(token, 'token')}`,
    SK: HOOK_TOKEN_SK,
  };
}

export function streamPk(runId: string, name: string): string {
  return `${PREFIX.stream}${requireRunId(runId)}#${requireComponent(name, 'stream name')}`;
}

/** Sort key of a stream chunk; sequence numbers are 0-based. */
export function chunkSk(seq: number): string {
  return `${PREFIX.chunk}${padSortNumber(seq)}`;
}

export function chunkSeqFromSk(sk: string): number {
  return parseSortNumber(sk, PREFIX.chunk);
}

export function chunkKey(runId: string, name: string, seq: number): TableKey {
  return { PK: streamPk(runId, name), SK: chunkSk(seq) };
}

export function streamClosedKey(runId: string, name: string): TableKey {
  return { PK: streamPk(runId, name), SK: STREAM_CLOSED_SK };
}

/** The partition listing a run's stream names, for `streams.list`. */
export function streamNamesPk(runId: string): string {
  return `${PREFIX.streamNames}${requireRunId(runId)}`;
}

/** Records that the run has a stream called `name`. */
export function streamNameKey(runId: string, name: string): TableKey {
  return {
    PK: streamNamesPk(runId),
    SK: `${PREFIX.streamName}${requireComponent(name, 'stream name')}`,
  };
}

export function idempotencyKey(key: string): TableKey {
  return {
    PK: `${PREFIX.idempotency}${requireComponent(key, 'idempotencyKey')}`,
    SK: IDEMPOTENCY_SK,
  };
}

/**
 * Index attributes of a new run item: GSI1 (all runs), GSI2 (per workflow)
 * and GSI3 (active runs), which a new run is.
 */
export function runIndexKeys(
  runId: string,
  workflowName: string
): {
  GSI1PK: string;
  GSI1SK: string;
  GSI2PK: string;
  GSI2SK: string;
  GSI3PK: string;
  GSI3SK: string;
} {
  return {
    GSI1PK: ALL_RUNS_GSI1PK,
    GSI1SK: requireRunId(runId),
    GSI2PK: `${PREFIX.runsByWorkflow}${requireComponent(workflowName, 'workflowName')}`,
    GSI2SK: runId,
    ...activeRunIndexKeys(runId),
  };
}

/** GSI3 attributes of a run that has not ended. */
export function activeRunIndexKeys(runId: string): {
  GSI3PK: string;
  GSI3SK: string;
} {
  return { GSI3PK: ACTIVE_RUNS_GSI3PK, GSI3SK: requireRunId(runId) };
}

/** Index attributes of a hook item: GSI1 (all hooks). */
export function hookIndexKeys(hookId: string): {
  GSI1PK: string;
  GSI1SK: string;
} {
  return {
    GSI1PK: ALL_HOOKS_GSI1PK,
    GSI1SK: requireComponent(hookId, 'hookId'),
  };
}
