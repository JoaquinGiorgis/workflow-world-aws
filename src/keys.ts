/**
 * Key layout of the single DynamoDB table.
 *
 * | Entity              | PK                       | SK                 |
 * | ------------------- | ------------------------ | ------------------ |
 * | Run                 | `RUN#<runId>`            | `META`             |
 * | Event               | `RUN#<runId>`            | `EVT#<slot>`       |
 * | Step                | `RUN#<runId>`            | `STEP#<stepId>`    |
 * | Hook                | `RUN#<runId>`            | `HOOK#<hookId>`    |
 * | Wait                | `RUN#<runId>`            | `WAIT#<waitId>`    |
 * | Snapshot            | `RUN#<runId>`            | `SNAP#<id>`        |
 * | Hook token          | `HOOKTOKEN#<token>`      | `TOKEN`            |
 * | Stream chunk        | `STREAM#<runId>#<name>`  | `CHUNK#<seq>`      |
 * | Stream end marker   | `STREAM#<runId>#<name>`  | `CLOSED`           |
 * | Queue idempotency   | `IDEMP#<key>`            | `Q`                |
 *
 * Event slots and chunk sequence numbers are zero-padded to
 * {@link SORT_NUMBER_WIDTH} digits so that the lexicographic order DynamoDB
 * sorts by is the numeric order.
 *
 * GSI1 lists runs per deployment and GSI2 per workflow name, both ordered by
 * creation time.
 */

export interface TableKey {
  PK: string;
  SK: string;
}

/** Digits in a padded event slot or chunk sequence number. */
export const SORT_NUMBER_WIDTH = 10;
/** Largest number that fits in {@link SORT_NUMBER_WIDTH} digits. */
export const MAX_SORT_NUMBER = 10 ** SORT_NUMBER_WIDTH - 1;

export const RUN_META_SK = 'META';
export const HOOK_TOKEN_SK = 'TOKEN';
export const STREAM_CLOSED_SK = 'CLOSED';
export const IDEMPOTENCY_SK = 'Q';

export const PREFIX = {
  run: 'RUN#',
  event: 'EVT#',
  step: 'STEP#',
  hook: 'HOOK#',
  wait: 'WAIT#',
  snapshot: 'SNAP#',
  hookToken: 'HOOKTOKEN#',
  stream: 'STREAM#',
  chunk: 'CHUNK#',
  idempotency: 'IDEMP#',
  runsByDeployment: 'RUNS#',
  runsByWorkflow: 'WF#',
} as const;

export const GSI1_NAME = 'GSI1';
export const GSI2_NAME = 'GSI2';

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

/** Sort key of the event at a 1-based slot of its run's log. */
export function eventSk(slot: number): string {
  if (!Number.isInteger(slot) || slot < 1) {
    throw new RangeError(`Event slots start at 1, got ${slot}`);
  }
  return `${PREFIX.event}${padSortNumber(slot)}`;
}

export function eventSlotFromSk(sk: string): number {
  return parseSortNumber(sk, PREFIX.event);
}

export function eventKey(runId: string, slot: number): TableKey {
  return { PK: runPk(runId), SK: eventSk(slot) };
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

export function waitKey(runId: string, waitId: string): TableKey {
  return {
    PK: runPk(runId),
    SK: `${PREFIX.wait}${requireComponent(waitId, 'waitId')}`,
  };
}

export function snapshotKey(runId: string, snapshotId: string): TableKey {
  return {
    PK: runPk(runId),
    SK: `${PREFIX.snapshot}${requireComponent(snapshotId, 'snapshotId')}`,
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

export function idempotencyKey(key: string): TableKey {
  return {
    PK: `${PREFIX.idempotency}${requireComponent(key, 'idempotencyKey')}`,
    SK: IDEMPOTENCY_SK,
  };
}

function creationSortKey(createdAt: Date, runId: string): string {
  if (Number.isNaN(createdAt.getTime())) {
    throw new RangeError('createdAt must be a valid date');
  }
  return `${createdAt.toISOString()}#${requireRunId(runId)}`;
}

/** GSI1 attributes: runs of a deployment, oldest first. */
export function runsByDeploymentGsi1(
  deploymentId: string,
  createdAt: Date,
  runId: string
): { GSI1PK: string; GSI1SK: string } {
  return {
    GSI1PK: `${PREFIX.runsByDeployment}${requireComponent(deploymentId, 'deploymentId')}`,
    GSI1SK: creationSortKey(createdAt, runId),
  };
}

/** GSI2 attributes: runs of a workflow, oldest first. */
export function runsByWorkflowGsi2(
  workflowName: string,
  createdAt: Date,
  runId: string
): { GSI2PK: string; GSI2SK: string } {
  return {
    GSI2PK: `${PREFIX.runsByWorkflow}${requireComponent(workflowName, 'workflowName')}`,
    GSI2SK: creationSortKey(createdAt, runId),
  };
}
