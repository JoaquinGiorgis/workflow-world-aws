/**
 * Mapping between table items and World entities.
 *
 * An item holds the entity's own fields under their contract names, next
 * to the key and index attributes and some bookkeeping (`type`, the run's
 * `seq`/`seqInBand`/`rev` counters). Reading drops those, normalizes
 * binaries and parses with the contract's schema, which turns the stored
 * ISO strings back into `Date`s.
 */
import type {
  Event,
  Hook,
  ResolveData,
  Step,
  StepWithoutData,
  Wait,
  WorkflowRun,
  WorkflowRunWithoutData,
} from '@workflow/world';
import {
  EventSchema,
  HookSchema,
  StepSchema,
  WaitSchema,
  WorkflowRunSchema,
} from '@workflow/world';
import { fromStorable } from './values.js';

export type Item = Record<string, unknown>;

/** Attributes that are not part of any entity. */
const BOOKKEEPING = new Set([
  'PK',
  'SK',
  'type',
  'GSI1PK',
  'GSI1SK',
  'GSI2PK',
  'GSI2SK',
  'seq',
  'seqInBand',
  'rev',
  'expiresAt',
  'resumePayloadDigest',
]);

/** The entity fields of an item, normalized, without null values. */
export function entityFields(item: Item): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(item)) {
    if (BOOKKEEPING.has(key) || value === null || value === undefined) {
      continue;
    }
    out[key] = fromStorable(value);
  }
  return out;
}

export function runFromItem(item: Item): WorkflowRun {
  return WorkflowRunSchema.parse(entityFields(item)) as WorkflowRun;
}

/**
 * Step fields a stored step may lack, present as `undefined` on the entity
 * the way world-postgres returns every column of its row.
 */
const OPTIONAL_STEP_FIELDS: Record<string, undefined> = {
  input: undefined,
  output: undefined,
  error: undefined,
  startedAt: undefined,
  completedAt: undefined,
  retryAfter: undefined,
};

export function stepFromItem(item: Item): Step {
  return StepSchema.parse({ ...OPTIONAL_STEP_FIELDS, ...entityFields(item) });
}

export function hookFromItem(item: Item): Hook {
  return HookSchema.parse(entityFields(item));
}

export function waitFromItem(item: Item): Wait {
  return WaitSchema.parse(entityFields(item));
}

export function eventFromItem(item: Item): Event {
  return EventSchema.parse(entityFields(item));
}

export function filterRunData(
  run: WorkflowRun,
  resolveData: ResolveData
): WorkflowRun | WorkflowRunWithoutData {
  if (resolveData === 'none') {
    const {
      input: _input,
      output: _output,
      dynamicWorkflowCode: _dynamicWorkflowCode,
      ...rest
    } = run;
    return { input: undefined, output: undefined, ...rest };
  }
  return run;
}

export function filterStepData(
  step: Step,
  resolveData: ResolveData
): Step | StepWithoutData {
  if (resolveData === 'none') {
    const { input: _input, output: _output, ...rest } = step;
    return { input: undefined, output: undefined, ...rest };
  }
  return step;
}

export function filterHookData(hook: Hook, resolveData: ResolveData): Hook {
  if (resolveData === 'none' && 'metadata' in hook) {
    const { metadata: _metadata, ...rest } = hook;
    return { metadata: undefined, ...rest };
  }
  return hook;
}
