/**
 * `events.create` for every event type but `run_created`: the validations
 * world-postgres makes, in the same order and with the same errors, decided
 * on consistent reads and committed with the run's counters (see
 * `commit.ts`). Hook writes are in `hook-events.ts`, runs created before
 * event sourcing in `legacy.ts`.
 */
import {
  EntityConflictError,
  HookNotFoundError,
  RunExpiredError,
  RunNotSupportedError,
  TooEarlyError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import type { AttributeChange } from '@workflow/world';
import {
  applyAttributeChanges,
  isChildEntityCreationEvent,
  isHookEventRequiringExistence,
  isLegacySpecVersion,
  isTerminalRunEventType,
  isTerminalStepStatus,
  isTerminalWorkflowRunStatus,
  requiresNewerWorld,
  SPEC_VERSION_MAX_SUPPORTED,
  SPEC_VERSION_SINGLE_ORCHESTRATOR,
  validateAttributeChanges,
} from '@workflow/world';
import {
  attrSetKey,
  hookIdKey,
  hookKey,
  hookTokenKey,
  stepKey,
  waitKey,
} from '../keys.js';
import {
  type Append,
  type Attempt,
  commit,
  type Decision,
  type NewEvent,
  type Plan,
  readRun,
  type TaggedItem,
} from './commit.js';
import type { StorageContext } from './context.js';
import {
  checkFence,
  endOfRunCleanup,
  eventTime,
  hasEvent,
  type Outcome,
  pointerCheck,
  type Request,
} from './create-support.js';
import { decideHookCreated, forceClaimRefusal } from './hook-events.js';
import { tokenDelete } from './hook-writes.js';
import { type Item, runFromItem, stepFromItem, waitFromItem } from './items.js';
import { decideLegacy } from './legacy.js';
import { attributesOf } from './runs.js';
import type { TransactItem } from './table.js';
import { toStorable } from './values.js';

/** Tags the hook id pointer delete of a `hook_disposed`. */
function disposalTag(hookId: string): string {
  return `dispose:${hookId}`;
}

function stepItem(step: Record<string, unknown>): Item {
  return toStorable(step) as Item;
}

/** Decides and commits every event type but `run_created`. */
export function createEventForRun(
  context: StorageContext,
  request: Request
): Promise<Outcome> {
  const { table } = context;
  const { runId, data } = request;
  return commit<Outcome>(
    table,
    `${data.eventType} for run "${runId}"`,
    (attempt) => decide(context, request, attempt),
    context.retry
  );
}

async function decide(
  context: StorageContext,
  { runId, data, params, specVersion }: Request,
  attempt: Attempt
): Promise<Decision<Outcome>> {
  const { table } = context;
  const state = await readRun(table, runId);
  checkFence(runId, state, params);
  const createdAt = eventTime(params);
  const now = new Date();
  const runItem = state.item;

  // Same skip as world-postgres: step_completed and step_retrying only
  // touch a running step, which may finish on any run.
  const validatesRun = !['step_completed', 'step_retrying'].includes(
    data.eventType
  );
  const currentRun = validatesRun ? runItem : undefined;
  const runStatus = String(currentRun?.status ?? '');
  const runSpec =
    typeof currentRun?.specVersion === 'number'
      ? currentRun.specVersion
      : undefined;

  if (currentRun) {
    if (requiresNewerWorld(runSpec)) {
      throw new RunNotSupportedError(
        runSpec as number,
        SPEC_VERSION_MAX_SUPPORTED
      );
    }
    if (isLegacySpecVersion(runSpec)) {
      return decideLegacy(table, runId, state, data);
    }
  }
  if (
    !runItem &&
    (data.eventType === 'attr_set' || data.eventType === 'run_started')
  ) {
    throw new WorkflowRunNotFoundError(runId);
  }

  const own = (events: NewEvent[], extra: Partial<Append> = {}): Append => {
    if (!runItem) throw new WorkflowRunNotFoundError(runId);
    return {
      runId,
      state,
      events: events.map((event) => ({ specVersion, createdAt, ...event })),
      inBand: params?.inBand === true,
      ...extra,
    };
  };
  const event = (
    eventData: unknown = 'eventData' in data ? data.eventData : undefined
  ): NewEvent => ({
    eventType: data.eventType,
    correlationId: data.correlationId,
    eventData,
  });
  /** The common shape: own events, then the answer from the last one. */
  const answer = (
    append: Append,
    writes: TaggedItem[],
    result: Omit<Outcome, 'event' | 'allocated'>,
    cleanup?: TransactItem[]
  ): Decision<Outcome> => ({
    plan: {
      appends: [append],
      writes,
      cleanup,
      done: (committed) => {
        const events = committed.events.get(runId) ?? [];
        return {
          ...result,
          event: events.at(-1),
          allocated: events.length > 0 ? 1 : 0,
        };
      },
    } satisfies Plan<Outcome>,
  });

  const createsChildEntity = isChildEntityCreationEvent(data);
  const lazyStepStart = createsChildEntity && data.eventType === 'step_started';

  // Terminal run.
  if (currentRun && isTerminalWorkflowRunStatus(runStatus)) {
    if (data.eventType === 'run_cancelled' && runStatus === 'cancelled') {
      return answer(own([event()]), [], { run: runFromItem(currentRun) });
    }
    if (data.eventType === 'run_started') {
      throw new RunExpiredError(
        `Workflow run "${runId}" is already in terminal state "${runStatus}"`
      );
    }
    if (isTerminalRunEventType(data.eventType)) {
      throw new EntityConflictError(
        `Cannot transition run from terminal state "${runStatus}"`
      );
    }
    if (createsChildEntity) {
      throw new EntityConflictError(
        `Cannot create new entities on run in terminal state "${runStatus}"`
      );
    }
    if (data.eventType === 'attr_set') {
      throw new EntityConflictError(
        `Cannot set attributes on run in terminal state "${runStatus}"`
      );
    }
  }

  // Step ordering and terminal state.
  let validatedStep: Item | undefined;
  if (
    (data.eventType === 'step_started' || data.eventType === 'step_retrying') &&
    data.correlationId
  ) {
    validatedStep = await table.get(stepKey(runId, data.correlationId));
    if (!validatedStep && !lazyStepStart) {
      throw new WorkflowWorldError(`Step "${data.correlationId}" not found`);
    }
    if (lazyStepStart && validatedStep) {
      throw new EntityConflictError(
        `Step "${data.correlationId}" already created`
      );
    }
    if (validatedStep) {
      const status = String(validatedStep.status);
      if (isTerminalStepStatus(status)) {
        throw new EntityConflictError(
          `Cannot modify step in terminal state "${status}"`
        );
      }
      if (currentRun && isTerminalWorkflowRunStatus(runStatus)) {
        if (status !== 'running' || data.eventType === 'step_started') {
          throw new RunExpiredError(
            `Cannot ${data.eventType === 'step_started' ? 'start' : 'modify non-running'} step on run in terminal state "${runStatus}"`
          );
        }
      }
    }
  }

  // Hook existence, by hook id across runs (as world-postgres).
  let liveHook: { pointer: Item; hook?: Item } | undefined;
  if (isHookEventRequiringExistence(data.eventType) && data.correlationId) {
    const pointer = await table.get(hookIdKey(data.correlationId));
    if (!pointer) {
      if (data.eventType === 'hook_received') {
        const refusal = await forceClaimRefusal(
          table,
          runId,
          data.correlationId,
          'eventData' in data ? data.eventData : undefined
        );
        if (refusal) throw refusal;
      }
      // The hook was there when the previous attempt decided, and another
      // disposal took it first: world-postgres answers that race with a
      // conflict, not with a missing hook.
      if (
        data.eventType === 'hook_disposed' &&
        attempt.failure?.reasons.has(disposalTag(data.correlationId))
      ) {
        throw new EntityConflictError(
          `Hook "${data.correlationId}" already disposed`
        );
      }
      throw new HookNotFoundError(data.correlationId);
    }
    liveHook = {
      pointer,
      hook: await table.get(hookKey(String(pointer.runId), data.correlationId)),
    };
  }

  switch (data.eventType) {
    case 'run_started': {
      const run = runItem as Item;
      if (run.status === 'running') {
        return { result: { run: runFromItem(run), allocated: 0 } };
      }
      const set = { status: 'running', startedAt: now, updatedAt: now };
      return answer(own([event(undefined)], { set }), [], {
        run: runFromItem({ ...run, ...(toStorable(set) as Item) }),
      });
    }

    case 'run_completed':
    case 'run_failed':
    case 'run_cancelled': {
      const run = runItem;
      if (!run) throw new WorkflowRunNotFoundError(runId);
      const set: Record<string, unknown> =
        data.eventType === 'run_completed'
          ? { status: 'completed', output: data.eventData.output }
          : data.eventType === 'run_failed'
            ? {
                status: 'failed',
                error: data.eventData.error,
                errorCode: data.eventData.errorCode,
              }
            : { status: 'cancelled' };
      Object.assign(set, { completedAt: now, updatedAt: now });
      const cleanup = await endOfRunCleanup(table, runId, true);
      return answer(
        own([event()], { set }),
        [],
        { run: runFromItem({ ...run, ...(toStorable(set) as Item) }) },
        cleanup
      );
    }

    case 'attr_set': {
      const run = runItem as Item;
      const { allowReservedAttributes, writer } = data.eventData;
      const changes = data.eventData.changes as AttributeChange[];
      const writes: TaggedItem[] = [];
      if (data.correlationId && writer.type === 'workflow') {
        const marker = attrSetKey(runId, data.correlationId);
        if (await table.get(marker)) {
          throw new EntityConflictError(
            `attr_set for correlationId "${data.correlationId}" already exists in run "${runId}"`
          );
        }
        writes.push({
          item: {
            Put: {
              Item: { ...marker, type: 'attr_set' },
              ConditionExpression: 'attribute_not_exists(PK)',
            },
          },
        });
      }
      const existing = attributesOf(run);
      validateAttributeChanges(changes, {
        existingKeys: Object.keys(existing),
        allowReservedAttributes: allowReservedAttributes === true,
      });
      const attributes = applyAttributeChanges(existing, changes);
      const set = { attributes, updatedAt: now };
      return answer(own([event()], { set }), writes, {
        run: runFromItem({ ...run, ...(toStorable(set) as Item) }),
      });
    }

    case 'step_created': {
      const stepId = data.correlationId;
      const existing = await table.get(stepKey(runId, stepId));
      if (existing) {
        // A step with no event at all (left by a write that did not use one
        // transaction) is completed by writing its step_created; otherwise
        // this is a duplicate. Any event counts: on a single-orchestrator
        // run a step can be created by its step_started alone.
        if (await hasEvent(table, runId, stepId)) {
          throw new EntityConflictError(
            `step_created for correlationId "${stepId}" already exists in run "${runId}"`
          );
        }
        return answer(own([event()]), [], { step: stepFromItem(existing) });
      }
      const step = stepItem({
        ...stepKey(runId, stepId),
        type: 'step',
        runId,
        stepId,
        stepName: data.eventData.stepName,
        status: 'pending',
        input: data.eventData.input,
        attempt: 0,
        specVersion,
        createdAt: now,
        updatedAt: now,
      });
      return answer(
        own([event()]),
        [
          {
            item: {
              Put: {
                Item: step,
                ConditionExpression: 'attribute_not_exists(PK)',
              },
            },
          },
        ],
        { step: stepFromItem(step) }
      );
    }

    case 'step_started': {
      const stepId = data.correlationId;
      const events: NewEvent[] = [];
      let base = validatedStep;
      // A single-orchestrator run (spec >= 9) records an inline step's
      // creation on its step_started alone.
      const creationOnStart =
        lazyStepStart && (runSpec ?? 0) >= SPEC_VERSION_SINGLE_ORCHESTRATOR;
      const eventData = data.eventData as
        | (Record<string, unknown> & { input?: unknown; stepName?: string })
        | undefined;
      if (lazyStepStart && !validatedStep) {
        base = {
          ...stepKey(runId, stepId),
          type: 'step',
          runId,
          stepId,
          stepName: eventData?.stepName,
          input: eventData?.input,
          status: 'pending',
          attempt: 0,
          specVersion,
          createdAt: now.toISOString(),
          updatedAt: now.toISOString(),
        };
        if (!creationOnStart) {
          events.push({
            eventType: 'step_created',
            correlationId: stepId,
            eventData: {
              stepName: eventData?.stepName,
              input: eventData?.input,
            },
          });
        }
      }
      const step = base as Item;
      if (
        typeof step.retryAfter === 'string' &&
        Date.parse(step.retryAfter) > Date.now()
      ) {
        throw new TooEarlyError(
          `Cannot start step "${stepId}": retryAfter timestamp has not been reached yet`,
          {
            retryAfter: Math.ceil(
              (Date.parse(step.retryAfter) - Date.now()) / 1000
            ),
          }
        );
      }
      let stored: unknown = eventData;
      if (eventData && 'input' in eventData && !creationOnStart) {
        const { input: _input, ...rest } = eventData;
        stored = rest;
      }
      events.push(event(stored));
      const { retryAfter: _retryAfter, ...rest } = step;
      const started = stepItem({
        ...rest,
        status: 'running',
        attempt: Number(step.attempt ?? 0) + 1,
        startedAt: step.startedAt ?? now,
        updatedAt: now,
      });
      return answer(own(events), [{ item: { Put: { Item: started } } }], {
        step: stepFromItem(started),
        ...(lazyStepStart && !validatedStep && { stepCreated: true as const }),
      });
    }

    case 'step_completed':
    case 'step_failed':
    case 'step_retrying': {
      const stepId = data.correlationId;
      const step = validatedStep ?? (await table.get(stepKey(runId, stepId)));
      if (!step) throw new WorkflowWorldError(`Step "${stepId}" not found`);
      if (isTerminalStepStatus(String(step.status))) {
        throw new EntityConflictError(
          `Cannot modify step in terminal state "${String(step.status)}"`
        );
      }
      let next: Record<string, unknown>;
      if (data.eventType === 'step_completed') {
        next = {
          ...step,
          status: 'completed',
          output: data.eventData.result,
          completedAt: now,
        };
      } else if (data.eventType === 'step_failed') {
        next = {
          ...step,
          status: 'failed',
          error: data.eventData.error,
          completedAt: now,
        };
      } else {
        const { retryAfter: _retryAfter, ...rest } = step;
        next = {
          ...rest,
          status: 'pending',
          error: data.eventData.error,
          retryAfter: data.eventData.retryAfter,
        };
      }
      const updated = stepItem({ ...next, updatedAt: now });
      return answer(own([event()]), [{ item: { Put: { Item: updated } } }], {
        step: stepFromItem(updated),
      });
    }

    case 'hook_created':
      return decideHookCreated(
        table,
        runId,
        state,
        data,
        specVersion,
        attempt,
        own,
        answer
      );

    case 'hook_disposed': {
      const pointer = liveHook?.pointer as Item;
      const hookRunId = String(pointer.runId);
      const hookId = data.correlationId;
      // The pointer delete must find the hook still there: of a disposal
      // and a delivery racing, only one commits first.
      const writes: TaggedItem[] = [
        { item: { Delete: { Key: hookKey(hookRunId, hookId) } } },
        {
          tag: disposalTag(hookId),
          item: {
            Delete: {
              Key: hookIdKey(hookId),
              ConditionExpression: '#runId = :runId',
              ExpressionAttributeNames: { '#runId': 'runId' },
              ExpressionAttributeValues: { ':runId': hookRunId },
            },
          },
        },
      ];
      const token = liveHook?.hook?.token ?? pointer.token;
      if (typeof token === 'string') {
        const claim = await table.get(hookTokenKey(token));
        if (claim?.runId === hookRunId && claim.hookId === hookId) {
          writes.push({
            tag: 'token',
            item: tokenDelete(token, hookRunId, hookId),
          });
        }
      }
      return answer(own([event()]), writes, {});
    }

    case 'hook_received': {
      if (runItem && isTerminalWorkflowRunStatus(String(runItem.status))) {
        throw new RunExpiredError(
          `Workflow run "${runId}" is already in terminal state "${String(runItem.status)}"`
        );
      }
      const pointer = liveHook?.pointer as Item;
      return answer(
        own([event()]),
        [{ item: pointerCheck(data.correlationId, String(pointer.runId)) }],
        {}
      );
    }

    case 'wait_created': {
      const corr = data.correlationId;
      const existing = await table.get(waitKey(runId, corr));
      if (existing) {
        if (
          existing.status !== 'waiting' ||
          (await hasEvent(table, runId, corr, 'wait_created'))
        ) {
          throw new EntityConflictError(`Wait "${corr}" already exists`);
        }
        // A waiting row without its event: write the event.
        return answer(own([event()]), [], { wait: waitFromItem(existing) });
      }
      const wait = toStorable({
        ...waitKey(runId, corr),
        type: 'wait',
        waitId: `${runId}-${corr}`,
        runId,
        status: 'waiting',
        resumeAt: data.eventData.resumeAt,
        specVersion,
        createdAt: now,
        updatedAt: now,
      }) as Item;
      return answer(
        own([event()]),
        [
          {
            item: {
              Put: {
                Item: wait,
                ConditionExpression: 'attribute_not_exists(PK)',
              },
            },
          },
        ],
        { wait: waitFromItem(wait) }
      );
    }

    case 'wait_completed': {
      const corr = data.correlationId;
      const existing = await table.get(waitKey(runId, corr));
      // A finished run's waits are gone, even one its cleanup left behind.
      if (
        !existing ||
        (runItem && isTerminalWorkflowRunStatus(String(runItem.status)))
      ) {
        throw new WorkflowWorldError(`Wait "${corr}" not found`);
      }
      if (existing.status === 'completed') {
        if (await hasEvent(table, runId, corr, 'wait_completed')) {
          throw new EntityConflictError(`Wait "${corr}" already completed`);
        }
        // Completed without its event: write the event.
        return answer(own([event()]), [], { wait: waitFromItem(existing) });
      }
      const wait = toStorable({
        ...existing,
        status: 'completed',
        completedAt: now,
        updatedAt: now,
      }) as Item;
      return answer(own([event()]), [{ item: { Put: { Item: wait } } }], {
        wait: waitFromItem(wait),
      });
    }

    default:
      // Events with no entity (a sealed-log `noop`, for instance).
      return answer(own([event()]), [], {});
  }
}
