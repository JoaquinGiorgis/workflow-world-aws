/**
 * The hook writes of `events.create`: `hook_created` (token claims,
 * conflicts and forced takeovers), and `hook_received` with a resume id.
 */
import { createHash } from 'node:crypto';
import {
  EntityConflictError,
  HookForceClaimedError,
  HookNotFoundError,
  RunExpiredError,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import type { AnyEventRequest, Hook } from '@workflow/world';
import {
  isStartHookId,
  isTerminalWorkflowRunStatus,
  SPEC_VERSION_LEGACY,
  SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM,
  slotToEventId,
} from '@workflow/world';
import {
  eventKey,
  hookIdKey,
  hookIndexKeys,
  hookKey,
  hookTokenKey,
  PREFIX,
  resumeKey,
  runPk,
} from '../keys.js';
import {
  type Attempt,
  commit,
  type Decision,
  type NewEvent,
  type RunState,
  readRun,
  runState,
  type TaggedItem,
} from './commit.js';
import type { StorageContext } from './context.js';
import {
  type Answer,
  checkFence,
  eventTime,
  hasEvent,
  type Outcome,
  type Own,
  pointerCheck,
  type Request,
} from './create-support.js';
import { claimToken, pointerDelete } from './hook-writes.js';
import { type TokenOwner, tokenOwner } from './hooks.js';
import { hookFromItem, type Item } from './items.js';
import type { Table } from './table.js';
import { toStorable } from './values.js';

function sha256(payload: unknown): string {
  const bytes =
    payload instanceof Uint8Array
      ? payload
      : new TextEncoder().encode(
          JSON.stringify(payload, (_key, value) =>
            value instanceof Uint8Array
              ? { $bytes: Buffer.from(value).toString('base64') }
              : value
          ) ?? 'undefined'
        );
  return createHash('sha256').update(bytes).digest('hex');
}

/**
 * The refusal for a `hook_received` to a hook another run took over
 * (`experimental_force`): a redirect naming the token's current owner, so
 * `resumeHook()` follows the token. The evidence is the hook's own
 * `hook_disposed{forceClaimedBy}` in its run's log; undefined otherwise.
 */
export async function forceClaimRefusal(
  table: Table,
  runId: string,
  hookId: string,
  eventData: unknown
): Promise<HookForceClaimedError | undefined> {
  const [disposal] = await table.collect(
    {
      KeyConditionExpression: 'PK = :pk AND begins_with(SK, :prefix)',
      FilterExpression: 'correlationId = :c AND eventType = :t',
      ExpressionAttributeValues: {
        ':pk': runPk(runId),
        ':prefix': PREFIX.event,
        ':c': hookId,
        ':t': 'hook_disposed',
      },
      ConsistentRead: true,
    },
    1
  );
  const data = disposal?.eventData as
    | {
        token?: unknown;
        forceClaimedBy?: { runId?: unknown; hookId?: unknown };
      }
    | undefined;
  const claimedBy = data?.forceClaimedBy;
  if (
    typeof claimedBy?.runId !== 'string' ||
    typeof claimedBy.hookId !== 'string'
  ) {
    return undefined;
  }
  const requestToken = (eventData as { token?: unknown } | undefined)?.token;
  const token =
    typeof requestToken === 'string'
      ? requestToken
      : typeof data?.token === 'string'
        ? data.token
        : undefined;
  const successor =
    token === undefined ? undefined : await table.get(hookTokenKey(token));
  return new HookForceClaimedError(
    token ?? '',
    typeof successor?.runId === 'string' ? successor.runId : claimedBy.runId,
    typeof successor?.hookId === 'string' ? successor.hookId : claimedBy.hookId
  );
}

export async function decideHookCreated(
  table: Table,
  runId: string,
  _state: RunState,
  data: Extract<AnyEventRequest, { eventType: 'hook_created' }>,
  specVersion: number,
  attempt: Attempt,
  own: Own,
  answer: Answer
): Promise<Decision<Outcome>> {
  const hookId = data.correlationId;
  const { eventData } = data;
  const { token } = eventData;
  if (isStartHookId(runId, hookId)) {
    throw new WorkflowWorldError(
      `Hook "${hookId}" is the start Hook of run "${runId}", created by its admission`,
      { status: 400 }
    );
  }
  const now = new Date();
  const owner = await tokenOwner(
    table,
    token,
    attempt.failure?.reasons.get('token')?.item
  );

  const conflict = (
    conflictingRunId: string,
    forceRefusedReason?: 'victim-spec-version'
  ) =>
    answer(
      own([
        {
          eventType: 'hook_conflict',
          correlationId: hookId,
          eventData: {
            token,
            conflictingRunId,
            ...(forceRefusedReason && { forceRefusedReason }),
          },
        },
      ]),
      [],
      {}
    );

  // The token already names this hook: a duplicate, or a hook left without
  // its event, completed here.
  if (owner && owner.runId === runId && owner.hookId === hookId) {
    if (await hasEvent(table, runId, hookId, 'hook_created')) {
      throw new EntityConflictError(`Hook "${hookId}" already created`);
    }
    return answer(
      own([{ eventType: 'hook_created', correlationId: hookId, eventData }]),
      [],
      {
        ...(owner.hook && { hook: hookFromItem(owner.hook) }),
      }
    );
  }

  const pointer = await table.get(hookIdKey(hookId));
  if (pointer) {
    // The hook id is taken (by this run with another token, or by another
    // run): world-postgres journals the event and creates no hook, unless
    // the event is a duplicate.
    if (
      pointer.runId === runId &&
      (await hasEvent(table, runId, hookId, 'hook_created'))
    ) {
      throw new EntityConflictError(`Hook "${hookId}" already created`);
    }
    if (!owner?.holds || owner.runId !== String(pointer.runId)) {
      return answer(
        own([{ eventType: 'hook_created', correlationId: hookId, eventData }]),
        [],
        {}
      );
    }
  }

  const hookFor = (claimedFrom?: Hook['claimedFrom']) =>
    toStorable({
      ...hookKey(runId, hookId),
      ...hookIndexKeys(hookId),
      type: 'hook',
      runId,
      hookId,
      token,
      ownerId: '',
      projectId: '',
      environment: '',
      metadata: eventData.metadata,
      tokenRetentionUntil: eventData.tokenRetentionUntil,
      specVersion,
      isWebhook: eventData.isWebhook,
      isSystem: eventData.isSystem ?? false,
      createdAt: now,
      claimedFrom,
    }) as Item;
  /**
   * The new hook and its id pointer. `pointerOwner`: the run the pointer
   * names now, re-pointed with one conditional put (a transaction may not
   * delete and put one item).
   */
  const newHookWrites = (hook: Item, pointerOwner?: string): TaggedItem[] => [
    {
      item: {
        Put: {
          Item: toStorable({ ...hookIdKey(hookId), runId, token }) as Item,
          ...(pointerOwner === undefined
            ? { ConditionExpression: 'attribute_not_exists(PK)' }
            : {
                ConditionExpression: '#runId = :runId',
                ExpressionAttributeNames: { '#runId': 'runId' },
                ExpressionAttributeValues: { ':runId': pointerOwner },
              }),
        },
      },
    },
    {
      item: {
        Put: { Item: hook, ConditionExpression: 'attribute_not_exists(PK)' },
      },
    },
  ];

  if (owner?.holds) {
    if (eventData.force !== true) return conflict(owner.runId);
    return takeOver(
      runId,
      hookId,
      data,
      specVersion,
      owner,
      own,
      answer,
      hookFor,
      newHookWrites
    );
  }

  const hook = hookFor();
  return answer(
    own([{ eventType: 'hook_created', correlationId: hookId, eventData }]),
    [...claimToken(token, { runId, hookId }, owner), ...newHookWrites(hook)],
    { hook: hookFromItem(hook) }
  );
}

/**
 * `createHook({ experimental_force: true })` on a token another hook holds:
 * in one transaction, the victim's `hook_disposed{forceClaimedBy}` (when
 * its run is live), its hook and pointer deleted, the token re-pointed, and
 * the claimer's hook and `hook_created{forceClaimedFrom}`.
 */
function takeOver(
  runId: string,
  hookId: string,
  data: Extract<AnyEventRequest, { eventType: 'hook_created' }>,
  specVersion: number,
  victim: TokenOwner,
  own: Own,
  answer: Answer,
  hookFor: (claimedFrom?: Hook['claimedFrom']) => Item,
  newHookWrites: (hook: Item, pointerOwner?: string) => TaggedItem[]
): Decision<Outcome> {
  const { token } = data.eventData;
  const victimRun = victim.run;
  const victimRunning =
    victimRun !== undefined &&
    !isTerminalWorkflowRunStatus(String(victimRun.status));
  const victimSpec =
    typeof victimRun?.specVersion === 'number'
      ? victimRun.specVersion
      : undefined;
  if (
    victimRunning &&
    (victimSpec ?? SPEC_VERSION_LEGACY) < SPEC_VERSION_SUPPORTS_HOOK_FORCE_CLAIM
  ) {
    return answer(
      own([
        {
          eventType: 'hook_conflict',
          correlationId: hookId,
          eventData: {
            token,
            conflictingRunId: victim.runId,
            forceRefusedReason: 'victim-spec-version',
          },
        },
      ]),
      [],
      {}
    );
  }
  const claimedFrom: NonNullable<Hook['claimedFrom']> = {
    runId: victim.runId,
    hookId: victim.hookId,
    ...(victimRunning && {
      workflowName: String(victimRun?.workflowName),
      deploymentId: String(victimRun?.deploymentId),
      ...(victimSpec !== undefined && { runSpecVersion: victimSpec }),
    }),
  };
  const disposal: NewEvent | undefined = victimRunning
    ? {
        eventType: 'hook_disposed',
        correlationId: victim.hookId,
        eventData: {
          token,
          forceClaimedBy: { runId, hookId },
        },
        specVersion: victimSpec ?? specVersion,
      }
    : undefined;
  const created: NewEvent = {
    eventType: 'hook_created',
    correlationId: hookId,
    eventData: { ...data.eventData, forceClaimedFrom: claimedFrom },
  };
  const hook = hookFor(claimedFrom);
  const writes: TaggedItem[] = [
    ...claimToken(token, { runId, hookId }, victim, true),
    { item: { Delete: { Key: hookKey(victim.runId, victim.hookId) } } },
    // The victim's hook id may be the claimer's own: then its pointer is
    // re-pointed instead of deleted.
    ...(victim.hookId === hookId
      ? newHookWrites(hook, victim.runId)
      : [
          { item: pointerDelete(victim.runId, victim.hookId) },
          ...newHookWrites(hook),
        ]),
  ];

  if (victim.runId === runId) {
    // The victim is this run: its disposal comes first in its own log.
    const decision = answer(
      own([...(disposal ? [disposal] : []), created]),
      writes,
      { hook: hookFromItem(hook) }
    );
    return decision;
  }
  const decision = answer(own([created]), writes, { hook: hookFromItem(hook) });
  if (disposal && 'plan' in decision) {
    decision.plan.appends.push({
      runId: victim.runId,
      state: runState(victimRun),
      events: [disposal],
    });
  }
  return decision;
}

/**
 * `hook_received` with a `resumeId` (`hookResumeDedup`): the first write of
 * a resume id appends the event and claims the id; every later one with the
 * same id and contents converges on that event, even after the hook was
 * disposed or the run ended.
 */
export function createHookResume(
  context: StorageContext,
  request: Request & {
    data: Extract<AnyEventRequest, { eventType: 'hook_received' }>;
    resumeId: string;
  }
): Promise<Outcome> {
  const { table } = context;
  const { runId, data, params, resumeId, specVersion } = request;
  if (!data.correlationId) {
    throw new WorkflowWorldError('Hook identity is required', { status: 400 });
  }
  const digest = sha256(data.eventData.payload);
  if (params?.resumePayloadDigest && params.resumePayloadDigest !== digest) {
    throw new WorkflowWorldError(
      'Hook resume digest does not match its payload',
      { status: 422 }
    );
  }
  return commit<Outcome>(
    table,
    `hook_received for run "${runId}"`,
    async () => {
      const state = await readRun(table, runId);
      checkFence(runId, state, params);
      if (!state.item) throw new WorkflowRunNotFoundError(runId);
      const prior = await table.get(resumeKey(runId, resumeId));
      if (prior) {
        const event = await table.get(eventKey(runId, String(prior.eventId)));
        const priorToken = (event?.eventData as { token?: unknown } | undefined)
          ?.token;
        if (
          event?.eventType !== 'hook_received' ||
          event.correlationId !== data.correlationId ||
          event.resumePayloadDigest !== digest ||
          priorToken !== data.eventData.token
        ) {
          throw new WorkflowWorldError(
            'Hook resume identity reused with different contents',
            { status: 422 }
          );
        }
        return { result: { event, allocated: 0 } };
      }
      if (isTerminalWorkflowRunStatus(String(state.item.status))) {
        throw new RunExpiredError('Cannot resume a hook on a terminal run');
      }
      const pointer = await table.get(hookIdKey(data.correlationId));
      if (
        pointer?.runId !== runId ||
        (data.eventData.token !== undefined &&
          pointer.token !== data.eventData.token)
      ) {
        throw new HookNotFoundError(data.correlationId);
      }
      const createdAt = eventTime(params);
      return {
        plan: {
          appends: [
            {
              runId,
              state,
              inBand: params?.inBand === true,
              events: [
                {
                  eventType: 'hook_received',
                  correlationId: data.correlationId,
                  eventData: data.eventData,
                  specVersion,
                  createdAt,
                  resumeId,
                  resumePayloadDigest: digest,
                },
              ],
            },
          ],
          writes: [
            { item: pointerCheck(data.correlationId, runId) },
            {
              item: {
                Put: {
                  Item: {
                    ...resumeKey(runId, resumeId),
                    type: 'resume',
                    eventId: slotToEventId(state.seq + 1),
                  },
                  ConditionExpression: 'attribute_not_exists(PK)',
                },
              },
            },
          ],
          done: (committed) => ({
            event: committed.events.get(runId)?.[0],
            allocated: 1,
          }),
        },
      };
    },
    context.retry
  );
}
