/**
 * Creating a run, with or without a start Hook (`start({ experimental_hook
 * })`, see `start-hook.ts` in @workflow/world).
 *
 * The run item, its `run_created` event and, for a start Hook, the
 * candidate's admission decision and the start Hook's three items (hook, id
 * pointer, token claim) are one transaction. The decision item
 * (`RUN#<runId>` / `ADMISSION`) is written only if absent, so the first
 * attempt for a candidate decides it and every later one, through either
 * the direct `run_created` or the queued `run_started`, reads that decision
 * back. A refusal is written on its own, before it is answered.
 */
import {
  EntityConflictError,
  HookConflictError,
  START_HOOK_ADMISSION_REJECTED,
  WorkflowRunNotFoundError,
  WorkflowWorldError,
} from '@workflow/errors';
import type { StartHook, WorkflowRun } from '@workflow/world';
import {
  START_HOOK_CANDIDATE_MAX_AGE_MS,
  slotToEventId,
  startHookId,
  ulidToDate,
  validateAttributeChanges,
} from '@workflow/world';
import {
  admissionKey,
  eventKey,
  hookIdKey,
  hookIndexKeys,
  hookKey,
  runIndexKeys,
  runMetaKey,
} from '../keys.js';
import { type Attempt, commit, type Decision, readRun } from './commit.js';
import type { StorageContext } from './context.js';
import { claimToken } from './hook-writes.js';
import { type TokenOwner, tokenOwner } from './hooks.js';
import { type Item, runFromItem } from './items.js';
import { debugLog } from './log.js';
import { toStorable } from './values.js';

/** What `run_created` (or a resilient `run_started`) carries to create a run. */
export interface RunCreationData {
  deploymentId: string;
  workflowName: string;
  input: unknown;
  executionContext?: Record<string, unknown>;
  attributes?: Record<string, string>;
  allowReservedAttributes?: true;
  encryptionPublicKey?: string;
  dynamicWorkflowCode?: unknown;
  dynamicWorkflowCodeRef?: string;
  startHook?: StartHook;
}

export type RunCreation =
  /** Created now; `event` is its stored `run_created`. */
  | { created: true; run: WorkflowRun; event: Item }
  /** The candidate was admitted before: its run, and its `run_created`. */
  | { created: false; run: WorkflowRun; event: Item | undefined };

/** Debug breadcrumb of a start Hook decision; never names the token. */
function startHookLog(message: string, detail: Record<string, string>) {
  debugLog(`start Hook: ${message}`, detail);
}

/** The start Hook without metadata, with its id: what the run keeps. */
function runStartHook(runId: string, startHook: StartHook) {
  const { metadata: _metadata, ...rest } = startHook;
  return { ...rest, hookId: startHookId(runId) };
}

/**
 * Creates the run `runId`. Without a start Hook a run that already exists
 * is an `EntityConflictError`. With one, a candidate admitted before
 * answers with its run, and a refused one with `HookConflictError`.
 */
export async function createRun(
  context: StorageContext,
  runId: string,
  data: RunCreationData,
  specVersion: number,
  origin: 'run_created' | 'run_started'
): Promise<RunCreation> {
  const { table } = context;
  if (data.dynamicWorkflowCodeRef !== undefined) {
    throw new Error(
      'The AWS World does not support deferred dynamic workflow code refs'
    );
  }
  validateAttributeChanges(
    Object.entries(data.attributes ?? {}).map(([key, value]) => ({
      key,
      value,
    })),
    { allowReservedAttributes: data.allowReservedAttributes === true }
  );
  const { startHook } = data;

  const outcome = await commit<RunCreation | { refusedBy: string }>(
    table,
    `the creation of run "${runId}"`,
    async (
      attempt: Attempt
    ): Promise<Decision<RunCreation | { refusedBy: string }>> => {
      const state = await readRun(table, runId);
      const now = new Date();
      let owner: TokenOwner | undefined;

      if (startHook) {
        const decided = await table.get(admissionKey(runId));
        if (decided) {
          if (decided.token !== startHook.token) {
            throw new WorkflowWorldError(
              `Workflow run "${runId}" was already admitted with a different start Hook token`,
              {
                status: 400,
                code: START_HOOK_ADMISSION_REJECTED,
                reason: 'token_mismatch',
              }
            );
          }
          if (decided.decision !== 'accepted') {
            throw new HookConflictError(
              startHook.token,
              String(decided.conflictingRunId ?? '')
            );
          }
          if (!state.item) throw new WorkflowRunNotFoundError(runId);
          return {
            result: {
              created: false,
              run: runFromItem(state.item),
              event: await table.get(eventKey(runId, slotToEventId(1))),
            },
          };
        }
        // Orphan bound: a candidate this old can never be created.
        const candidateTime = ulidToDate(runId.replace(/^wrun_/, ''));
        if (
          candidateTime &&
          Date.now() - candidateTime.getTime() > START_HOOK_CANDIDATE_MAX_AGE_MS
        ) {
          throw new WorkflowWorldError(
            `Workflow run "${runId}" is too old to be admitted with a start Hook`,
            {
              status: 400,
              code: START_HOOK_ADMISSION_REJECTED,
              reason: 'expired',
            }
          );
        }
        if (state.item) {
          throw new WorkflowWorldError(
            `Workflow run "${runId}" already exists without a start Hook`,
            {
              status: 400,
              code: START_HOOK_ADMISSION_REJECTED,
              reason: 'ordinary_run',
            }
          );
        }
        owner = await tokenOwner(
          table,
          startHook.token,
          attempt.failure?.reasons.get('token')?.item
        );
        if (owner?.holds && owner.runId === runId) {
          // The other copy of this candidate was admitted after the
          // decision read above: answer with its run.
          const run = await table.get(runMetaKey(runId));
          if (!run) throw new WorkflowRunNotFoundError(runId);
          return {
            result: {
              created: false,
              run: runFromItem(run),
              event: await table.get(eventKey(runId, slotToEventId(1))),
            },
          };
        }
        if (owner?.holds) {
          // Refused: the decision commits on its own, then is answered.
          const refusedBy = owner.runId;
          return {
            plan: {
              appends: [],
              writes: [
                {
                  tag: 'admission',
                  item: {
                    Put: {
                      Item: toStorable({
                        ...admissionKey(runId),
                        type: 'admission',
                        runId,
                        token: startHook.token,
                        decision: 'rejected',
                        conflictingRunId: refusedBy,
                        createdAt: now,
                        expiresAt: admissionExpiry(now),
                      }) as Item,
                      ConditionExpression: 'attribute_not_exists(PK)',
                    },
                  },
                },
              ],
              done: () => {
                startHookLog('refused a candidate', {
                  runId,
                  conflictingRunId: refusedBy,
                });
                return { refusedBy };
              },
            },
          };
        }
      } else if (state.item) {
        throw new EntityConflictError(`Workflow run "${runId}" already exists`);
      }

      const runItem = toStorable({
        ...runMetaKey(runId),
        ...runIndexKeys(runId, data.workflowName),
        type: 'run',
        runId,
        status: 'pending',
        deploymentId: data.deploymentId,
        workflowName: data.workflowName,
        specVersion,
        executionContext: data.executionContext,
        input: data.input,
        attributes: data.attributes,
        encryptionPublicKey: data.encryptionPublicKey,
        dynamicWorkflowCode: data.dynamicWorkflowCode,
        ...(startHook && { startHook: runStartHook(runId, startHook) }),
        createdAt: now,
        updatedAt: now,
      }) as Item;
      const {
        dynamicWorkflowCode: _code,
        dynamicWorkflowCodeRef: _ref,
        ...eventData
      } = data;
      const writes = [];
      if (startHook) {
        const hookId = startHookId(runId);
        writes.push(
          {
            tag: 'admission',
            item: {
              Put: {
                Item: toStorable({
                  ...admissionKey(runId),
                  type: 'admission',
                  runId,
                  token: startHook.token,
                  decision: 'accepted',
                  createdAt: now,
                  expiresAt: admissionExpiry(now),
                }) as Item,
                ConditionExpression: 'attribute_not_exists(PK)',
              },
            },
          },
          ...claimToken(startHook.token, { runId, hookId }, owner),
          {
            item: {
              Put: {
                Item: toStorable({
                  ...hookIdKey(hookId),
                  runId,
                  token: startHook.token,
                }) as Item,
                ConditionExpression: 'attribute_not_exists(PK)',
              },
            },
          },
          {
            item: {
              Put: {
                Item: toStorable({
                  ...hookKey(runId, hookId),
                  ...hookIndexKeys(hookId),
                  type: 'hook',
                  runId,
                  hookId,
                  token: startHook.token,
                  ownerId: '',
                  projectId: '',
                  environment: '',
                  metadata: startHook.metadata,
                  tokenRetentionUntil: startHook.tokenRetentionUntil,
                  specVersion,
                  isWebhook: false,
                  isSystem: false,
                  createdAt: now,
                }) as Item,
              },
            },
          }
        );
      }
      return {
        plan: {
          appends: [
            {
              runId,
              state,
              create: runItem,
              events: [
                {
                  eventType: 'run_created',
                  eventData: startHook
                    ? {
                        ...eventData,
                        startHook: { ...startHook, hookId: startHookId(runId) },
                      }
                    : eventData,
                  specVersion,
                  createdAt: now,
                },
              ],
            },
          ],
          writes,
          done: (committed) => {
            if (startHook) startHookLog('accepted a candidate', { runId });
            const event = committed.events.get(runId)?.[0] as Item;
            return {
              created: true,
              run: runFromItem({ ...runItem, seq: 1 }),
              event,
            };
          },
        },
      };
    },
    context.retry
  );

  if ('refusedBy' in outcome) {
    throw new HookConflictError(startHook?.token ?? '', outcome.refusedBy);
  }
  if (origin === 'run_created' && !outcome.created && !startHook) {
    throw new EntityConflictError(`Workflow run "${runId}" already exists`);
  }
  return outcome;
}

/** Decisions outlive every candidate window by an hour, then expire (TTL). */
function admissionExpiry(now: Date): number {
  return Math.ceil(
    (now.getTime() + START_HOOK_CANDIDATE_MAX_AGE_MS + 60 * 60 * 1000) / 1000
  );
}
