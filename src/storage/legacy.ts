import { RunExpiredError } from '@workflow/errors';
import type { AnyEventRequest } from '@workflow/world';
import {
  isTerminalWorkflowRunStatus,
  SPEC_VERSION_CURRENT,
} from '@workflow/world';
import type { Decision, RunState } from './commit.js';
import { endOfRunCleanup, type Outcome } from './create-support.js';
import { type Item, runFromItem } from './items.js';
import type { Table } from './table.js';
import { toStorable } from './values.js';

/**
 * A run created before event sourcing (spec 1): `run_cancelled` updates the
 * run directly and journals nothing; `wait_completed` and `hook_received`
 * are journaled with `wevt_` ids and change no entity; anything else is
 * refused.
 */
export async function decideLegacy(
  table: Table,
  runId: string,
  state: RunState,
  data: AnyEventRequest
): Promise<Decision<Outcome>> {
  const run = state.item as Item;
  const now = new Date();
  switch (data.eventType) {
    case 'run_cancelled': {
      const set = { status: 'cancelled', completedAt: now, updatedAt: now };
      const cleanup = await endOfRunCleanup(table, runId, false);
      return {
        plan: {
          appends: [{ runId, state, events: [], set, legacyIds: true }],
          cleanup,
          done: () => ({
            run: runFromItem({ ...run, ...(toStorable(set) as Item) }),
            allocated: 0,
          }),
        },
      };
    }
    case 'wait_completed':
    case 'hook_received': {
      if (
        data.eventType === 'hook_received' &&
        isTerminalWorkflowRunStatus(String(run.status))
      ) {
        throw new RunExpiredError(
          `Workflow run "${runId}" is already in terminal state "${String(run.status)}"`
        );
      }
      return {
        plan: {
          appends: [
            {
              runId,
              state,
              legacyIds: true,
              events: [
                {
                  eventType: data.eventType,
                  correlationId: data.correlationId,
                  eventData: 'eventData' in data ? data.eventData : undefined,
                  specVersion: SPEC_VERSION_CURRENT,
                },
              ],
            },
          ],
          done: (committed) => ({
            event: committed.events.get(runId)?.[0],
            allocated: 0,
          }),
        },
      };
    }
    default:
      throw new Error(
        `Event type '${data.eventType}' not supported for legacy runs ` +
          `(specVersion: ${typeof run.specVersion === 'number' ? run.specVersion : 'undefined'}). ` +
          'Please upgrade @workflow packages.'
      );
  }
}
