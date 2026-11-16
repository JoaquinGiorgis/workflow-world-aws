import { WorkflowRunNotFoundError } from '@workflow/errors';
import type {
  AttributeChange,
  ExperimentalSetAttributesResult,
  Storage,
  WorkflowRunStatus,
} from '@workflow/world';
import {
  applyAttributeChanges,
  isTerminalWorkflowRunStatus,
  validateAttributeChanges,
} from '@workflow/world';
import {
  ALL_RUNS_GSI1PK,
  GSI1_NAME,
  GSI2_NAME,
  PREFIX,
  runMetaKey,
} from '../keys.js';
import { commit, type RetryOptions, readRun } from './commit.js';
import { cursorOf, decodeCursor } from './cursor.js';
import { filterRunData, type Item, runFromItem } from './items.js';
import { sleep, type Table } from './table.js';

/**
 * How often `runs.waitForTerminalStatus` re-reads the run. DynamoDB has no
 * change notification a waiter could park on (DynamoDB Streams need a
 * consumer), so the wait is a consistent read every interval.
 */
export const RUN_STATUS_POLL_INTERVAL_MS = 500;

export function createRunsStorage(
  table: Table,
  retry?: RetryOptions
): Storage['runs'] {
  const getRun = (async (id, params) => {
    const item = await table.get(runMetaKey(id));
    if (!item) throw new WorkflowRunNotFoundError(id);
    return filterRunData(runFromItem(item), params?.resolveData ?? 'all');
  }) as Storage['runs']['get'];

  return {
    get: getRun,

    waitForTerminalStatus: (async (id, params) => {
      const deadline = Date.now() + (params?.timeoutMs ?? 0);
      while (true) {
        const run = await getRun(id, params);
        if (isTerminalWorkflowRunStatus(run.status)) return run;
        const remainingMs = deadline - Date.now();
        if (remainingMs <= 0 || params?.signal?.aborted) return run;
        await sleep(
          Math.min(remainingMs, RUN_STATUS_POLL_INTERVAL_MS),
          params?.signal
        );
      }
    }) as NonNullable<Storage['runs']['waitForTerminalStatus']>,

    getMany: (async (ids, params) => {
      const unique = [...new Set(ids)];
      if (unique.length === 0) return [];
      const items = await table.batchGet(unique.map(runMetaKey));
      const resolveData = params?.resolveData ?? 'all';
      const byId = new Map(
        items.map((item) => [
          String(item.runId),
          filterRunData(runFromItem(item), resolveData),
        ])
      );
      return ids.map((id) => byId.get(id) ?? null);
    }) as NonNullable<Storage['runs']['getMany']>,

    list: (async (params) => {
      const limit = params?.pagination?.limit ?? 20;
      // Run ids are ULIDs, so the index's id order is creation order.
      const ascending = params?.pagination?.sortOrder === 'asc';
      const statuses: WorkflowRunStatus[] | undefined =
        params?.status === undefined
          ? undefined
          : Array.isArray(params.status)
            ? params.status
            : [params.status];
      if (statuses?.length === 0) {
        return { data: [], cursor: null, hasMore: false };
      }
      const byWorkflow = params?.workflowName !== undefined;
      const indexKeys = byWorkflow
        ? ['GSI2PK', 'GSI2SK']
        : ['GSI1PK', 'GSI1SK'];
      const partition = byWorkflow
        ? `${PREFIX.runsByWorkflow}${params?.workflowName}`
        : ALL_RUNS_GSI1PK;
      const scope = `${byWorkflow ? GSI2_NAME : GSI1_NAME}:${partition}`;
      const values: Record<string, unknown> = { ':pk': partition };
      statuses?.forEach((status, i) => {
        values[`:s${i}`] = status;
      });
      const cursor = params?.pagination?.cursor;
      const items = await table.collect(
        {
          IndexName: byWorkflow ? GSI2_NAME : GSI1_NAME,
          KeyConditionExpression: `${indexKeys[0]} = :pk`,
          ...(statuses && {
            FilterExpression: `#status IN (${statuses.map((_, i) => `:s${i}`).join(', ')})`,
            ExpressionAttributeNames: { '#status': 'status' },
          }),
          ExpressionAttributeValues: values,
          ScanIndexForward: ascending,
          ...(cursor && { ExclusiveStartKey: decodeCursor(scope, cursor) }),
        },
        limit + 1
      );
      const page = items.slice(0, limit);
      const resolveData = params?.resolveData ?? 'all';
      const last = page.at(-1);
      return {
        data: page.map((item) => filterRunData(runFromItem(item), resolveData)),
        hasMore: items.length > limit,
        cursor: last ? cursorOf(scope, last, indexKeys) : null,
      };
    }) as Storage['runs']['list'],

    async experimentalSetAttributes(
      runId: string,
      changes: AttributeChange[],
      options?: { allowReservedAttributes?: boolean }
    ): Promise<ExperimentalSetAttributesResult> {
      return commit(
        table,
        `the attributes of run "${runId}"`,
        async () => {
          const state = await readRun(table, runId);
          if (!state.item) throw new WorkflowRunNotFoundError(runId);
          const existing = attributesOf(state.item);
          validateAttributeChanges(changes, {
            existingKeys: Object.keys(existing),
            allowReservedAttributes: options?.allowReservedAttributes,
          });
          const attributes = applyAttributeChanges(existing, changes);
          return {
            plan: {
              appends: [
                {
                  runId,
                  state,
                  events: [],
                  set: { attributes, updatedAt: new Date() },
                },
              ],
              done: () => ({ attributes }),
            },
          };
        },
        retry
      );
    },
  };
}

export function attributesOf(run: Item): Record<string, string> {
  const attributes = run.attributes;
  return attributes && typeof attributes === 'object'
    ? { ...(attributes as Record<string, string>) }
    : {};
}
