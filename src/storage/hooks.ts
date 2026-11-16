/**
 * Hooks: reads, and the token ownership rules `events.create` shares.
 *
 * A hook is three items: the hook itself in its run's partition, a pointer
 * from its id to the run (`HOOKID#`), and the claim on its token
 * (`HOOKTOKEN#`). A hook holds its token while its run is live, or after the
 * run ended until its `tokenRetentionUntil`. A finished run's other hooks
 * are deleted with its terminal event; one left behind (a cleanup that did
 * not fit the transaction) is invisible by the same rule.
 */
import { HookNotFoundError } from '@workflow/errors';
import type { Hook, ListHooksParams, Storage } from '@workflow/world';
import { isTerminalWorkflowRunStatus } from '@workflow/world';
import {
  ALL_HOOKS_GSI1PK,
  GSI1_NAME,
  hookIdKey,
  hookKey,
  hookTokenKey,
  PREFIX,
  runMetaKey,
  runPk,
} from '../keys.js';
import { cursorOf, decodeCursor } from './cursor.js';
import { filterHookData, hookFromItem, type Item } from './items.js';
import type { QueryInput, Table } from './table.js';

/** Whether a hook still holds its token, given its run's item. */
export function holdsToken(hook: Item, run: Item | undefined): boolean {
  const retention =
    typeof hook.tokenRetentionUntil === 'string'
      ? Date.parse(hook.tokenRetentionUntil)
      : Number.NaN;
  if (retention > Date.now()) return true;
  return run === undefined || !isTerminalWorkflowRunStatus(String(run.status));
}

/** The hook item an id points to, if the pointer and the hook exist. */
export async function findHook(
  table: Table,
  hookId: string
): Promise<{ pointer: Item; hook: Item } | undefined> {
  const pointer = await table.get(hookIdKey(hookId));
  if (typeof pointer?.runId !== 'string') return undefined;
  const hook = await table.get(hookKey(pointer.runId, hookId));
  return hook ? { pointer, hook } : undefined;
}

export interface TokenOwner {
  /** The token claim item. */
  claim: Item;
  runId: string;
  hookId: string;
  /** The owning hook, unless only its claim was left behind. */
  hook?: Item;
  /** The owning run's item. */
  run?: Item;
  /** Whether the owner still holds the token. */
  holds: boolean;
}

/**
 * Who claims `token`. `claim` is the claim item when the caller already has
 * it (the item a failed condition returned).
 */
export async function tokenOwner(
  table: Table,
  token: string,
  claim?: Item
): Promise<TokenOwner | undefined> {
  const item = claim ?? (await table.get(hookTokenKey(token)));
  if (typeof item?.runId !== 'string' || typeof item.hookId !== 'string') {
    return undefined;
  }
  const [hook, run] = await Promise.all([
    table.get(hookKey(item.runId, item.hookId)),
    table.get(runMetaKey(item.runId)),
  ]);
  return {
    claim: item,
    runId: item.runId,
    hookId: item.hookId,
    hook,
    run,
    holds: hook !== undefined && holdsToken(hook, run),
  };
}

function readHook(item: Item): Hook {
  const hook = hookFromItem(item);
  hook.isWebhook ??= true;
  return hook;
}

export function createHooksStorage(table: Table): Storage['hooks'] {
  /** Keeps the hooks that hold their token, reading each run once. */
  async function available(items: Item[]): Promise<Item[]> {
    const runIds = [...new Set(items.map((item) => String(item.runId)))];
    const runs = new Map(
      (await table.batchGet(runIds.map(runMetaKey))).map((run) => [
        String(run.runId),
        run,
      ])
    );
    return items.filter((item) =>
      holdsToken(item, runs.get(String(item.runId)))
    );
  }

  return {
    async get(hookId, params) {
      const found = await findHook(table, hookId);
      if (
        !found ||
        !holdsToken(
          found.hook,
          await table.get(runMetaKey(found.pointer.runId as string))
        )
      ) {
        throw new HookNotFoundError(hookId);
      }
      return filterHookData(readHook(found.hook), params?.resolveData ?? 'all');
    },

    async getByToken(token, params) {
      const owner = await tokenOwner(table, token);
      if (!owner?.hook || !owner.holds) throw new HookNotFoundError(token);
      return filterHookData(readHook(owner.hook), params?.resolveData ?? 'all');
    },

    async list(params: ListHooksParams) {
      const limit = params?.pagination?.limit ?? 100;
      const ascending = (params?.pagination?.sortOrder ?? 'asc') === 'asc';
      const cursor = params?.pagination?.cursor;
      const query: QueryInput = params.runId
        ? {
            KeyConditionExpression: 'PK = :pk AND begins_with(SK, :prefix)',
            ExpressionAttributeValues: {
              ':pk': runPk(params.runId),
              ':prefix': PREFIX.hook,
            },
            ConsistentRead: true,
          }
        : {
            IndexName: GSI1_NAME,
            KeyConditionExpression: 'GSI1PK = :pk',
            ExpressionAttributeValues: { ':pk': ALL_HOOKS_GSI1PK },
          };
      const indexKeys = params.runId ? [] : ['GSI1PK', 'GSI1SK'];
      const found: Item[] = [];
      let startKey: Record<string, unknown> | undefined = cursor
        ? decodeCursor(cursor)
        : undefined;
      do {
        const page = await table.queryPage({
          ...query,
          ScanIndexForward: ascending,
          ExclusiveStartKey: startKey,
          Limit: limit + 1,
        });
        found.push(...(await available(page.items)));
        startKey = page.lastKey;
      } while (startKey && found.length <= limit);
      const values = found.slice(0, limit);
      const resolveData = params?.resolveData ?? 'all';
      const last = values.at(-1);
      return {
        data: values.map((item) =>
          filterHookData(hookFromItem(item), resolveData)
        ),
        cursor: last ? cursorOf(last, indexKeys) : null,
        hasMore: found.length > limit,
      };
    },
  };
}
