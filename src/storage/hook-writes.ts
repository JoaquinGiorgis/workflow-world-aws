import { hookIdKey, hookKey, hookTokenKey } from '../keys.js';
import type { TaggedItem } from './commit.js';
import type { TokenOwner } from './hooks.js';
import type { Item } from './items.js';
import type { TransactItem } from './table.js';
import { toStorable } from './values.js';

/**
 * Writes that point `token` at a new hook. The claim is conditioned on the
 * claim the decision read: absent, or the same `(runId, hookId)` as before.
 * A claimer that lost the race fails that condition, and the claim that won
 * comes back on the cancellation reason (tag `token`), which is how the
 * next decision learns the owner. A previous owner that no longer held the
 * token (its run ended, its retention passed) loses its leftover hook and
 * pointer in the same transaction.
 */
export function claimToken(
  token: string,
  claimer: { runId: string; hookId: string },
  previous: TokenOwner | undefined,
  /** A forced takeover: `previous` still holds the token, on purpose. */
  takeover = false
): TaggedItem[] {
  const claim: TaggedItem = {
    tag: 'token',
    item: {
      Put: {
        Item: toStorable({ ...hookTokenKey(token), ...claimer }) as Item,
        ...(previous
          ? {
              ConditionExpression: '#runId = :runId AND #hookId = :hookId',
              ExpressionAttributeNames: {
                '#runId': 'runId',
                '#hookId': 'hookId',
              },
              ExpressionAttributeValues: {
                ':runId': previous.runId,
                ':hookId': previous.hookId,
              },
            }
          : { ConditionExpression: 'attribute_not_exists(PK)' }),
        ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
      },
    },
  };
  if (previous?.holds && !takeover) {
    throw new Error(
      `Token claim for ${claimer.runId} decided on a claim that is still held`
    );
  }
  if (!previous || previous.holds) return [claim];
  // One transaction may not write an item twice: a leftover with the
  // claimer's own hook id is overwritten by the claimer's writes instead.
  if (previous.hookId === claimer.hookId) return [claim];
  return [claim, ...removeHook(previous.runId, previous.hookId, false)];
}

/**
 * Deletes a hook and its id pointer (only while it still points at this
 * run), and with `token` its token claim, conditioned on the claim still
 * naming this hook.
 */
export function removeHook(
  runId: string,
  hookId: string,
  token: string | false
): TaggedItem[] {
  const items: TaggedItem[] = [
    { item: { Delete: { Key: hookKey(runId, hookId) } } },
    { item: pointerDelete(runId, hookId) },
  ];
  if (token !== false) {
    items.push({ tag: 'token', item: tokenDelete(token, runId, hookId) });
  }
  return items;
}

export function pointerDelete(runId: string, hookId: string): TransactItem {
  return {
    Delete: {
      Key: hookIdKey(hookId),
      ConditionExpression: 'attribute_not_exists(PK) OR #runId = :runId',
      ExpressionAttributeNames: { '#runId': 'runId' },
      ExpressionAttributeValues: { ':runId': runId },
    },
  };
}

export function tokenDelete(
  token: string,
  runId: string,
  hookId: string
): TransactItem {
  return {
    Delete: {
      Key: hookTokenKey(token),
      ConditionExpression: '#runId = :runId AND #hookId = :hookId',
      ExpressionAttributeNames: { '#runId': 'runId', '#hookId': 'hookId' },
      ExpressionAttributeValues: { ':runId': runId, ':hookId': hookId },
    },
  };
}
