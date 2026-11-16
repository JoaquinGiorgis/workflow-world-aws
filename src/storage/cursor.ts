import { WorkflowWorldError } from '@workflow/errors';

/**
 * Opaque pagination cursors: the DynamoDB key (`LastEvaluatedKey` shape) of
 * the last item a page returned, and the query it belongs to (its index and
 * partition, the "scope"), as base64url JSON. Passing it back as
 * `ExclusiveStartKey` resumes right after that item, in either direction.
 * A cursor minted by another query (another list, run or workflow) is
 * refused with a 400 before anything is read, rather than failing in
 * DynamoDB. Only string key attributes occur in this table.
 */
export type CursorKey = Record<string, string>;

export function encodeCursor(scope: string, key: CursorKey): string {
  return Buffer.from(JSON.stringify({ s: scope, k: key }), 'utf8').toString(
    'base64url'
  );
}

/**
 * @throws {WorkflowWorldError} (400) for anything that is not a cursor of
 *   the query `scope`.
 */
export function decodeCursor(scope: string, cursor: string): CursorKey {
  let parsed: { s?: unknown; k?: unknown } | undefined;
  try {
    parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  } catch {
    parsed = undefined;
  }
  const key = parsed?.k as CursorKey | undefined;
  if (
    parsed === null ||
    typeof parsed !== 'object' ||
    key === null ||
    typeof key !== 'object' ||
    Array.isArray(key) ||
    typeof key.PK !== 'string' ||
    typeof key.SK !== 'string' ||
    !Object.values(key).every((value) => typeof value === 'string')
  ) {
    throw new WorkflowWorldError(`Invalid pagination cursor: ${cursor}`, {
      status: 400,
    });
  }
  if (parsed.s !== scope) {
    throw new WorkflowWorldError(
      'Pagination cursor belongs to another list: pass the cursor the same list returned',
      { status: 400 }
    );
  }
  return key;
}

/** The cursor of an item: the key attributes an index query needs. */
export function cursorOf(
  scope: string,
  item: Record<string, unknown>,
  indexKeys: readonly string[] = []
): string {
  const key: CursorKey = {};
  for (const name of ['PK', 'SK', ...indexKeys]) {
    const value = item[name];
    if (typeof value === 'string') key[name] = value;
  }
  return encodeCursor(scope, key);
}
