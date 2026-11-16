import { WorkflowWorldError } from '@workflow/errors';

/**
 * Opaque pagination cursors: the DynamoDB key (`LastEvaluatedKey` shape) of
 * the last item a page returned, as base64url JSON. Passing it back as
 * `ExclusiveStartKey` resumes right after that item, in either direction.
 * Only string key attributes occur in this table.
 */
export type CursorKey = Record<string, string>;

export function encodeCursor(key: CursorKey): string {
  return Buffer.from(JSON.stringify(key), 'utf8').toString('base64url');
}

/** @throws {WorkflowWorldError} (400) for anything that is not a cursor. */
export function decodeCursor(cursor: string): CursorKey {
  let parsed: unknown;
  try {
    parsed = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8'));
  } catch {
    parsed = undefined;
  }
  if (
    parsed === null ||
    typeof parsed !== 'object' ||
    Array.isArray(parsed) ||
    typeof (parsed as CursorKey).PK !== 'string' ||
    typeof (parsed as CursorKey).SK !== 'string' ||
    !Object.values(parsed).every((value) => typeof value === 'string')
  ) {
    throw new WorkflowWorldError(`Invalid pagination cursor: ${cursor}`, {
      status: 400,
    });
  }
  return parsed as CursorKey;
}

/** The cursor of an item: the key attributes an index query needs. */
export function cursorOf(
  item: Record<string, unknown>,
  indexKeys: readonly string[] = []
): string {
  const key: CursorKey = {};
  for (const name of ['PK', 'SK', ...indexKeys]) {
    const value = item[name];
    if (typeof value === 'string') key[name] = value;
  }
  return encodeCursor(key);
}
