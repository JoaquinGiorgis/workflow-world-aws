/**
 * Conversions between World values and DynamoDB attribute values.
 *
 * The document client maps strings, numbers, booleans, null, arrays and
 * plain objects as they are, and `Uint8Array` to the Binary type. Dates
 * have no DynamoDB type: they are stored as ISO 8601 strings, which every
 * World schema coerces back (`z.coerce.date()`). Binary values come back as
 * plain `Uint8Array`s, whatever view the SDK hands out.
 */

/** Turns a value into one the document client can marshall. */
export function toStorable(value: unknown): unknown {
  if (value instanceof Date) return value.toISOString();
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return value.map(toStorable);
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value)) {
      if (entry !== undefined) out[key] = toStorable(entry);
    }
    return out;
  }
  return value;
}

/** Normalizes what the document client returns. */
export function fromStorable(value: unknown): unknown {
  if (value instanceof Uint8Array) {
    return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  }
  if (value instanceof Set) return [...value].map(fromStorable);
  if (Array.isArray(value)) return value.map(fromStorable);
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value)) {
      out[key] = fromStorable(entry);
    }
    return out;
  }
  return value;
}

const encoder = new TextEncoder();

/**
 * Size of a value as DynamoDB counts it toward the 400 KB item limit,
 * rounded up: UTF-8 bytes of strings and names, raw bytes of binaries, and
 * at most 21 bytes per number.
 */
export function sizeOf(value: unknown): number {
  if (value === null || value === undefined) return 1;
  if (typeof value === 'string') return encoder.encode(value).length;
  if (typeof value === 'number') return 21;
  if (typeof value === 'boolean') return 1;
  if (value instanceof Uint8Array) return value.byteLength;
  if (Array.isArray(value)) {
    return value.reduce<number>((total, entry) => total + 1 + sizeOf(entry), 3);
  }
  if (typeof value === 'object') {
    let total = 3;
    for (const [key, entry] of Object.entries(value)) {
      if (entry === undefined) continue;
      total += 1 + encoder.encode(key).length + sizeOf(entry);
    }
    return total;
  }
  return encoder.encode(String(value)).length;
}

/** Drops undefined entries, as the stored item would. */
export function compact<T extends Record<string, unknown>>(value: T): T {
  const out: Record<string, unknown> = {};
  for (const [key, entry] of Object.entries(value)) {
    if (entry !== undefined && entry !== null) out[key] = entry;
  }
  return out as T;
}
