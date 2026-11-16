/**
 * The JSON the queue carries a payload in, from `queue()` to the request a
 * delivery makes, with `Uint8Array` values tagged as base64 (the same
 * envelope world-postgres uses): the first delivery of a run carries its
 * input as bytes (`runInput.input`).
 */

interface TaggedBytes {
  __type: 'Uint8Array';
  data: string;
}

function isTaggedBytes(value: unknown): value is TaggedBytes {
  return (
    value !== null &&
    typeof value === 'object' &&
    (value as TaggedBytes).__type === 'Uint8Array' &&
    typeof (value as TaggedBytes).data === 'string'
  );
}

/** `value` as JSON, with every `Uint8Array` tagged. */
export function serializePayload(value: unknown): string {
  return JSON.stringify(value, (_key, inner: unknown) =>
    inner instanceof Uint8Array
      ? ({
          __type: 'Uint8Array',
          data: Buffer.from(inner).toString('base64'),
        } satisfies TaggedBytes)
      : inner
  );
}

/** The inverse of {@link serializePayload}. */
export function deserializePayload(text: string): unknown {
  return JSON.parse(text, (_key, inner: unknown) =>
    isTaggedBytes(inner)
      ? new Uint8Array(Buffer.from(inner.data, 'base64'))
      : inner
  );
}
