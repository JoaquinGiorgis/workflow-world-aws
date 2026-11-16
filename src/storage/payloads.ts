/**
 * Payloads too large for a DynamoDB item, kept in S3.
 *
 * An attribute that does not fit is written to
 * `s3://<bucket>/<runId>/<kind>/<id>/<attribute>-<digest>` and the item
 * keeps a reference in its place: `{ s3Ref, s3Encoding, s3Bytes }`. Reads
 * put the value back, so every caller sees the item as it was written.
 *
 * - Binary values (`Uint8Array`) are stored as they are; any other value as
 *   JSON with `Uint8Array`s tagged (the queue's transport encoding), which
 *   is what DynamoDB would have returned: the World only stores values it
 *   has already made storable (dates as ISO strings).
 * - Object keys carry the SHA-256 of the content, so a write retried after
 *   a lost race (or a value written again unchanged) maps to the same
 *   object, and an object never changes once written: reads cache them.
 * - A resolved item remembers the reference of each value it resolved
 *   ({@link RESOLVED}), so writing it back, or measuring it, uses the
 *   reference again instead of a second copy of the bytes.
 */
import { createHash } from 'node:crypto';
import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
  type S3Client,
} from '@aws-sdk/client-s3';
import { WorkflowWorldError } from '@workflow/errors';
import { PREFIX } from '../keys.js';
import { deserializePayload, serializePayload } from '../queue/transport.js';
import type { Item } from './items.js';

/** What an item holds in place of a value kept in S3. */
export interface S3Ref {
  /** `s3://<bucket>/<key>`. */
  s3Ref: string;
  s3Encoding: 'binary' | 'json';
  /** Size of the stored object. */
  s3Bytes: number;
}

const REF_KEYS = ['s3Bytes', 's3Encoding', 's3Ref'];

/**
 * Whether `value` is a reference. Only the exact shape counts: the World
 * never stores a top-level value that is a map with just these three keys.
 */
export function isS3Ref(value: unknown): value is S3Ref {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    return false;
  }
  if (value instanceof Uint8Array) return false;
  const ref = value as Partial<S3Ref>;
  return (
    Object.keys(value).sort().join() === REF_KEYS.join() &&
    typeof ref.s3Ref === 'string' &&
    ref.s3Ref.startsWith('s3://') &&
    (ref.s3Encoding === 'binary' || ref.s3Encoding === 'json') &&
    typeof ref.s3Bytes === 'number'
  );
}

/**
 * On a resolved item: the reference each resolved attribute was read from,
 * next to the value it resolved to. A symbol, so it survives `{ ...item }`
 * but not the document client's marshalling, `Object.entries` or JSON.
 */
export const RESOLVED = Symbol.for('@joaquingiorgis/world-aws//resolved');

type Resolved = Record<string, { ref: S3Ref; value: unknown }>;

/** The resolved attributes of an item, if any. */
function resolvedOf(item: Item): Resolved | undefined {
  return (item as { [RESOLVED]?: Resolved })[RESOLVED];
}

/**
 * `item` as it is stored: every value that still is the one a reference
 * resolved to is the reference again. Other attributes are kept as they
 * are, so a value changed since the read is written anew.
 */
export function storedForm(item: Item): Item {
  const resolved = resolvedOf(item);
  if (!resolved) return item;
  const out: Item = { ...item };
  for (const [attribute, { ref, value }] of Object.entries(resolved)) {
    if (out[attribute] === value) out[attribute] = ref;
  }
  return out;
}

/** The S3 key of an item's attribute, before the content digest. */
export function objectKeyPrefix(item: Item, attribute: string): string {
  const pk = String(item.PK ?? '');
  const sk = String(item.SK ?? '');
  const segment = (value: string) => encodeURIComponent(value);
  let runId = typeof item.runId === 'string' ? item.runId : '_';
  let kind = 'item';
  let id = `${pk}/${sk}`;
  if (pk.startsWith(PREFIX.run)) {
    runId = pk.slice(PREFIX.run.length);
    const kinds: [string, string][] = [
      [PREFIX.event, 'event'],
      [PREFIX.step, 'step'],
      [PREFIX.hook, 'hook'],
      [PREFIX.wait, 'wait'],
    ];
    const match = kinds.find(([prefix]) => sk.startsWith(prefix));
    if (match) {
      kind = match[1];
      id = sk.slice(match[0].length);
    } else if (sk === 'META') {
      kind = 'run';
      id = runId;
    } else if (sk === 'SNAPSHOT') {
      kind = 'snapshot';
      id = runId;
    } else {
      id = sk;
    }
  } else if (pk.startsWith(PREFIX.stream)) {
    // STREAM#<runId>#<name>: run ids have no '#'.
    const rest = pk.slice(PREFIX.stream.length);
    const at = rest.indexOf('#');
    runId = rest.slice(0, at);
    kind = 'stream';
    id = `${rest.slice(at + 1)}/${sk}`;
  }
  return `${segment(runId)}/${kind}/${segment(id)}/${segment(attribute)}`;
}

/** Thrown when a reference points at an object that cannot be read back. */
export class PayloadObjectError extends WorkflowWorldError {
  constructor(ref: string, cause: unknown) {
    super(
      `Could not read the payload stored at ${ref}: ${cause instanceof Error ? cause.message : String(cause)}`,
      { status: 500, cause }
    );
    this.name = 'PayloadObjectError';
  }
}

export interface PayloadStoreOptions {
  s3: S3Client;
  /** Where new payloads go. Without it nothing is offloaded. */
  bucket?: string;
  /** Bytes of objects kept in memory for reads; default 32 MiB. */
  cacheBytes?: number;
}

/** How many recently written object keys are remembered. */
const RECENT_WRITES = 1_024;
/**
 * How long a written key is trusted to still exist, so a write retried
 * after a lost race does not upload the same bytes again. Short: another
 * process may delete the object (a snapshot it replaced) at any time.
 */
const RECENT_WRITE_MS = 30_000;

/** Reads and writes payloads in S3; see the module comment. */
export class PayloadStore {
  readonly bucket: string | undefined;
  private readonly s3: S3Client;
  private readonly cacheBytes: number;
  private readonly cache = new Map<string, Uint8Array>();
  private cached = 0;
  /** Recently written keys, with when. */
  private readonly written = new Map<string, number>();

  constructor(options: PayloadStoreOptions) {
    this.s3 = options.s3;
    this.bucket = options.bucket;
    this.cacheBytes = options.cacheBytes ?? 32 * 1024 * 1024;
  }

  /** Whether new payloads can be offloaded (a bucket is configured). */
  get enabled(): boolean {
    return this.bucket !== undefined;
  }

  /**
   * Writes `value` under `keyPrefix` and returns its reference.
   *
   * @throws when no bucket is configured.
   */
  async put(keyPrefix: string, value: unknown): Promise<S3Ref> {
    const bucket = this.bucket;
    if (bucket === undefined) {
      throw new Error('PayloadStore.put needs a bucket');
    }
    const binary = value instanceof Uint8Array;
    const body = binary
      ? value
      : new TextEncoder().encode(serializePayload(value));
    const digest = createHash('sha256').update(body).digest('hex');
    const key = `${keyPrefix}-${digest.slice(0, 40)}`;
    const writtenAt = this.written.get(key);
    if (writtenAt === undefined || Date.now() - writtenAt > RECENT_WRITE_MS) {
      await this.s3.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: body,
          ContentType: binary ? 'application/octet-stream' : 'application/json',
        })
      );
      this.remember(key);
    }
    this.store(`s3://${bucket}/${key}`, body);
    return {
      s3Ref: `s3://${bucket}/${key}`,
      s3Encoding: binary ? 'binary' : 'json',
      s3Bytes: body.byteLength,
    };
  }

  /** The value a reference stands for. */
  async get(ref: S3Ref): Promise<unknown> {
    let body = this.cache.get(ref.s3Ref);
    if (body) {
      // Most recently used last.
      this.cache.delete(ref.s3Ref);
      this.cache.set(ref.s3Ref, body);
    } else {
      const { bucket, key } = parseRef(ref.s3Ref);
      try {
        const object = await this.s3.send(
          new GetObjectCommand({ Bucket: bucket, Key: key })
        );
        body = await (
          object.Body as {
            transformToByteArray(): Promise<Uint8Array>;
          }
        ).transformToByteArray();
      } catch (error) {
        throw new PayloadObjectError(ref.s3Ref, error);
      }
      this.store(ref.s3Ref, body);
    }
    return ref.s3Encoding === 'binary'
      ? new Uint8Array(body)
      : deserializePayload(new TextDecoder().decode(body));
  }

  /** Deletes the object behind `ref`. Failures are the caller's to ignore. */
  async delete(ref: S3Ref): Promise<void> {
    const { bucket, key } = parseRef(ref.s3Ref);
    this.cache.delete(ref.s3Ref);
    this.written.delete(key);
    await this.s3.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
  }

  /**
   * `item` with every reference replaced by its value, remembering the
   * references ({@link RESOLVED}). The item itself when it has none.
   */
  async resolve(item: Item): Promise<Item> {
    const refs = Object.entries(item).filter(([, value]) => isS3Ref(value));
    if (refs.length === 0) return item;
    const out: Item = { ...item };
    const resolved: Resolved = {};
    await Promise.all(
      refs.map(async ([attribute, ref]) => {
        const value = await this.get(ref as S3Ref);
        out[attribute] = value;
        resolved[attribute] = { ref: ref as S3Ref, value };
      })
    );
    Object.defineProperty(out, RESOLVED, {
      value: resolved,
      enumerable: true,
    });
    return out;
  }

  private store(ref: string, body: Uint8Array): void {
    if (body.byteLength > this.cacheBytes / 4) return;
    const previous = this.cache.get(ref);
    if (previous) this.cached -= previous.byteLength;
    this.cache.delete(ref);
    this.cache.set(ref, body);
    this.cached += body.byteLength;
    for (const [oldest, bytes] of this.cache) {
      if (this.cached <= this.cacheBytes) break;
      this.cache.delete(oldest);
      this.cached -= bytes.byteLength;
    }
  }

  private remember(key: string): void {
    this.written.delete(key);
    this.written.set(key, Date.now());
    if (this.written.size > RECENT_WRITES) {
      const [oldest] = this.written.keys();
      if (oldest !== undefined) this.written.delete(oldest);
    }
  }
}

export function parseRef(ref: string): { bucket: string; key: string } {
  const rest = ref.slice('s3://'.length);
  const slash = rest.indexOf('/');
  return { bucket: rest.slice(0, slash), key: rest.slice(slash + 1) };
}
