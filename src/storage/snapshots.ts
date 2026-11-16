import type { SnapshotMetadata, Storage } from '@workflow/world';
import {
  decodeSnapshotEnvelope,
  encodeSnapshotEnvelope,
} from '@workflow/world';
import { snapshotKey } from '../keys.js';
import type { Table } from './table.js';
import { fromStorable } from './values.js';

/**
 * VM snapshots: one item per run, holding the snapshot envelope (metadata
 * and bytes in one blob, see `encodeSnapshotEnvelope`), so a load never
 * pairs one suspension's bytes with another's metadata. `eventsCursor` and
 * `createdAt` are copied next to it for inspection only.
 *
 * Snapshots over the item limit fail with `PayloadTooLargeError` until the
 * S3 offload lands.
 */
export function createSnapshotsStorage(
  table: Table
): NonNullable<Storage['experimental_snapshots']> {
  return {
    async save(runId: string, data: Uint8Array, metadata: SnapshotMetadata) {
      await table.put({
        ...snapshotKey(runId),
        type: 'snapshot',
        runId,
        data: encodeSnapshotEnvelope(metadata, data),
        eventsCursor: metadata.eventsCursor,
        createdAt: new Date(metadata.createdAt).toISOString(),
      });
    },

    async load(runId: string) {
      const item = await table.get(snapshotKey(runId));
      const data = fromStorable(item?.data);
      if (!(data instanceof Uint8Array)) return null;
      // Anything that does not decode is a clean miss: full replay.
      return decodeSnapshotEnvelope(data);
    },

    async delete(runId: string) {
      await table.delete(snapshotKey(runId));
    },
  };
}
