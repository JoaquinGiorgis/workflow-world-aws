/**
 * `experimental_snapshots` storage.
 *
 * Ported from vercel/workflow, packages/world-local/src/storage/
 * snapshots-storage.test.ts (main at b1f92691bcc3bc18b9df094d45379c6464cf4d65).
 * Copyright Vercel, Inc. Licensed under the Apache License, Version 2.0
 * (http://www.apache.org/licenses/LICENSE-2.0). Modified: one DynamoDB table
 * per suite; SQLite reads and writes became item reads and writes. The
 * path-traversal case is skipped (see below).
 */
import { QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import type { World } from '@workflow/world';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { runPk, snapshotKey } from '../../src/keys.js';
import {
  createStorageTestContext,
  type StorageTestContext,
} from './storage-context.js';

describe('snapshots storage (world-aws)', () => {
  let context: StorageTestContext;
  let snapshots: NonNullable<World['experimental_snapshots']>;

  beforeAll(async () => {
    context = await createStorageTestContext('snapshots');
    snapshots = context.world.experimental_snapshots!;
  }, 120_000);

  afterAll(async () => {
    await context?.close();
  });

  it('returns null when no snapshot exists', async () => {
    expect(await snapshots.load('wrun_missing')).toBeNull();
  });

  it('round-trips snapshot bytes and metadata', async () => {
    const data = new Uint8Array([1, 2, 3, 250, 251, 252]);
    const createdAt = new Date('2025-06-01T12:00:00Z');
    await snapshots.save('wrun_a', data, {
      eventsCursor: 'evnt_123',
      createdAt,
    });

    const loaded = await snapshots.load('wrun_a');
    expect(loaded).not.toBeNull();
    expect(new Uint8Array(loaded!.data)).toEqual(data);
    expect(loaded!.metadata.eventsCursor).toBe('evnt_123');
    expect(+loaded!.metadata.createdAt).toBe(+createdAt);
  });

  it('overwrites the previous snapshot on save', async () => {
    await snapshots.save('wrun_a', new Uint8Array([1]), {
      eventsCursor: null,
      createdAt: new Date(),
    });
    await snapshots.save('wrun_a', new Uint8Array([9, 9]), {
      eventsCursor: 'evnt_9',
      createdAt: new Date(),
    });

    const loaded = await snapshots.load('wrun_a');
    expect(new Uint8Array(loaded!.data)).toEqual(new Uint8Array([9, 9]));
    expect(loaded!.metadata.eventsCursor).toBe('evnt_9');
  });

  it('supports a null events cursor', async () => {
    await snapshots.save('wrun_b', new Uint8Array([7]), {
      eventsCursor: null,
      createdAt: new Date(),
    });
    const loaded = await snapshots.load('wrun_b');
    expect(loaded!.metadata.eventsCursor).toBeNull();
  });

  it('delete removes the snapshot (idempotent)', async () => {
    await snapshots.save('wrun_c', new Uint8Array([1]), {
      eventsCursor: null,
      createdAt: new Date(),
    });
    await snapshots.delete('wrun_c');
    expect(await snapshots.load('wrun_c')).toBeNull();
    // Deleting again is a no-op.
    await snapshots.delete('wrun_c');
  });

  it('stores metadata and bytes in ONE item (atomic pairing — no torn save)', async () => {
    await snapshots.save('wrun_atomic', new Uint8Array([1, 2, 3]), {
      eventsCursor: 'evnt_x',
      createdAt: new Date(),
    });
    const page = await context.clients.documents.send(
      new QueryCommand({
        TableName: context.tableName,
        KeyConditionExpression: 'PK = :pk',
        ExpressionAttributeValues: { ':pk': runPk('wrun_atomic') },
      })
    );
    expect(page.Items?.map((item) => item.SK)).toEqual([
      snapshotKey('wrun_atomic').SK,
    ]);
  });

  it('treats a corrupt envelope as a miss instead of returning torn state', async () => {
    await snapshots.save('wrun_corrupt', new Uint8Array([1, 2, 3]), {
      eventsCursor: 'evnt_x',
      createdAt: new Date(),
    });
    await context.clients.documents.send(
      new UpdateCommand({
        TableName: context.tableName,
        Key: snapshotKey('wrun_corrupt'),
        UpdateExpression: 'SET #data = :data',
        ExpressionAttributeNames: { '#data': 'data' },
        ExpressionAttributeValues: { ':data': new Uint8Array([0xde, 0xad]) },
      })
    );
    expect(await snapshots.load('wrun_corrupt')).toBeNull();
  });

  // Skipped: about file paths. world-local names a file after the run id;
  // here the run id is a DynamoDB key component, where `..`, `/` or `.` are
  // plain characters. The ids that do break the key layout (empty, `#`) are
  // rejected by the key helpers (src/keys.test.ts).
  it.skip('rejects path-traversal runIds on every operation', () => {});
});
