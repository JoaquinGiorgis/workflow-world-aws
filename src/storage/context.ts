import type { RetryOptions } from './commit.js';
import type { Table } from './table.js';

/** What every part of the storage shares. */
export interface StorageContext {
  table: Table;
  retry?: RetryOptions;
}
