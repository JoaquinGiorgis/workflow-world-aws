import { WorkflowWorldError } from '@workflow/errors';

/** `WorkflowWorldError.code` of {@link PayloadTooLargeError}. */
export const PAYLOAD_TOO_LARGE_CODE = 'PAYLOAD_TOO_LARGE';

/**
 * Largest item this World writes. DynamoDB refuses items over 400 KB; the
 * margin covers the key and index attributes.
 */
export const MAX_ITEM_BYTES = 350 * 1024;

/** DynamoDB's own item size limit. */
export const DYNAMODB_ITEM_BYTES = 400 * 1024;

/** DynamoDB's limit for one TransactWriteItems request. */
export const MAX_TRANSACTION_BYTES = 4 * 1024 * 1024;

/** `WorkflowWorldError.code` of {@link MissingTableIndexError}. */
export const MISSING_TABLE_INDEX_CODE = 'MISSING_TABLE_INDEX';

/**
 * The table lacks an index this World reads: a table created by an older
 * bootstrap (GSI3, the runs that have not ended, came with the SQS queue).
 * Raised instead of DynamoDB's bare `ValidationException`, with the fix.
 */
export class MissingTableIndexError extends WorkflowWorldError {
  readonly tableName: string;
  readonly indexName: string;

  constructor(tableName: string, indexName: string, cause?: unknown) {
    super(
      `The DynamoDB table "${tableName}" has no index "${indexName}", which this version of the AWS World reads${indexName === 'GSI3' ? ' (GSI3 lists the runs that have not ended, for recovery on start and for runs.list by status)' : ''}. On a local emulator, run \`pnpm bootstrap:local\` again: it adds the index to an existing table. On AWS, add it with UpdateTable (GlobalSecondaryIndexUpdates: Create ${indexName}, keys ${indexName}PK/${indexName}SK as strings, projection ALL) and backfill the existing items, as the README describes under "Upgrading a table".`,
      {
        status: 500,
        code: MISSING_TABLE_INDEX_CODE,
        ...(cause !== undefined && { cause }),
      }
    );
    this.name = 'MissingTableIndexError';
    this.tableName = tableName;
    this.indexName = indexName;
  }
}

/**
 * A write would store an item over {@link MAX_ITEM_BYTES} (a large step
 * input or output, run input, hook payload or VM snapshot). Moving such
 * payloads to S3 is planned; until then the write is refused before it
 * reaches DynamoDB, and nothing is written.
 */
export class PayloadTooLargeError extends WorkflowWorldError {
  /** The size, when the World measured it (not when DynamoDB refused). */
  readonly bytes: number | undefined;
  readonly limit: number;

  constructor(
    what: string,
    bytes: number | undefined,
    limit: number = MAX_ITEM_BYTES
  ) {
    super(
      `${what} is ${bytes === undefined ? 'larger than DynamoDB accepts' : `${bytes} bytes`}, over the ${limit} bytes the AWS World stores in one DynamoDB item. Payloads this large need S3 offload, which this version does not have yet.`,
      { status: 413, code: PAYLOAD_TOO_LARGE_CODE }
    );
    this.name = 'PayloadTooLargeError';
    this.bytes = bytes;
    this.limit = limit;
  }
}
