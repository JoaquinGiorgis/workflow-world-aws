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

/**
 * What the World keeps free below {@link MAX_TRANSACTION_BYTES} for what
 * DynamoDB counts besides the item values: keys, attribute names and
 * expressions.
 */
export const TRANSACTION_MARGIN_BYTES = 256 * 1024;

/** `WorkflowWorldError.code` of {@link PayloadBucketError}. */
export const PAYLOAD_BUCKET_NOT_FOUND_CODE = 'PAYLOAD_BUCKET_NOT_FOUND';

/**
 * The S3 bucket the World is configured to store large payloads in does
 * not exist, or is in another region than the World's (S3 answers
 * `PermanentRedirect`). Nothing is written.
 */
export class PayloadBucketError extends WorkflowWorldError {
  readonly bucket: string;

  constructor(bucket: string, cause: unknown, otherRegion = false) {
    super(
      otherRegion
        ? `The S3 bucket "${bucket}" is in another region than the AWS World (WORKFLOW_AWS_REGION, or the \`region\` option). The World stores payloads too large for DynamoDB or SQS there; it is set with the \`bucket\` option of createWorld() or the WORKFLOW_AWS_BUCKET environment variable. Use a bucket in the World's region.`
        : `The S3 bucket "${bucket}" does not exist. The AWS World stores payloads too large for DynamoDB or SQS there; it is set with the \`bucket\` option of createWorld() or the WORKFLOW_AWS_BUCKET environment variable. Create the bucket, or point the setting at one that exists.`,
      { status: 500, code: PAYLOAD_BUCKET_NOT_FOUND_CODE, cause }
    );
    this.name = 'PayloadBucketError';
    this.bucket = bucket;
  }
}

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
 * input or output, run input, hook payload, VM snapshot or stream chunk),
 * a transaction over DynamoDB's 4 MB, or a queue message over SQS's size,
 * and the World cannot move enough of it to S3: no bucket is configured, or
 * what is left does not fit either. Nothing is written or sent.
 */
export class PayloadTooLargeError extends WorkflowWorldError {
  /** The size, when the World measured it (not when DynamoDB refused). */
  readonly bytes: number | undefined;
  readonly limit: number;

  constructor(
    what: string,
    bytes: number | undefined,
    options: {
      /** Default {@link MAX_ITEM_BYTES}. */
      limit?: number;
      /** What the limit is; default "the AWS World stores in one DynamoDB item". */
      where?: string;
      /** Whether the large values went to S3 already (a bucket is set). */
      offloaded?: boolean;
    } = {}
  ) {
    const limit = options.limit ?? MAX_ITEM_BYTES;
    const where =
      options.where ??
      (limit === MAX_TRANSACTION_BYTES
        ? 'DynamoDB accepts in one transaction'
        : 'the AWS World stores in one DynamoDB item');
    const advice = options.offloaded
      ? 'It does not fit even with its large values stored in S3.'
      : 'Payloads this large are stored in S3: configure a bucket with the `bucket` option of createWorld() or the WORKFLOW_AWS_BUCKET environment variable.';
    super(
      `${what} is ${bytes === undefined ? 'larger than DynamoDB accepts' : `${bytes} bytes`}, over the ${limit} bytes ${where}. ${advice}`,
      { status: 413, code: PAYLOAD_TOO_LARGE_CODE }
    );
    this.name = 'PayloadTooLargeError';
    this.bytes = bytes;
    this.limit = limit;
  }
}
