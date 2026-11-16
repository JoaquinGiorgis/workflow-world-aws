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
