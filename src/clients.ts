import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { S3Client } from '@aws-sdk/client-s3';
import { SQSClient } from '@aws-sdk/client-sqs';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { AwsWorldConfig } from './config.js';

export interface AwsClients {
  dynamodb: DynamoDBClient;
  /** Document client over `dynamodb`: plain JS values in and out. */
  documents: DynamoDBDocumentClient;
  sqs: SQSClient;
  s3: S3Client;
}

export type ClientConfig = Pick<AwsWorldConfig, 'region' | 'endpoint'>;

/**
 * How long one attempt of an SQS request may take before the SDK abandons
 * it with a `TimeoutError`: longer than a long poll (`WaitTimeSeconds` is at
 * most 20 s), so `ReceiveMessage` is never cut short.
 */
export const SQS_REQUEST_TIMEOUT_MS = 30_000;

/** Attempts per SQS request, the first included (the SDK's default). */
export const SQS_MAX_ATTEMPTS = 3;

/** The SDK's longest backoff between two attempts. */
const SDK_MAX_RETRY_DELAY_MS = 20_000;

/**
 * The longest one SQS request can take, retries included: every attempt
 * times out and every backoff is the longest. The queue's idempotency keeps
 * a send's reservation well past this (`RESERVATION_TIMEOUT_MS`).
 */
export const SQS_MAX_SEND_MS =
  SQS_MAX_ATTEMPTS * SQS_REQUEST_TIMEOUT_MS +
  (SQS_MAX_ATTEMPTS - 1) * SDK_MAX_RETRY_DELAY_MS;

/**
 * Creates the SDK v3 clients the World uses, all in one region. With an
 * `endpoint` (LocalStack, moto), every client talks to it, and S3 switches to
 * path-style addressing because emulators do not serve bucket subdomains.
 * The SQS client is bounded to {@link SQS_MAX_SEND_MS} per request.
 * Credentials come from the SDK's default provider chain.
 */
export function createClients(config: ClientConfig): AwsClients {
  const base = {
    region: config.region,
    ...(config.endpoint !== undefined && { endpoint: config.endpoint }),
  };

  const dynamodb = new DynamoDBClient(base);
  const documents = DynamoDBDocumentClient.from(dynamodb, {
    marshallOptions: {
      removeUndefinedValues: true,
      convertClassInstanceToMap: false,
    },
    unmarshallOptions: { wrapNumbers: false },
  });
  const sqs = new SQSClient({
    ...base,
    maxAttempts: SQS_MAX_ATTEMPTS,
    // Without throwOnRequestTimeout the SDK only warns past requestTimeout.
    requestHandler: {
      requestTimeout: SQS_REQUEST_TIMEOUT_MS,
      throwOnRequestTimeout: true,
    },
  });
  const s3 = new S3Client({
    ...base,
    ...(config.endpoint !== undefined && { forcePathStyle: true }),
  });

  return { dynamodb, documents, sqs, s3 };
}

/** Releases the sockets held by every client. */
export function destroyClients(clients: AwsClients): void {
  clients.documents.destroy();
  clients.dynamodb.destroy();
  clients.sqs.destroy();
  clients.s3.destroy();
}
