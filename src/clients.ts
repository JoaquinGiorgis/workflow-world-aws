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
 * Creates the SDK v3 clients the World uses, all in one region. With an
 * `endpoint` (LocalStack, moto), every client talks to it, and S3 switches to
 * path-style addressing because emulators do not serve bucket subdomains.
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
  const sqs = new SQSClient(base);
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
