import { describe, expect, it } from 'vitest';
import {
  createClients,
  destroyClients,
  SQS_MAX_ATTEMPTS,
  SQS_MAX_SEND_MS,
  SQS_REQUEST_TIMEOUT_MS,
} from './clients.js';

describe('createClients', () => {
  it('uses the configured endpoint for every client', async () => {
    const clients = createClients({
      region: 'us-east-1',
      endpoint: 'http://localhost:4566',
    });
    try {
      for (const client of [clients.dynamodb, clients.sqs, clients.s3]) {
        expect(await client.config.region()).toBe('us-east-1');
        const endpoint = await client.config.endpoint?.();
        expect(endpoint?.hostname).toBe('localhost');
        expect(endpoint?.port).toBe(4566);
        expect(endpoint?.protocol).toBe('http:');
      }
      expect(clients.s3.config.forcePathStyle).toBe(true);
    } finally {
      destroyClients(clients);
    }
  });

  it('leaves endpoint resolution to the SDK without an override', async () => {
    const clients = createClients({ region: 'eu-west-1' });
    try {
      expect(await clients.sqs.config.region()).toBe('eu-west-1');
      expect(clients.sqs.config.endpoint).toBeUndefined();
      expect(clients.s3.config.forcePathStyle).toBe(false);
    } finally {
      destroyClients(clients);
    }
  });

  it('bounds how long one SQS request can take, retries included', async () => {
    const clients = createClients({ region: 'us-east-1' });
    try {
      expect(await clients.sqs.config.maxAttempts()).toBe(SQS_MAX_ATTEMPTS);
      // The handler resolves its options lazily; read them as it will.
      const handler = clients.sqs.config.requestHandler as unknown as {
        configProvider: Promise<Record<string, unknown>>;
      };
      expect(await handler.configProvider).toMatchObject({
        requestTimeout: SQS_REQUEST_TIMEOUT_MS,
        throwOnRequestTimeout: true,
      });
      // A long poll (up to 20 s) must fit in one attempt.
      expect(SQS_REQUEST_TIMEOUT_MS).toBeGreaterThan(20_000);
      // Every attempt times out, plus the SDK's backoff (at most 20 s each).
      expect(SQS_MAX_SEND_MS).toBe(
        SQS_MAX_ATTEMPTS * SQS_REQUEST_TIMEOUT_MS +
          (SQS_MAX_ATTEMPTS - 1) * 20_000
      );
    } finally {
      destroyClients(clients);
    }
  });

  it('wraps the DynamoDB client in a document client', () => {
    const clients = createClients({ region: 'us-east-1' });
    try {
      expect(clients.documents.config.region).toBe(
        clients.dynamodb.config.region
      );
    } finally {
      destroyClients(clients);
    }
  });
});
