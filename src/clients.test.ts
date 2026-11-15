import { describe, expect, it } from 'vitest';
import { createClients, destroyClients } from './clients.js';

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
