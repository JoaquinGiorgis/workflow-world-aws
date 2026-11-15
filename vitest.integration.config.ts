import { defineConfig } from 'vitest/config';

// Integration tests against a local AWS emulator (moto_server or LocalStack)
// at WORKFLOW_AWS_ENDPOINT. The global setup starts moto_server when nothing
// answers there, and creates the table, queues and bucket.
export default defineConfig({
  test: {
    include: ['test/integration/**/*.test.ts'],
    globalSetup: ['test/integration/global-setup.ts'],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    fileParallelism: false,
  },
});
