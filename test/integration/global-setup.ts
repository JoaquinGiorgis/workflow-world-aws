/**
 * Global setup for the integration tests.
 *
 * 1. Uses the emulator at WORKFLOW_AWS_ENDPOINT (default
 *    http://localhost:4566). When nothing answers there and the endpoint is
 *    local, starts `moto_server` on that port (MOTO_SERVER_BIN, or
 *    `moto_server` on PATH) and stops it again afterwards.
 * 2. Sets dummy `test`/`test` credentials in this process's environment,
 *    which the test workers inherit. Emulators accept any credentials.
 * 3. Creates a fresh table, queues and bucket with the bootstrap script and
 *    hands their names to the tests through `inject('aws')`.
 */
import { type ChildProcess, spawn } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';
import type { TestProject } from 'vitest/node';
import {
  type BootstrapResult,
  bootstrapLocal,
} from '../../scripts/bootstrap-local.js';

export interface IntegrationContext extends BootstrapResult {
  endpoint: string;
  region: string;
  /**
   * The emulator is moto_server, which runs each request on its own thread
   * with no lock around TransactWriteItems. The storage tests then send their
   * DynamoDB requests one at a time; see `test/integration/storage-context.ts`.
   */
  moto: boolean;
}

declare module 'vitest' {
  export interface ProvidedContext {
    aws: IntegrationContext;
  }
}

const DEFAULT_ENDPOINT = 'http://localhost:4566';
const STARTUP_TIMEOUT_MS = 30_000;

async function isUp(endpoint: string): Promise<boolean> {
  return (await serverHeader(endpoint)) !== undefined;
}

/** The `Server` header the endpoint answers with, or undefined if it is down. */
async function serverHeader(endpoint: string): Promise<string | undefined> {
  try {
    const response = await fetch(endpoint, {
      signal: AbortSignal.timeout(1_000),
    });
    return response.headers.get('server') ?? '';
  } catch {
    return undefined;
  }
}

async function startMoto(endpoint: string): Promise<ChildProcess> {
  const url = new URL(endpoint);
  if (url.hostname !== 'localhost' && url.hostname !== '127.0.0.1') {
    throw new Error(
      `Nothing answers at ${endpoint}, and moto_server can only be started for a local endpoint`
    );
  }
  const bin = process.env.MOTO_SERVER_BIN || 'moto_server';
  const port = url.port || '80';
  const child = spawn(bin, ['-H', url.hostname, '-p', port], {
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  let stderr = '';
  child.stderr?.on('data', (chunk: Buffer) => {
    stderr = (stderr + chunk.toString()).slice(-4_000);
  });
  const failed = new Promise<never>((_, reject) => {
    child.once('error', (error) =>
      reject(
        new Error(
          `Could not start ${bin} (${error.message}). Install moto[server] or set MOTO_SERVER_BIN, or start LocalStack at ${endpoint}`
        )
      )
    );
    child.once('exit', (code) =>
      reject(new Error(`${bin} exited with code ${code}:\n${stderr}`))
    );
  });

  const deadline = Date.now() + STARTUP_TIMEOUT_MS;
  const ready = (async () => {
    while (Date.now() < deadline) {
      if (await isUp(endpoint)) return;
      await sleep(200);
    }
    throw new Error(`${bin} did not answer at ${endpoint} within 30s`);
  })();

  try {
    await Promise.race([ready, failed]);
  } catch (error) {
    child.kill();
    throw error;
  }
  failed.catch(() => {});
  return child;
}

export default async function setup(project: TestProject) {
  const endpoint = process.env.WORKFLOW_AWS_ENDPOINT || DEFAULT_ENDPOINT;
  const region = process.env.WORKFLOW_AWS_REGION || 'us-east-1';

  // Dummy credentials for the emulator. Never real ones.
  process.env.AWS_ACCESS_KEY_ID = 'test';
  process.env.AWS_SECRET_ACCESS_KEY = 'test';
  delete process.env.AWS_SESSION_TOKEN;
  delete process.env.AWS_PROFILE;

  let moto: ChildProcess | undefined;
  if (await isUp(endpoint)) {
    console.log(`[integration] using the emulator already at ${endpoint}`);
  } else {
    moto = await startMoto(endpoint);
    console.log(
      `[integration] started moto_server (pid ${moto.pid}) at ${endpoint}`
    );
  }

  const suffix = `it${Date.now().toString(36)}`;
  const resources = await bootstrapLocal({
    region,
    endpoint,
    tableName: `workflow-${suffix}`,
    workflowQueueName: `workflow-${suffix}`,
    stepQueueName: `step-${suffix}`,
    bucket: `workflow-payloads-${suffix}`,
  });

  // moto_server is a Werkzeug app; LocalStack and DynamoDB Local are not.
  const isMoto =
    moto !== undefined ||
    /werkzeug/i.test((await serverHeader(endpoint)) ?? '');
  project.provide('aws', { ...resources, endpoint, region, moto: isMoto });

  return async () => {
    if (moto && moto.exitCode === null) {
      moto.kill('SIGTERM');
      console.log('[integration] stopped moto_server');
    }
  };
}
