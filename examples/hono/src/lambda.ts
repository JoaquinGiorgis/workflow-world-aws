/**
 * Lambda mode: the two functions of `template.yaml`, from one bundle.
 *
 * - `api`: the HTTP API behind a function URL. It starts runs (sending
 *   messages to SQS) and reads them back; it runs no worker. It answers
 *   once `world.settled()` resolves: some sends mark their idempotency key
 *   sent after `queue()` returns, and Lambda may freeze the process as
 *   soon as the function answers.
 * - `queue`: the target of the SQS event source mappings on the workflow
 *   and step queues. It answers `{ batchItemFailures }`, so the mappings
 *   need `ReportBatchItemFailures`.
 * - `recover`: invoked on a schedule. `queue` recovers runs on the first
 *   event of each process only; this recovers the ones whose next message
 *   was lost while no event arrives.
 */
import { createSqsLambdaHandler } from '@joaquingiorgis/world-aws';
import { handle } from 'hono/aws-lambda';
import { app } from './app.ts';
import { world } from './world.ts';

const http = handle(app);

export const api = async (...args: Parameters<typeof http>) => {
  const response = await http(...args);
  await world.settled();
  return response;
};

export const queue = createSqsLambdaHandler({ world });

export const recover = async () => ({ recovered: await world.recover() });
