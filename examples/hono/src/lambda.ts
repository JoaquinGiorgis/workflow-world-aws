/**
 * Lambda mode: the two functions of `template.yaml`, from one bundle.
 *
 * - `api`: the HTTP API behind a function URL. It starts runs (sending
 *   messages to SQS) and reads them back; it runs no worker.
 * - `queue`: the target of the SQS event source mappings on the workflow
 *   and step queues. It answers `{ batchItemFailures }`, so the mappings
 *   need `ReportBatchItemFailures`.
 */
import { createSqsLambdaHandler } from '@joaquingiorgis/world-aws';
import { handle } from 'hono/aws-lambda';
import { app } from './app.ts';
import { world } from './world.ts';

export const api = handle(app);

export const queue = createSqsLambdaHandler({ world });
