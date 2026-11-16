/**
 * The HTTP API: start a greeting, read the run back, read its stream.
 *
 * The same app runs as a Node server (`server.ts`, worker mode) and as a
 * Lambda function URL (`lambda.ts`, Lambda mode).
 */
import { Hono } from 'hono';
import { getRun, start } from 'workflow/api';
import manifest from '../.well-known/workflow/v1/manifest.json' with {
  type: 'json',
};

/** The workflow as `workflow build` registered it. */
const greeting = manifest.workflows['workflows/greeting.ts'].greeting;

export const app = new Hono()
  .post('/greetings/:name', async (c) => {
    const run = await start(greeting, [c.req.param('name')]);
    return c.json({ runId: run.runId }, 202);
  })
  .get('/runs/:runId', async (c) => {
    const run = getRun<string>(c.req.param('runId'));
    const status = await run.status;
    return c.json({
      runId: run.runId,
      status,
      ...(status === 'completed' && { result: await run.returnValue }),
    });
  })
  .get('/runs/:runId/stream', (c) => {
    // Every chunk the run wrote, as it arrives, until the stream closes.
    const chunks = getRun(c.req.param('runId')).getReadable<string>();
    return new Response(chunks.pipeThrough(new TextEncoderStream()), {
      headers: { 'content-type': 'text/plain; charset=utf-8' },
    });
  });
