/**
 * Worker mode: one Node process serves the API and runs the queue worker,
 * which long-polls the workflow and step queues.
 */
import { serve } from '@hono/node-server';
import { app } from './app.ts';
import { world } from './world.ts';

// Re-enqueues the runs that have not ended (at most once per interval per
// table), then starts polling.
await world.start();

const server = serve(
  { fetch: app.fetch, port: Number(process.env.PORT ?? 3000) },
  (info) => console.log(`worker mode: listening on http://localhost:${info.port}`)
);

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    server.close();
    // Stops receiving, waits for running deliveries (shutdownGraceMs).
    void world.close().then(() => process.exit(0));
  });
}
