// Starts a greeting, prints its stream as it arrives, then the finished run.
//
//   node scripts/try.mjs [name] [base URL]
const name = process.argv[2] ?? 'world';
const base = process.argv[3] ?? `http://localhost:${process.env.PORT ?? 3000}`;

const started = await fetch(`${base}/greetings/${encodeURIComponent(name)}`, {
  method: 'POST',
});
const { runId } = await started.json();
console.log(`POST /greetings/${name} -> ${started.status} ${runId}`);

const stream = await fetch(`${base}/runs/${runId}/stream`);
process.stdout.write(`GET /runs/${runId}/stream -> `);
for await (const chunk of stream.body) {
  process.stdout.write(new TextDecoder().decode(chunk));
}
process.stdout.write('\n');

const deadline = Date.now() + 60_000;
for (;;) {
  const run = await (await fetch(`${base}/runs/${runId}`)).json();
  if (run.status === 'completed' || run.status === 'failed') {
    console.log(`GET /runs/${runId} -> ${JSON.stringify(run)}`);
    process.exitCode = run.status === 'completed' ? 0 : 1;
    break;
  }
  if (Date.now() > deadline) {
    console.log(`GET /runs/${runId} -> still ${run.status} after 60 s`);
    process.exitCode = 1;
    break;
  }
  await new Promise((resolve) => setTimeout(resolve, 250));
}
