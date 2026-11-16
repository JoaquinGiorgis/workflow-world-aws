# @joaquingiorgis/world-aws

A [Workflow SDK](https://github.com/vercel/workflow) World backed by AWS:
DynamoDB for storage, SQS for the queue and S3 for large payloads.

> **Status: work in progress, M1.** Storage is implemented: runs, steps,
> events, hooks, waits and VM snapshots in one DynamoDB table. The queue
> (`not implemented yet (M2)`) and streams (`not implemented yet (M3)`) are
> not, so the World cannot run workflows yet. The package name is
> provisional and nothing is published to npm.

## Configuration

| Variable                          | Required | Description                                                                   |
| --------------------------------- | -------- | ----------------------------------------------------------------------------- |
| `WORKFLOW_AWS_REGION`             | yes      | AWS region, for example `us-east-1`                                           |
| `WORKFLOW_AWS_TABLE`              | yes      | DynamoDB table name                                                           |
| `WORKFLOW_AWS_WORKFLOW_QUEUE_URL` | yes      | SQS queue URL for workflow messages                                           |
| `WORKFLOW_AWS_STEP_QUEUE_URL`     | yes      | SQS queue URL for step messages                                               |
| `WORKFLOW_AWS_BUCKET`             | no       | S3 bucket for payloads over the DynamoDB item limit                           |
| `WORKFLOW_AWS_ENDPOINT`           | no       | Endpoint override for LocalStack or moto, for example `http://localhost:4566` |

Credentials come from the standard AWS SDK v3 provider chain. The package
never reads or stores them itself.

| Variable                                 | Default | Description                                                  |
| ---------------------------------------- | ------- | ------------------------------------------------------------ |
| `WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS` | `30`    | Longest `experimental_minRetention` a hook may ask for, days |

`createWorld({ clients })` accepts SDK clients you created yourself (for
instrumentation or custom credentials); the World does not destroy them on
`close()`.

## Storage

Everything lives in one table (on-demand billing, TTL on `expiresAt`):

| Item                | PK                  | SK                     |
| ------------------- | ------------------- | ---------------------- |
| Run                 | `RUN#<runId>`       | `META`                 |
| Event               | `RUN#<runId>`       | `EVT#<eventId>`        |
| Step                | `RUN#<runId>`       | `STEP#<stepId>`        |
| Hook                | `RUN#<runId>`       | `HOOK#<hookId>`        |
| Wait                | `RUN#<runId>`       | `WAIT#<correlationId>` |
| VM snapshot         | `RUN#<runId>`       | `SNAPSHOT`             |
| Hook resume id      | `RUN#<runId>`       | `RESUME#<resumeId>`    |
| `attr_set` id       | `RUN#<runId>`       | `ATTR#<correlationId>` |
| Start Hook decision | `RUN#<runId>`       | `ADMISSION`            |
| Hook id → run       | `HOOKID#<hookId>`   | `HOOK`                 |
| Hook token claim    | `HOOKTOKEN#<token>` | `TOKEN`                |

GSI1 lists every run (`RUNS`) and every hook (`HOOKS`) by id; GSI2 lists
the runs of one workflow (`WF#<workflowName>`). Both are eventually
consistent, like any DynamoDB index, so `runs.list` and `hooks.list`
without a `runId` can miss a write from the last moments. Everything else
is read with strongly consistent reads.

Each event write is one `TransactWriteItems`: the run item's counters
(`seq`, the last event slot; `seqInBand`; `rev`, moved by every write to the
run) are updated on the condition that `rev` is what the write read, the
events are put at the next slots (`evnt_` + the zero-padded position), and
the entities the event changes are written in the same transaction. A
writer that loses a race is cancelled and decides again on fresh reads,
with backoff and jitter, so positions are dense and unique. Hook tokens are
claimed with a conditional put on `HOOKTOKEN#<token>`; a claim that loses
learns the owner from the transaction's cancellation reasons and answers
`hook_conflict` with `conflictingRunId`.

Capabilities: `hookRetention`, `atomicStartHook`, `hookResumeDedup`,
`hookForceClaim`, `dynamicWorkflowCode` and `inBandFence` (spec 9 runs on
`WORKFLOW_SPEC_VERSION=9`). `runs.waitForTerminalStatus` polls the run
every 500 ms.

Limits:

- An item is at most 400 KB in DynamoDB. A write that would store a larger
  payload (step input or output, run input, hook payload, VM snapshot) is
  refused with `PayloadTooLargeError` (`status: 413`,
  `code: 'PAYLOAD_TOO_LARGE'`) before anything is written. Offloading large
  payloads to S3 is planned (M3).
- `events.listByCorrelationId` reads the run's log with a filter, so its
  cost grows with the length of the log.
- The `RUNS` and `HOOKS` index partitions take every run and hook: at very
  high write rates they become hot partitions.

## Development

Requires Node.js 22 or later and pnpm (through corepack).

```bash
pnpm install
pnpm lint && pnpm typecheck && pnpm test
pnpm build
```

Integration tests run against a local AWS emulator. They start
`moto_server` on port 4566 when nothing answers at `WORKFLOW_AWS_ENDPOINT`
(set `MOTO_SERVER_BIN` if it is not on `PATH`), or use LocalStack if it is
already running. The storage suites include the storage tests of
world-postgres and world-local, ported, and the `@workflow/world`
conformance suites for the in-band fence and the atomic start Hook.
moto_server does not isolate concurrent `TransactWriteItems`, so against
moto the suites send their DynamoDB requests one at a time (see
`test/integration/storage-context.ts`):

```bash
WORKFLOW_AWS_ENDPOINT=http://localhost:4566 pnpm test:integration
```

To create the table, queues and bucket by hand against an emulator:

```bash
WORKFLOW_AWS_ENDPOINT=http://localhost:4566 pnpm bootstrap:local
```

## Roadmap

- M0: scaffold, config, key layout, local bootstrap, CI
- M1: storage (runs, events, steps, hooks, waits, snapshots) — done
- M2: SQS queue (worker mode, long delays, idempotency, DLQ)
- M3: streams, S3 offload, full `@workflow/world-testing` suite
- M4: docs, IAM policy, example, Lambda mode, first release

## License

MIT
