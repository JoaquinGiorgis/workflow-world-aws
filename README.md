# @joaquingiorgis/world-aws

A [Workflow SDK](https://github.com/vercel/workflow) World backed by AWS:
DynamoDB for storage, SQS for the queue and S3 for large payloads.

> **Status: work in progress, M2.** Storage (runs, steps, events, hooks,
> waits and VM snapshots in one DynamoDB table) and the queue (two SQS
> standard queues with a worker in the app's process) are implemented, and
> workflows that do not use streams run. Streams throw
> `not implemented yet (M3)`. The package name is provisional and nothing
> is published to npm.

## Configuration

| Variable                          | Required | Description                                                                   |
| --------------------------------- | -------- | ----------------------------------------------------------------------------- |
| `WORKFLOW_AWS_REGION`             | yes      | AWS region, for example `us-east-1`                                           |
| `WORKFLOW_AWS_TABLE`              | yes      | DynamoDB table name                                                           |
| `WORKFLOW_AWS_WORKFLOW_QUEUE_URL` | yes      | SQS queue URL for workflow messages                                           |
| `WORKFLOW_AWS_STEP_QUEUE_URL`     | yes      | SQS queue URL for step messages                                               |
| `WORKFLOW_AWS_BUCKET`             | no       | S3 bucket for payloads over the DynamoDB item limit                           |
| `WORKFLOW_AWS_ENDPOINT`           | no       | Endpoint override for LocalStack or moto, for example `http://localhost:4566` |
| `WORKFLOW_AWS_BASE_URL`           | no       | Base URL of the app the queue delivers to over HTTP (see [Queue](#queue))     |

Credentials come from the standard AWS SDK v3 provider chain. The package
never reads or stores them itself.

| Variable                                  | `createWorld()` option     | Default | Description                                                     |
| ----------------------------------------- | -------------------------- | ------- | --------------------------------------------------------------- |
| `WORKFLOW_AWS_CONCURRENCY`                | `concurrency`              | `10`    | Messages each queue (workflow, step) runs at once               |
| `WORKFLOW_AWS_VISIBILITY_TIMEOUT_SECONDS` | `visibilityTimeoutSeconds` | `60`    | Visibility timeout of a received message, renewed while it runs |
| `WORKFLOW_AWS_SHUTDOWN_GRACE_MS`          | `shutdownGraceMs`          | `30000` | How long `close()` waits for running deliveries                 |
| `WORKFLOW_AWS_POLL_WAIT_SECONDS`          | `pollWaitSeconds`          | `20`    | Long polling wait of each `ReceiveMessage` (0-20)               |
| `WORKFLOW_AWS_STREAM_POLL_INTERVAL_MS`    | `streamPollIntervalMs`     | `250`   | Poll interval of live stream reads (M3)                         |
| `WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS`  |                            | `30`    | Longest `experimental_minRetention` a hook may ask for, days    |

Every variable but the last can also be passed to `createWorld()`
(`region`, `tableName`, `workflowQueueUrl`, `stepQueueUrl`, `bucket`,
`endpoint`, `baseUrl` and the options above); an explicit option wins
over its variable.

`createWorld({ clients })` accepts SDK clients you created yourself (for
instrumentation or custom credentials); the World does not destroy them on
`close()`. The SQS client the World creates gives up on a request after 3
attempts of at most 30 seconds each (`maxAttempts: 3`, `requestHandler:
{ requestTimeout: 30_000, throwOnRequestTimeout: true }`), so one request
takes at most 130 seconds with the SDK's backoff; an SQS client of your own
should be bounded the same way (see Idempotency under [Queue](#queue)). `createWorld({ retry })` tunes how a write that loses a race with
another writer of the same run is retried (`maxAttempts`,
`immediateAttempts`, `baseDelayMs`, `maxDelayMs`): by default 40 attempts,
the first two at once and then with exponential backoff and full jitter
capped at 250 ms, after which the write fails with a 503
`WorkflowWorldError`.

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
the runs of one workflow (`WF#<workflowName>`); GSI3 is sparse and lists
only the runs that have not ended (`ACTIVE`): a run item carries its GSI3
attributes from its creation until the write that ends the run removes
them, in the same transaction. `runs.list` filtered to `pending` and/or
`running` (without a workflow name), which startup recovery uses, reads
GSI3, so it does not grow with the number of finished runs. The indexes
are eventually consistent, like any DynamoDB index, so `runs.list` and
`hooks.list` without a `runId` can miss a write from the last moments.
Everything else is read with strongly consistent reads. The queue's
idempotency keys live in the same table (`IDEMP#<key>` / `Q`, see
[Queue](#queue)).

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

- DynamoDB stores items of up to 400 KB. The World refuses any item over
  350 KB, keeping the rest as margin for key and index attributes: a write
  that would store a larger payload (step input or output, run input or
  output, hook payload, VM snapshot), or grow the run item past it (input,
  output, error and attributes live on one item), is refused with
  `PayloadTooLargeError` (`status: 413`, `code: 'PAYLOAD_TOO_LARGE'`) before
  anything is written. If DynamoDB itself refuses an item for its size, the
  same error is raised. Offloading large payloads to S3 is planned (M3).
- Pagination cursors of `runs.list`, `steps.list` and `hooks.list` are
  opaque and only valid for the list (index, run or workflow) that returned
  them; any other cursor is refused with a 400 `WorkflowWorldError`.
- `events.listByCorrelationId` reads the run's log with a filter, so its
  cost grows with the length of the log.
- The `RUNS`, `HOOKS` and `ACTIVE` index partitions take every run, hook
  and active run: at very high write rates they become hot partitions.
- A table created before GSI3 needs the index added; see
  [Upgrading a table](#upgrading-a-table). Until then `start()` and
  `runs.list` by status fail with `MissingTableIndexError`
  (`code: 'MISSING_TABLE_INDEX'`), which names the index and the fix.

### Upgrading a table

A table created by an older bootstrap has GSI1 and GSI2 but not GSI3 (the
runs that have not ended). Against an emulator, run
`pnpm bootstrap:local` again: it adds GSI3 to the existing table, waits for
it and backfills the runs. On AWS, add the index once:

```bash
aws dynamodb update-table --table-name <table> \
  --attribute-definitions AttributeName=GSI3PK,AttributeType=S AttributeName=GSI3SK,AttributeType=S \
  --global-secondary-index-updates \
  '[{"Create":{"IndexName":"GSI3","KeySchema":[{"AttributeName":"GSI3PK","KeyType":"HASH"},{"AttributeName":"GSI3SK","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]}'
```

and, once it is `ACTIVE`, backfill it as a one-off step: every run item
(`SK = META`) whose `status` is not `completed`, `failed` or `cancelled`
gets `GSI3PK = ACTIVE` and `GSI3SK = <runId>`. Runs created afterwards
carry the attributes from their creation. `backfillActiveRunIndex(clients,
tableName)` in `scripts/bootstrap-local.ts` does exactly that (a scan and a
conditional update per run, safe to run again) with whatever clients you
pass it.

## Queue

Two SQS standard queues, each with its own dead-letter queue: the workflow
queue carries orchestrator messages and the step queue step messages
(those with a `stepId`), so the two poll and scale apart. FIFO queues are
not supported: they have no per-message delay.

- **Messages.** The SQS body holds the payload (JSON, with `Uint8Array`
  values as tagged base64), the full queue name, when it was first
  enqueued and a message id of the World's own (`msg_<ulid>`). That id is
  `meta.messageId` on every delivery of the message; SQS's own `MessageId`
  changes whenever the World sends a message again.
- **Idempotency.** A send with `idempotencyKey` first reserves the key with
  a conditional put on `IDEMP#<key>` / `Q` (absent, or expired), which
  records the message id it sends under, and marks it sent afterwards. A
  later send under the key answers that id and sends nothing. A failed send
  gives the key back; a reservation whose sender died is taken over after
  10 minutes, under the same id. That is well past the longest an SQS
  request can take (130 seconds, see [Configuration](#configuration)), so
  a sender that is still sending is not overtaken. Marking the key sent
  and giving it back only apply to the sender's own reservation; a sender
  that was overtaken anyway logs a warning, since the message may then be
  in the queue twice. Keys are kept for 24 hours (`expiresAt`,
  DynamoDB TTL), whether or not the message was delivered, as on Vercel
  Queues.
- **Delays.** Up to 15 minutes with SQS's `DelaySeconds`. A longer delay is
  sent with 15 minutes and a `notBefore` time, and each early delivery
  sends the message on with the rest of the delay (not counted as a
  delivery).
- **Worker.** `start()` runs, per queue, `ceil(concurrency / 10)` loops of
  `ReceiveMessage` (long polling) sharing `concurrency` slots. Each message
  is delivered to the handler registered for its prefix in this process
  (`registerHandler`), or else POSTed to the prefix's route
  (`registerRoute`) or the Workflow SDK's flow route on the app's base
  URL: `baseUrl`, else `WORKFLOW_LOCAL_BASE_URL`, else
  `http://localhost:$PORT`. A heartbeat (`ChangeMessageVisibility`) keeps
  it invisible while it runs. It is deleted when the handler acknowledges
  it; a `{ timeoutSeconds }` answer sends the same message again with that
  delay (attempts counted as in world-postgres); a failure leaves it in the
  queue, invisible for `e^n` seconds (`n` is its receive count; at most an
  hour), so the queue's `maxReceiveCount` moves a message that keeps
  failing to the dead-letter queue. `meta.attempt` and `meta.deliveryCount`
  are SQS's `ApproximateReceiveCount` plus the deliveries of earlier copies
  of the message. A spec 9 run's orchestrator deliveries wait for each
  other within a process; across processes the in-band fence keeps an
  overlap safe, which is why `maxConcurrency` is not declared.
- **Shutdown.** `close()` stops receiving, gives back the messages it has
  not started, waits up to `shutdownGraceMs` for the running deliveries,
  then stops their heartbeats without acknowledging them (SQS delivers them
  again after their visibility timeout) and releases the clients. It is
  idempotent. One worker runs per process for a set of queues, even if
  several World instances call `start()`.
- **Recovery.** Before `start()` starts the worker, every run that has not
  ended gets a message (`reenqueueActiveRuns`), as in world-postgres. A
  duplicate only costs a replay, and a run created a moment before (which
  the eventually consistent index may miss) has its own message in SQS.
  If recovery fails, `start()` rejects without starting the worker, and the
  next `start()` tries both again.
- **Authentication.** `queueAuthMode` is `'skip'`, as in world-postgres:
  the runtime does not check requests to the flow route. IAM decides who
  can send to the queues, but anyone who can reach the route can post a
  delivery to it, so do not expose `/.well-known/workflow/v1/flow` beyond
  the app's network, or deliver in process with `registerHandler`.
- `getDeploymentId()` answers `aws`. `queueBatch` uses `SendMessageBatch`
  (10 messages per request) and reports each message.
  `QueueOptions.region`, `retentionSeconds` and `deploymentId` are ignored:
  retention is the queue's, and this World has one deployment.
- `experimental_hold` (in `@workflow/world` versions newer than the 5.2.0
  this package is built against) is ignored: the message is sent as usual
  and the result carries no `experimental_hold`, which the contract reads
  as "the queue delivers it". `experimental_queueHold` is not declared:
  SQS cannot hand its sender a message it just sent (deleting one takes a
  receipt from `ReceiveMessage`), so a hold would need a lease table next
  to the queue.

Give the queues a redrive policy of many receives. The local bootstrap
uses `maxReceiveCount: 73` (world-postgres' job attempts) and 14 days of
retention, so a failing delivery is retried for days and the runtime's own
delivery cap (49) comes first; with 5 receives a message would reach the
dead-letter queue after about a minute and a half of failures. A message
must fit the queue's `MaximumMessageSize` (at most 1 MiB, 256 KiB on many
queues): a larger payload fails the send until S3 offload (M3).

## IAM

The smallest policy the World needs so far (the bucket comes with M3):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:BatchGetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem",
        "dynamodb:ConditionCheckItem",
        "dynamodb:Query"
      ],
      "Resource": [
        "arn:aws:dynamodb:<region>:<account>:table/<table>",
        "arn:aws:dynamodb:<region>:<account>:table/<table>/index/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility"
      ],
      "Resource": [
        "arn:aws:sqs:<region>:<account>:<workflow-queue>",
        "arn:aws:sqs:<region>:<account>:<step-queue>"
      ]
    }
  ]
}
```

`TransactWriteItems` is authorized by the item actions inside it, and
`SendMessageBatch` by `sqs:SendMessage`. The dead-letter queues need no
permission for the World: SQS moves messages there itself. A process that
only starts runs and never calls `start()` needs no `sqs:ReceiveMessage`,
`sqs:DeleteMessage` or `sqs:ChangeMessageVisibility`.

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
moto the storage suites send their DynamoDB requests one at a time (see
`test/integration/storage-context.ts`). The queue runs end to end against
the emulator's SQS, and the `@workflow/world-testing` suite runs at spec 8
and spec 9 against the World bundled with tsup, each test starting its own
app server with its own table and queues (those servers do send requests
concurrently, moto included); its stream tests are skipped until M3:

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
- M2: SQS queue (worker mode, long delays, idempotency, DLQ) — done
- M3: streams, S3 offload, full `@workflow/world-testing` suite
- M4: docs, IAM policy, example, Lambda mode, first release

## License

MIT
