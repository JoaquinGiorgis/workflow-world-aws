# @joaquingiorgis/world-aws

A [Workflow SDK](https://github.com/vercel/workflow) World backed by AWS:
DynamoDB for storage and streams, SQS for the queue, and S3 for payloads
too large for either.

- One DynamoDB table holds runs, events, steps, hooks, waits, VM snapshots
  and stream chunks. Each event is committed in one transaction against a
  per-run counter, so event positions are dense and unique.
- Two SQS standard queues (workflow and step messages), each with a
  dead-letter queue. Messages are consumed by a worker in your app's
  process, or by a Lambda function behind an SQS event source mapping.
- Values over the DynamoDB item limit and queue messages over 256 KiB go to
  an S3 bucket.
- Passes every test the `@workflow/world-testing` suite runs for a World
  with `queueAuthMode: 'skip'` (it skips its queue-auth tests for those),
  at spec 8 and spec 9 (spec 9 is opt-in, `WORKFLOW_SPEC_VERSION=9`).

See [Status](#status) for what has and has not been tested.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [AWS resources](#aws-resources)
- [Worker mode and Lambda mode](#worker-mode-and-lambda-mode)
- [IAM](#iam)
- [How data is stored](#how-data-is-stored)
- [Limits](#limits)
- [Upgrading a table without GSI3](#upgrading-a-table-without-gsi3)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [Status](#status)

## Install

```bash
pnpm add @joaquingiorgis/world-aws
# or: npm install @joaquingiorgis/world-aws
```

Requires Node.js 22 or later and `@workflow/world` 5.2 or later (a peer
dependency, installed with the `workflow` package). The AWS SDK v3 clients
for DynamoDB, SQS and S3 come as dependencies.

The package is ESM only. `import` it (the Workflow runtime loads Worlds
with `import()`); `require()` from CommonJS works from Node.js 22.12 on.

## Quick start

Point the Workflow SDK at this World and tell it where the resources are:

```bash
WORKFLOW_TARGET_WORLD=@joaquingiorgis/world-aws
WORKFLOW_AWS_REGION=us-east-1
WORKFLOW_AWS_TABLE=workflow
WORKFLOW_AWS_WORKFLOW_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/workflow
WORKFLOW_AWS_STEP_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/step
WORKFLOW_AWS_BUCKET=my-workflow-payloads
```

Then start the queue worker once, when your server starts:

```ts
import { getWorld } from 'workflow/runtime';

const world = await getWorld();
await world.start?.();
```

Or create the World yourself, which also works when your app is bundled
(the runtime cannot load a package by name from inside a bundle):

```ts
import { createWorld } from '@joaquingiorgis/world-aws';
import { setWorld } from 'workflow/runtime';

const world = createWorld({ concurrency: 20 }); // the rest from WORKFLOW_AWS_*
setWorld(world);
await world.start();
```

By default the worker delivers each message over HTTP to your app's flow
route (`/.well-known/workflow/v1/flow`) on `baseUrl`. To deliver in
process instead, register the route's handler:

```ts
import { POST } from './.well-known/workflow/v1/flow.mjs'; // from `workflow build`

world.registerHandler('__wkf_workflow_', POST);
```

[`examples/hono`](examples/hono) is a complete app: a two-step workflow
with a stream, worker mode, Lambda mode, and a SAM template.

## Configuration

Every setting can be passed to `createWorld()` or set as an environment
variable; an explicit option wins over its variable. Empty strings count as
unset. `createWorld()` validates everything up front and throws an
`AwsWorldConfigError` listing every problem.

| Variable                          | Option             | Required | Description                                                                             |
| --------------------------------- | ------------------ | -------- | --------------------------------------------------------------------------------------- |
| `WORKFLOW_AWS_REGION`             | `region`           | yes      | AWS region, for example `us-east-1`                                                     |
| `WORKFLOW_AWS_TABLE`              | `tableName`        | yes      | DynamoDB table                                                                          |
| `WORKFLOW_AWS_WORKFLOW_QUEUE_URL` | `workflowQueueUrl` | yes      | SQS queue for workflow (orchestrator) messages                                          |
| `WORKFLOW_AWS_STEP_QUEUE_URL`     | `stepQueueUrl`     | yes      | SQS queue for step messages; must differ from the workflow queue                        |
| `WORKFLOW_AWS_BUCKET`             | `bucket`           | no       | S3 bucket for large payloads. Without it, such writes fail with a clear error           |
| `WORKFLOW_AWS_ENDPOINT`           | `endpoint`         | no       | Endpoint for every client, for LocalStack or moto (`http://localhost:4566`)             |
| `WORKFLOW_AWS_BASE_URL`           | `baseUrl`          | no       | Base URL for HTTP deliveries. Default `WORKFLOW_LOCAL_BASE_URL`, then `localhost:$PORT` |

| Variable                                  | Option                     | Default | Description                                                                             |
| ----------------------------------------- | -------------------------- | ------- | --------------------------------------------------------------------------------------- |
| `WORKFLOW_AWS_CONCURRENCY`                | `concurrency`              | `10`    | Messages each queue (workflow, step) runs at once; in Lambda mode, records of one event |
| `WORKFLOW_AWS_VISIBILITY_TIMEOUT_SECONDS` | `visibilityTimeoutSeconds` | `60`    | Worker mode: visibility of a received message, renewed while it runs                    |
| `WORKFLOW_AWS_SHUTDOWN_GRACE_MS`          | `shutdownGraceMs`          | `30000` | How long `close()` waits for running deliveries                                         |
| `WORKFLOW_AWS_POLL_WAIT_SECONDS`          | `pollWaitSeconds`          | `20`    | Worker mode: long polling wait of each `ReceiveMessage` (0-20)                          |
| `WORKFLOW_AWS_STREAM_POLL_INTERVAL_MS`    | `streamPollIntervalMs`     | `250`   | How often a live stream reader looks for new chunks                                     |
| `WORKFLOW_AWS_RECOVERY_INTERVAL_SECONDS`  | `recoveryIntervalSeconds`  | `900`   | Startup recovery runs at most once per this many seconds per table; 0 = every start     |
| `WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS`  |                            | `30`    | Longest `experimental_minRetention` a hook may ask for, in days                         |

Other `createWorld()` options:

- `clients`: AWS SDK clients you created yourself (`{ dynamodb, documents,
  sqs, s3 }`), for instrumentation or custom credentials. The World does
  not destroy them on `close()`. `region` and `endpoint` still have to say
  where they point. Bound your SQS client as the World bounds its own: 3
  attempts of at most 30 seconds (`maxAttempts: 3`, `requestHandler:
  { requestTimeout: 30_000, throwOnRequestTimeout: true }`), so one request
  takes at most 130 seconds (see Idempotency under
  [How data is stored](#queue)).
- `retry`: how a write that loses a race with another writer of the same
  run is retried (`maxAttempts`, `immediateAttempts`, `baseDelayMs`,
  `maxDelayMs`). By default 40 attempts, the first two at once, then
  exponential backoff with full jitter capped at 250 ms; after that the
  write fails with a 503 `WorkflowWorldError`.

Credentials come from the standard AWS SDK v3 provider chain (environment,
shared config, IAM role). The package never reads or stores them itself.

## AWS resources

The World needs:

| Resource       | Settings                                                                                                                                                                                |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| DynamoDB table | `PK` (S) / `SK` (S); GSI1 (`GSI1PK`/`GSI1SK`), GSI2 (`GSI2PK`/`GSI2SK`), GSI3 (`GSI3PK`/`GSI3SK`), all string keys, projection `ALL`; TTL on `expiresAt`. On-demand billing recommended |
| Two SQS queues | Standard (not FIFO: FIFO has no per-message delay). Retention 14 days. Redrive to a dead-letter queue after many receives (73 is what this repo uses, see [Queue](#queue))              |
| Two DLQs       | Standard, retention 14 days                                                                                                                                                             |
| S3 bucket      | Optional but recommended, used only by the World. A lifecycle rule expiring `queue/` after 15 days                                                                                      |

Three ways to create them:

- **Local emulator.** `WORKFLOW_AWS_ENDPOINT=http://localhost:4566 pnpm
  bootstrap:local` (from a clone of this repository) creates the table,
  the queues with their dead-letter queues, and the bucket on moto_server
  or LocalStack, and prints the variables to use. It refuses to run
  without an endpoint, so it never touches a real account.
- **SAM.** [`examples/hono/template.yaml`](examples/hono/template.yaml)
  creates all of them plus a Lambda deployment. Copy the resources you
  need.
- **By hand or with your own IaC**, following the table above.

## Worker mode and Lambda mode

Messages can be consumed in two ways. Sending is the same in both: any
process with the World can start runs.

### Worker mode

`world.start()` re-enqueues the runs that have not ended (see
[Recovery](#queue)) and starts a worker in the process. Per queue, it runs
`ceil(concurrency / 10)` long-polling `ReceiveMessage` loops sharing
`concurrency` slots. A heartbeat (`ChangeMessageVisibility`) keeps each
message invisible while it runs. `world.close()` stops receiving, gives
back the messages it has not started, waits up to `shutdownGraceMs` for
running deliveries, and abandons the rest (SQS delivers them again after
their visibility timeout). One worker runs per process for a set of
queues, even if several World instances call `start()`.

Use it on a long-running server: a container, ECS, EC2, a VM.

### Lambda mode

A Lambda function behind an SQS event source mapping on each queue
handles the messages; no worker runs:

```ts
import { createSqsLambdaHandler, createWorld } from '@joaquingiorgis/world-aws';
import { setWorld } from 'workflow/runtime';
import { POST } from './.well-known/workflow/v1/flow.mjs';

const world = createWorld();
setWorld(world);
world.registerHandler('__wkf_workflow_', POST);

export const handler = createSqsLambdaHandler({ world });
```

The handler takes the SQS event and answers `{ batchItemFailures }`. Each
record goes through the same steps as in the worker: the message id comes
from the body, `attempt` and `deliveryCount` from `ApproximateReceiveCount`
plus earlier copies of the message, bodies in S3 are read back, and early
hops of long delays are sent on. Then:

- a delivery that succeeded is left out of `batchItemFailures`, and Lambda
  deletes the message;
- a `{ timeoutSeconds }` answer (a redelivery with a delay) sends the same
  message again with that delay, and the record succeeds. Leaving the
  record in the queue with `ChangeMessageVisibility(timeoutSeconds)` would
  also delay it, but every such redelivery would count a receive towards
  the dead-letter queue;
- a failed delivery is reported in `batchItemFailures`, after the handler
  sets its visibility to the same backoff as the worker (`e^n` seconds for
  the `n`-th receive, at most an hour). Lambda leaves it in the queue. A
  record whose handling throws is backed off and reported the same way.

Records of one event run up to `concurrency` at a time; records of the
same spec 9 run wait for each other. The first event a process handles
also runs `world.recover()` (once per `recoveryIntervalSeconds` per table,
across processes; `createSqsLambdaHandler({ recover: false })` turns it
off). If it fails, the next event tries again, unless the failure would
repeat (a missing GSI3, access denied or another AWS client error that is
not throttling): then a warning says so and only a new process tries
again. Before answering, the handler waits for the idempotency-key
confirmations its deliveries started, since Lambda may freeze the process
right after.

**Recovery needs an event.** Lambda runs the handler only when a message
arrives, and the `api` side never recovers. If the message that would
continue a run is lost and the queues are otherwise idle, the run stays
`running` until some other message arrives. Where the queues can be idle,
recover on a schedule: an EventBridge rule invoking a small function that
calls `world.recover()` (the SAM template in `examples/hono` has one,
every 15 minutes). A periodic no-op message to a queue does not do it: a
warm process recovers only on its first event, and a body that is not a
World message is reported and retried until it reaches the dead-letter
queue. The per-table interval keeps a schedule from recovering more often
than `recoveryIntervalSeconds`.

Setting up the mappings:

- **`FunctionResponseTypes: ['ReportBatchItemFailures']` is required.**
  Without it, Lambda treats a batch whose function returned as a success
  and deletes every record, failed ones included.
- **Visibility timeout of each queue: at least the function's timeout.**
  Otherwise a message still running becomes visible and runs a second time
  in parallel. AWS recommends six times the function timeout.
- `BatchSize` up to 10 keeps an event's records within one function's
  time. A delivery runs a workflow step inline, so size the function
  timeout for your slowest step.
- Lambda's receives count towards `maxReceiveCount` too, including when
  the function is throttled. Keep a high value (73 here) and alarm on the
  dead-letter queues.

The types of the event (`SqsLambdaEvent`, `SqsLambdaRecord`,
`SqsBatchResponse`) are exported; they are a subset of
`@types/aws-lambda`'s `SQSEvent`, so either can be passed.

## IAM

The smallest policy the World needs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "WorkflowTableItems",
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:BatchGetItem",
        "dynamodb:PutItem",
        "dynamodb:UpdateItem",
        "dynamodb:DeleteItem",
        "dynamodb:ConditionCheckItem"
      ],
      "Resource": ["arn:aws:dynamodb:<region>:<account-id>:table/<table>"]
    },
    {
      "Sid": "WorkflowTableQueries",
      "Effect": "Allow",
      "Action": ["dynamodb:Query"],
      "Resource": [
        "arn:aws:dynamodb:<region>:<account-id>:table/<table>",
        "arn:aws:dynamodb:<region>:<account-id>:table/<table>/index/*"
      ]
    },
    {
      "Sid": "WorkflowQueues",
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility"
      ],
      "Resource": [
        "arn:aws:sqs:<region>:<account-id>:<workflow-queue>",
        "arn:aws:sqs:<region>:<account-id>:<step-queue>"
      ]
    },
    {
      "Sid": "WorkflowPayloads",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::<bucket>/*"]
    }
  ]
}
```

The same file is [`docs/iam-policy.json`](docs/iam-policy.json) (also in
the npm package); replace the placeholders. A unit test checks it against
every AWS SDK command in `src/`, so it stays the smallest policy that
works:

- `TransactWriteItems` is authorized by the item actions inside it, and
  `SendMessageBatch` by `sqs:SendMessage`. Only `Query` reads the indexes
  (GSI1, GSI2, GSI3).
- The dead-letter queues need no permission: SQS moves messages there
  itself.
- A process that only starts runs and never runs the worker needs no
  `sqs:ReceiveMessage`, `sqs:DeleteMessage` or
  `sqs:ChangeMessageVisibility`.
- In Lambda mode, the function's code calls `sqs:SendMessage` and
  `sqs:ChangeMessageVisibility`; the event source mapping receives and
  deletes with the function's role too, and also needs
  `sqs:GetQueueAttributes` on both queues (the managed policy
  `AWSLambdaSQSQueueExecutionRole`, or SAM's `SQSPollerPolicy`, grant the
  three).
- Without a bucket the S3 statement can go, unless a World with one wrote
  references earlier (reads then need `s3:GetObject`). The keys are
  `<runId>/...` and `queue/...` at the root of the bucket, so give the
  World a bucket of its own. A bucket encrypted with a customer managed
  KMS key also needs `kms:GenerateDataKey` and `kms:Decrypt` on that key.
- Queues encrypted with a customer managed KMS key (SSE-KMS) need
  `kms:GenerateDataKey` and `kms:Decrypt` on that key for every process
  that sends, and `kms:Decrypt` for the consumer: the worker's role, or in
  Lambda mode the function's role, which the event source mapping uses.
- The policy grants no `s3:ListBucket`, so S3 answers a `GetObject` of a
  missing key with `AccessDenied` instead of `NoSuchKey` (see
  [Troubleshooting](#troubleshooting)).
- Creating the resources (`pnpm bootstrap:local`, the SAM template in
  `examples/hono`) needs more: that is a deploy-time role, not the
  World's.

## How data is stored

### Storage

Everything lives in one table:

| Item                  | PK                      | SK                     |
| --------------------- | ----------------------- | ---------------------- |
| Run                   | `RUN#<runId>`           | `META`                 |
| Event                 | `RUN#<runId>`           | `EVT#<eventId>`        |
| Step                  | `RUN#<runId>`           | `STEP#<stepId>`        |
| Hook                  | `RUN#<runId>`           | `HOOK#<hookId>`        |
| Wait                  | `RUN#<runId>`           | `WAIT#<correlationId>` |
| VM snapshot           | `RUN#<runId>`           | `SNAPSHOT`             |
| Hook resume id        | `RUN#<runId>`           | `RESUME#<resumeId>`    |
| `attr_set` id         | `RUN#<runId>`           | `ATTR#<correlationId>` |
| Start Hook decision   | `RUN#<runId>`           | `ADMISSION`            |
| Hook id → run         | `HOOKID#<hookId>`       | `HOOK`                 |
| Hook token claim      | `HOOKTOKEN#<token>`     | `TOKEN`                |
| Stream chunk          | `STREAM#<runId>#<name>` | `CHUNK#<seq>`          |
| Stream end            | `STREAM#<runId>#<name>` | `CLOSED`               |
| Stream name           | `STREAMS#<runId>`       | `NAME#<name>`          |
| Queue idempotency key | `IDEMP#<key>`           | `Q`                    |
| Last recovery         | `RECOVERY`              | `LAST`                 |

GSI1 lists every run (`RUNS`) and every hook (`HOOKS`) by id; GSI2 the runs
of one workflow (`WF#<workflowName>`); GSI3 is sparse and lists only the
runs that have not ended (`ACTIVE`): a run item carries its GSI3
attributes from creation until the write that ends the run removes them,
in the same transaction. Startup recovery reads GSI3, so it does not grow
with the number of finished runs.

Each event write is one `TransactWriteItems`: the run item's counters
(`seq`, the last event slot; `seqInBand`; `rev`, moved by every write to
the run) are updated on the condition that `rev` is what the writer read,
the events are put at the next slots (`evnt_` + the zero-padded position),
and the entities the event changes are written in the same transaction. A
writer that loses a race is cancelled and decides again on fresh reads.
Hook tokens are claimed with a conditional put on `HOOKTOKEN#<token>`; a
claim that loses learns the owner from the cancellation reasons and
answers `hook_conflict` with `conflictingRunId`.

Capabilities: `hookRetention`, `atomicStartHook`, `hookResumeDedup`,
`hookForceClaim`, `dynamicWorkflowCode` and `inBandFence` (so spec 9 runs
are possible with `WORKFLOW_SPEC_VERSION=9`). `maxConcurrency` is not
declared: SQS standard queues cannot serialize a run's orchestrator
deliveries across processes; within a process they wait for each other,
and the in-band fence keeps an overlap across processes safe.
`runs.waitForTerminalStatus` polls the run every 500 ms.

### Streams

A stream is a partition, `STREAM#<runId>#<name>`, of numbered chunks
(`CHUNK#<seq>`) and, once closed, a `CLOSED` item with the chunk count.

- **Writes.** `write` and `writeMulti` put a group of chunks (up to 98,
  under 4 MB) in one transaction, each at the next free position on the
  condition that it is free and that the stream is not closed. Positions
  are dense and unique even with several writers. A write to a closed
  stream is dropped, as world-postgres' readers ignore rows after the
  first EOF. Within one World instance, a stream's writes and closes run
  in call order.
- **Close.** Puts `CLOSED` on the condition that the next position is
  still free; closing again does nothing.
- **Reads.** `get(runId, name, startIndex?)` returns a `ReadableStream`
  that queries for new chunks every `streamPollIntervalMs` until it has
  delivered every chunk before `CLOSED`. A negative `startIndex` counts
  back from the end of the stream as it is when `get()` is called.
  Cancelling the reader aborts its query and its wait; `world.close()`
  fails the readers still open. `getChunks` pages with an opaque cursor
  (100 chunks by default, at most 1000) and sets `done` only on the page
  that reaches the end of a closed stream, as world-local does: a
  consumer paging a finished stream keeps going while `hasMore` is true.
  `getInfo` gives the last index and whether the stream is closed; `list`
  the names of a run's streams.
- `createWriteSession` is not implemented (world-postgres does not either):
  the runtime uses the stateless methods.

### S3

With a bucket, what does not fit DynamoDB or SQS goes to S3:

- **Item values.** When an item would be over 350 KB, or a transaction over
  4 MB less a 256 KB margin, its largest values (never keys, counters or
  attributes a condition reads) are written to
  `s3://<bucket>/<runId>/<kind>/<id>/<attribute>-<sha256>` (`kind` is
  `run`, `event`, `step`, `hook`, `wait`, `snapshot` or `stream`; a stream
  chunk's `<id>` is `<stream name>/<position>`), and the item keeps a
  reference `{ s3Ref, s3Encoding, s3Bytes }`. Reads resolve it
  transparently. Objects are named by their content and never change, so
  the retries of one write upload each value once and reads cache them (32
  MiB per World). A VM snapshot gets its own object on every save, since
  replacing a snapshot deletes the old object. A World without a bucket
  still reads references written by one that had it.
- **Queue messages.** A message over 256 KiB is written whole to
  `s3://<bucket>/queue/<runId>/<messageId>/<ulid>-<sha256>`; its SQS body
  carries only the ids and the reference. The object is deleted once the
  message is handled.
- **Retention.** A run's objects stay as long as its items. Objects of
  queue messages that ended in a dead-letter queue, or of writes that never
  committed, are left behind: expire `queue/` after 15 days with a
  lifecycle rule (the queues keep messages 14 days).

### Queue

- **Messages.** The SQS body holds the payload (JSON, with `Uint8Array`
  values as tagged base64), the full queue name, when it was first
  enqueued and a message id of the World's own (`msg_<ulid>`). That id is
  `meta.messageId` on every delivery; SQS's own `MessageId` changes
  whenever the World sends a message again. Messages with a `stepId` go to
  the step queue, the rest to the workflow queue.
- **Delays.** Up to 15 minutes with SQS's `DelaySeconds`. A longer delay is
  sent with 15 minutes and a `notBefore` time, and each early arrival is
  sent on with the rest of the delay; that is not counted as a delivery.
- **Retries.** `{ timeoutSeconds }` sends the same message again with that
  delay (attempts counted as in world-postgres). A failure leaves the
  message in the queue, invisible for `e^n` seconds (`n` is its receive
  count; at most an hour), until the queue's `maxReceiveCount` moves it to
  the dead-letter queue. With 73 receives a failing message is retried for
  days and the runtime's own delivery cap (49) comes first; with 5 it would
  reach the dead-letter queue after about a minute and a half.
- **Idempotency.** A send with `idempotencyKey` reserves the key with a
  conditional put on `IDEMP#<key>` (absent or expired) that records the
  message id it sends under; a later send under the key answers that id
  and sends nothing. The key is marked sent in the background, after the
  message is in SQS; `close()`, and the Lambda handler before answering,
  wait for those writes. A failed send gives the key back. A reservation
  whose sender died is taken over after 10 minutes, under the same id,
  which is well past the 130 seconds an SQS request can take, so a sender
  that is still sending is not overtaken; one that was overtaken anyway
  logs a warning, since the message may then be in the queue twice. Keys
  are kept for 24 hours (`expiresAt`, DynamoDB TTL).
- **Recovery.** `start()` sends a message for every run that has not ended
  (`reenqueueActiveRuns`), as world-postgres does, in case the one that
  would continue it was lost. With SQS the queue outlives the process, so
  a recovery is recorded on the table (`RECOVERY` / `LAST`) and a process
  starting within `recoveryIntervalSeconds` (15 minutes) of another
  process's recovery skips it. If recovery fails, `start()` rejects
  without starting the worker, gives the record back, and the next
  `start()` tries again. `world.recover()` runs it on its own.
- **Authentication.** `queueAuthMode` is `'skip'`, as in world-postgres:
  the runtime does not check requests to the flow route. IAM decides who
  can send to the queues, but anyone who can reach the route can post a
  delivery to it. Do not expose `/.well-known/workflow/v1/flow` beyond
  your network, or deliver in process with `registerHandler` and do not
  serve the route at all.
- `getDeploymentId()` answers `aws`: this World has one deployment.
  `queueBatch` uses `SendMessageBatch` (10 messages per request) and
  reports each message. `QueueOptions.region`, `retentionSeconds` and
  `deploymentId` are ignored. `experimental_hold` is ignored (SQS cannot
  hand a sender back a message it just sent).
- Under `DEBUG=workflow:*` the queue logs each delivery's start and
  outcome with its message id, run and attempt.

## Limits

- **Item size.** DynamoDB stores items of up to 400 KB; the World keeps
  every item under 350 KB (the rest is margin for keys and index
  attributes) and every transaction under 4 MB less 256 KB, by moving the
  largest values to S3. Without a bucket, a write that would not fit is
  refused with `PayloadTooLargeError` (`status: 413`, `code:
  'PAYLOAD_TOO_LARGE'`) before anything is written; its message names
  `WORKFLOW_AWS_BUCKET`.
- **Queue messages.** 256 KiB per SQS message (the `MaximumMessageSize`
  every queue accepts). Larger ones go through S3; without a bucket the
  send fails with `PayloadTooLargeError`.
- **Stream polling.** Each live reader makes one strongly consistent query
  per `streamPollIntervalMs` (half a read unit when nothing is new) for as
  long as it is open. A larger interval trades latency for cost.
  DynamoDB Streams could replace polling later.
- **Hot partitions.** GSI1 holds every run (`RUNS`) and every hook
  (`HOOKS`) in one partition each, and GSI3 every active run (`ACTIVE`).
  A single index partition takes about 1,000 writes per second, so a very
  high rate of run creations and endings throttles there first (DynamoDB
  then throttles the base table writes too). Sharding those keys is a
  possible improvement; it is not done.
- **Eventually consistent indexes.** `runs.list` and `hooks.list` without
  a `runId` read GSIs, which can miss a write from the last moments.
  Everything else is read with strongly consistent reads. Recovery
  tolerates this: a run created a moment ago has its own message in SQS.
- **Other reads.** `runs.list` filtered by status together with a workflow
  name reads the workflow's runs with a filter.
  `events.listByCorrelationId` reads the run's log with a filter, so its
  cost grows with the log.
- **Cursors** of `runs.list`, `steps.list` and `hooks.list` are opaque and
  valid only for the list that returned them; any other cursor is refused
  with a 400.
- **Delays** longer than 15 minutes cost one SQS message per 15 minutes.
- **Retries** of a contended run write give up after about 9 seconds (40
  attempts) with a 503.

## Upgrading a table without GSI3

Tables created before GSI3 existed (this repository's bootstrap before the
SQS queue) have GSI1 and GSI2 only. Until GSI3 is added, `start()` and
`runs.list` by status fail with `MissingTableIndexError` (`code:
'MISSING_TABLE_INDEX'`), which names the index and the fix.

Against an emulator, run `pnpm bootstrap:local` again: it adds GSI3, waits
for it and backfills the runs. If the wait runs out on a large table, run
it again; it picks up where it was.

On AWS, add the index once:

```bash
aws dynamodb update-table --table-name <table> \
  --attribute-definitions AttributeName=GSI3PK,AttributeType=S AttributeName=GSI3SK,AttributeType=S \
  --global-secondary-index-updates \
  '[{"Create":{"IndexName":"GSI3","KeySchema":[{"AttributeName":"GSI3PK","KeyType":"HASH"},{"AttributeName":"GSI3SK","KeyType":"RANGE"}],"Projection":{"ProjectionType":"ALL"}}]}'
```

and, once it is `ACTIVE`, backfill it: every run item (`SK = META`) whose
`status` is not `completed`, `failed` or `cancelled` gets `GSI3PK =
ACTIVE` and `GSI3SK = <runId>`. Runs created afterwards carry the
attributes from creation. `backfillActiveRunIndex(clients, tableName)` in
`scripts/bootstrap-local.ts` does exactly that (a scan and a conditional
update per run, safe to run again) with the clients you pass it.

## Troubleshooting

**`AwsWorldConfigError: Invalid AWS World configuration`.** A required
variable is missing or a value is invalid; the message lists each problem
and the variable or option to set.

**`MissingTableIndexError: table "…" has no index "GSI3"`.** The table was
created without GSI3. See [Upgrading a table](#upgrading-a-table-without-gsi3).

**`PayloadTooLargeError` (413).** A value or queue message does not fit
DynamoDB or SQS and no bucket is configured. Set `WORKFLOW_AWS_BUCKET`.

**`PayloadBucketError` (`PAYLOAD_BUCKET_NOT_FOUND`).** The configured bucket
does not exist in the region, or the name is wrong.

**`AccessDeniedException` / `AccessDenied`.** The role misses an action of
the [IAM policy](#iam), often `dynamodb:Query` on `table/<table>/index/*`
or `dynamodb:ConditionCheckItem`. An S3 `AccessDenied` on `GetObject` can
also mean the object is gone: without `s3:ListBucket`, S3 answers a
missing key with 403, not 404. A `queue/...` body expired by a lifecycle
rule, for a message that sat in a queue longer than that, looks like
this.

**Runs stay `pending` or `running` and nothing happens.**

- Worker mode: is `world.start()` called in a process that stays up? Is
  the worker delivering somewhere it can reach? Without `registerHandler`
  it POSTs to `baseUrl` (or `localhost:$PORT`); a missing base URL fails
  each delivery with "no base URL is known".
- Lambda mode: do the event source mappings point at both queues and have
  `ReportBatchItemFailures`? Is the flow route's handler registered on the
  World the handler uses? A run whose next message was lost is recovered
  only on the first event of a new process; with idle queues that may
  never come. Recover on a schedule (see
  [Lambda mode](#lambda-mode)), or call `world.recover()` once by hand
  (it does nothing if the table was recovered within
  `recoveryIntervalSeconds`).
- Look at the dead-letter queues and at `DEBUG=workflow:*` output: each
  delivery's start and outcome is logged.

**The same step runs twice at the same time.** The message's visibility
ran out while it ran: in worker mode the heartbeat failed (check
`ChangeMessageVisibility` warnings), in Lambda mode the queue's visibility
timeout is shorter than the function's timeout.

**Messages reach the dead-letter queue quickly.** The queue's
`maxReceiveCount` is low. Each failed delivery backs off `e^n` seconds; use
a high count (73).

**"Idempotent send was overtaken" warning.** A send took longer than 10
minutes between reserving its key and marking it sent, and another sender
took the key over; the message may be in the queue twice. It usually means
SQS or DynamoDB requests were failing for minutes.

**Integration tests fail on moto with a step completed but its event
missing.** moto does not isolate concurrent `TransactWriteItems`; the
World relies on that. Use LocalStack or DynamoDB to confirm (see
[Development](#development)).

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
already running:

```bash
WORKFLOW_AWS_ENDPOINT=http://localhost:4566 pnpm test:integration
```

They include the storage tests of world-postgres and world-local (ported),
the `@workflow/world` conformance suites for the in-band fence and the
atomic start Hook, the stream suites of world-postgres and world-local,
the queue and Lambda mode end to end on the emulator's SQS (Lambda events
built from real `ReceiveMessage` calls), S3 offload, and the `@workflow/world-testing` suite (all but its
queue-auth tests, which it skips for this World's `queueAuthMode:
'skip'`) at spec 8 and spec 9 against the World
bundled with tsup, each test with its own table and queues. moto does not
isolate concurrent `TransactWriteItems`, so against moto the storage
suites send their DynamoDB requests one at a time (the world-testing
servers do not). Set `WORKFLOW_AWS_EMULATOR` (`moto`, `localstack` or
`aws`) to say which emulator answers; otherwise the setup tells moto from
the rest. A failed `@workflow/world-testing` test prints its server's
delivery trace and what its table and queues held.

CI runs lint, typecheck, unit tests and the build on Node 22 and 24, the
example, and the integration suite against both LocalStack (which
isolates transactions as DynamoDB does) and moto_server 5.x.

## Status

- `0.1.0` is the first release. The API (`createWorld`,
  `createSqsLambdaHandler`, the options and variables above) may still
  change before 1.0; changes are listed in `CHANGELOG.md`.
- Tested: unit tests with mocked AWS clients, and the integration suite
  above, including `@workflow/world-testing` at spec 8 and 9, against
  moto_server. The CI job against LocalStack is set up but has not run yet,
  and nothing has been run against real AWS. The SAM template in
  `examples/hono` is checked statically and has not been deployed. Treat
  production use accordingly, and please report what you find.
- Relation to [vercel/workflow#1371](https://github.com/vercel/workflow/pull/1371):
  that pull request, by another author, adds an AWS World inside the
  Workflow SDK monorepo; it is open and not published. The maintainers
  asked for third-party Worlds to ship as their own npm packages and be
  listed in `worlds-manifest.json`, which is what this package does. It is
  a separate implementation, not a fork of that work.

## License

MIT
