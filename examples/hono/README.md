# Hono example

A small [Hono](https://hono.dev) app that runs a Workflow SDK workflow on
`@joaquingiorgis/world-aws`. The workflow (`workflows/greeting.ts`) has two
steps; the second one writes a greeting word by word to the run's stream.

| Route                       | What it does                                    |
| --------------------------- | ----------------------------------------------- |
| `POST /greetings/:name`     | Starts a run, answers `{ runId }`               |
| `GET /runs/:runId`          | The run's status, and its result once completed |
| `GET /runs/:runId/stream`   | The run's stream, as the words arrive           |

It runs in either queue mode:

- **Worker mode** (`src/server.ts`): one Node process serves the API and
  runs the queue worker (`world.start()`).
- **Lambda mode** (`src/lambda.ts`): an `api` function serves the API, and a
  `queue` function behind SQS event source mappings delivers the messages
  (`createSqsLambdaHandler()`). `template.yaml` deploys it with SAM, and
  `src/local-lambda.ts` runs it locally, standing in for the event source
  mappings.

`src/world.ts` creates the World with `createWorld()` (configured from the
`WORKFLOW_AWS_*` variables), passes it to the runtime with `setWorld()`, and
registers the flow route's handler in process. That also works once the app
is bundled for Lambda, where the runtime could not load the package named
by `WORKFLOW_TARGET_WORLD`. The example has its own `package.json` and
lockfile; it is not part of the published package.

## Run it locally against moto or LocalStack

Requires Node.js 22.18 or later (the app's `.ts` files run with Node's type
stripping) and pnpm. From the repository root:

```bash
pnpm install && pnpm build          # the World, linked into the example

# An emulator on port 4566: moto_server (pip install "moto[server]") ...
moto_server -p 4566
# ... or LocalStack: docker run -p 4566:4566 localstack/localstack:4

# The table, queues and bucket on the emulator
WORKFLOW_AWS_ENDPOINT=http://localhost:4566 pnpm bootstrap:local
```

The bootstrap prints the variables to use. Then, in `examples/hono`:

```bash
pnpm install
pnpm build                          # workflow build: .well-known/workflow/v1

export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test   # emulator only
export WORKFLOW_TARGET_WORLD=@joaquingiorgis/world-aws
export WORKFLOW_AWS_ENDPOINT=http://localhost:4566
export WORKFLOW_AWS_REGION=us-east-1
export WORKFLOW_AWS_TABLE=workflow
export WORKFLOW_AWS_WORKFLOW_QUEUE_URL=http://localhost:4566/123456789012/workflow
export WORKFLOW_AWS_STEP_QUEUE_URL=http://localhost:4566/123456789012/step
export WORKFLOW_AWS_BUCKET=workflow-payloads

pnpm start              # worker mode, on port 3000 (PORT to change it)
# or
pnpm start:lambda       # Lambda mode, with the local event source mappings

node scripts/try.mjs Ada    # in another terminal
```

`scripts/try.mjs` starts a greeting, prints the stream as it arrives and
then the finished run:

```
POST /greetings/Ada -> 202 wrun_01M7JW97PVFS4NX7E2Q3AT8XYQ
GET /runs/wrun_01M7JW97PVFS4NX7E2Q3AT8XYQ/stream -> Hello Ada, this came through AWS
GET /runs/wrun_01M7JW97PVFS4NX7E2Q3AT8XYQ -> {"runId":"wrun_01M7JW97PVFS4NX7E2Q3AT8XYQ","status":"completed","result":"Hello Ada, this came through AWS"}
```

The queue URLs depend on the emulator (moto uses the account
`123456789012`, LocalStack `000000000000`); copy them from the bootstrap's
output.

## Deploy it with SAM

`template.yaml` creates everything the World needs and the two functions:

- the table: `PK`/`SK`, GSI1 to GSI3, TTL on `expiresAt`, on-demand
  billing, point-in-time recovery;
- the workflow and step queues, each with a dead-letter queue after 73
  receives and 14 days of retention, and a visibility timeout of six times
  the functions' timeout (360 s for 60 s);
- the payload bucket, private and encrypted, expiring `queue/` after 15
  days (bodies of messages that never got delivered);
- `ApiFunction` behind a function URL with `AWS_IAM` auth, and
  `QueueFunction` behind both queues with `ReportBatchItemFailures`;
- `RecoverFunction`, which calls `world.recover()` every 15 minutes (the
  `RecoverySchedule` parameter; leave it empty to drop the function).
  `QueueFunction` recovers runs only on the first event of each process,
  so without it a run whose next message was lost would wait for the next
  message to arrive while the queues are idle;
- two managed policies: `WorldPolicy`, the World's
  ([`docs/iam-policy.json`](../../docs/iam-policy.json)), for
  `QueueFunction`, and `SenderPolicy`, the same without
  `sqs:ReceiveMessage`, `sqs:DeleteMessage` and
  `sqs:ChangeMessageVisibility`, for the two functions that only send.
  SAM adds the SQS permissions the event source mappings need.

```bash
pnpm install && pnpm build
sam build                 # bundles src/lambda.ts with esbuild
sam deploy --guided
```

`sam build` bundles the published package from `node_modules`; to deploy
the example as is, the root package has to be built first (it is linked
with `link:../..`), or replace the link with a version from npm.

The function URL takes SigV4-signed requests only. To call it from a
browser or from outside AWS, put API Gateway with your own auth in front
of `ApiFunction`, or switch the URL to `AuthType: NONE` and add auth to
the Hono app. The flow route (`/.well-known/workflow/v1/flow`) is not
served by the API: deliveries reach the runtime in process only.

The function URL buffers responses (its default invoke mode), so on AWS
`GET /runs/:runId/stream` answers with the whole greeting once the stream
closes, not word by word. Streaming it would take `InvokeMode:
RESPONSE_STREAM` on the URL and Hono's `streamHandle` instead of `handle`
in `src/lambda.ts`; that has not been tried.

`sam delete` deletes the table, and with it every run, and the bucket
(CloudFormation deletes a bucket only once it is empty). The template sets
no `DeletionPolicy: Retain`; add it to `WorkflowTable` and `PayloadBucket`
before keeping data you need.

What was checked without an AWS account: `pnpm test:template` reads the
template and checks the table, queues, mappings, policies, recovery
schedule and lifecycle; an esbuild bundle of `src/lambda.ts` with the
template's settings loads and exports `api` and `queue`. The template has
not been deployed, and has not gone through `sam build`: the esbuild
`Banner` that gives the ESM bundle a `require` is unverified there. If the
built functions fail at import with `require is not defined`, switch the
`BuildProperties` to `Format: cjs` (without `OutExtension`).
