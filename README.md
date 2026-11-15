# @joaquingiorgis/world-aws

A [Workflow SDK](https://github.com/vercel/workflow) World backed by AWS:
DynamoDB for storage, SQS for the queue and S3 for large payloads.

> **Status: work in progress, M0.** This is the project scaffold only.
> `createWorld()` returns a World whose storage, queue and stream methods
> throw `not implemented yet (M1)`. Do not use it yet. The package name is
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

To create the table, queues and bucket by hand against an emulator:

```bash
WORKFLOW_AWS_ENDPOINT=http://localhost:4566 pnpm bootstrap:local
```

## Roadmap

- M0: scaffold, config, key layout, local bootstrap, CI
- M1: storage (runs, events, steps, hooks, waits)
- M2: SQS queue (worker mode, long delays, idempotency, DLQ)
- M3: streams, S3 offload, full `@workflow/world-testing` suite
- M4: docs, IAM policy, example, Lambda mode, first release

## License

MIT
