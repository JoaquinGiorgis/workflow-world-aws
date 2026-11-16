---
"@joaquingiorgis/world-aws": minor
---

First release: a Workflow SDK World on AWS.

- Storage and streams on one DynamoDB table. Each event is committed in one transaction against a per-run counter, so event positions are dense and unique. Supports hook token conflicts and forced takeovers, hook retention, resume deduplication, atomic start Hooks, dynamic workflow code and the in-band fence, so spec 9 runs are available with `WORKFLOW_SPEC_VERSION=9`.
- The queue on two SQS standard queues (workflow and step messages) with dead-letter queues: stable message ids across redeliveries, delays longer than 15 minutes, `idempotencyKey` deduplication for 24 hours, `queueBatch`, and startup recovery of the runs that have not ended, at most once per `recoveryIntervalSeconds` per table.
- Worker mode: `world.start()` long-polls both queues in the app's process, with a heartbeat while a delivery runs and a graceful `close()`.
- Lambda mode: `createSqsLambdaHandler()` handles the events of an SQS event source mapping and answers `{ batchItemFailures }`, backing failed records off as the worker does. It recovers runs on the first event of each process; where the queues can be idle, call `world.recover()` on a schedule as well.
- With a bucket, values too large for DynamoDB and queue messages over 256 KiB go to S3 and are read back transparently; without one, such writes fail with `PayloadTooLargeError`.
- Configuration through `createWorld()` options or `WORKFLOW_AWS_*` variables, validated up front. ESM only (`require()` works from Node.js 22.12). The minimal IAM policy ships as `docs/iam-policy.json` (`@joaquingiorgis/world-aws/iam-policy.json`).
