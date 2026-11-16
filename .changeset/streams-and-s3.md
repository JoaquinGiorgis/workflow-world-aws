---
"@joaquingiorgis/world-aws": minor
---

Implement streams and S3 offload. Streams live in the World's DynamoDB table as numbered chunks with a closing marker; `streams.get` polls for new chunks every `streamPollIntervalMs` (`WORKFLOW_AWS_STREAM_POLL_INTERVAL_MS`, default 250 ms), and the full `@workflow/world-testing` suite now passes. With a bucket (`bucket` / `WORKFLOW_AWS_BUCKET`), values that do not fit a DynamoDB item or transaction, and queue messages over 256 KiB, are stored in S3 and read back transparently; without one, such writes fail with `PayloadTooLargeError`, whose message now names the setting.

Also: a table created without GSI3 now fails with `MissingTableIndexError` naming the index, and `pnpm bootstrap:local` adds it to an existing table and backfills it; if startup recovery fails, `start()` rejects without leaving the queue worker running, and the next `start()` retries; the worker no longer lets a heartbeat in flight override a message's final visibility.
