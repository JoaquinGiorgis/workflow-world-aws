---
'@joaquingiorgis/world-aws': minor
---

Implement the World's storage on DynamoDB: runs, steps, events, hooks, waits and VM snapshots live in one table, and every event is committed in one transaction against a per-run counter, so event positions are dense and unique. Includes hook token conflicts, forced takeovers, hook retention, resume deduplication, atomic start Hooks, the in-band fence for spec 9 runs, and `runs.waitForTerminalStatus`. Payloads over the DynamoDB item limit fail with `PayloadTooLargeError` until S3 offload lands. The queue and streams are not implemented yet.
