---
'@joaquingiorgis/world-aws': minor
---

Implement the World's storage on DynamoDB: runs, steps, events, hooks, waits and VM snapshots live in one table, and every event is committed in one transaction against a per-run counter, so event positions are dense and unique. Includes hook token conflicts, forced takeovers, hook retention, resume deduplication, atomic start Hooks, the in-band fence for spec 9 runs, and `runs.waitForTerminalStatus`. Items over 350 KB, including a run item that an update would grow past it, fail with `PayloadTooLargeError` before anything is written, until S3 offload lands. `createWorld({ retry })` tunes the retries of a write that loses a race, and a pagination cursor passed to a list that did not mint it is refused with a 400. The queue and streams are not implemented yet.
