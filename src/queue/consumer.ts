/**
 * The worker: polls the workflow and step queues and runs each message.
 *
 * Per queue, `ceil(concurrency / 10)` loops long-poll `ReceiveMessage` and
 * share `concurrency` slots, so at most that many messages of the queue run
 * at once. For each message:
 *
 * - A message whose `notBefore` is still ahead (a delay over SQS's 15
 *   minutes) is sent again with the rest of the delay, and deleted. It is
 *   not a delivery.
 * - Otherwise it is delivered (`delivery.ts`) while a heartbeat keeps it
 *   invisible (`ChangeMessageVisibility`). `attempt` and `deliveryCount` are
 *   SQS's `ApproximateReceiveCount` plus the offsets in the body.
 * - Acknowledged: deleted.
 * - `{ timeoutSeconds: n }`: the same message (same id, next delivery
 *   count) is sent again with an `n` second delay, then this one is deleted,
 *   counting attempts as world-postgres does (a spec 9 handoff keeps
 *   `attempt`, a spec 9 orchestrator's timed redelivery counts one, a step's
 *   in-place retry starts it over). Sending a new message rather than
 *   extending the visibility keeps these redeliveries out of SQS's receive
 *   count, which is what moves a message to the dead-letter queue.
 * - Failed (an error status or a thrown error): left in the queue, invisible
 *   for an exponential backoff (`e^receiveCount` seconds, capped at an
 *   hour), and received again; after the queue's `maxReceiveCount` SQS moves
 *   it to the dead-letter queue.
 *
 * A spec 9 run's orchestrator deliveries wait for each other in this
 * process (`RunGate`), as in world-local. Across processes nothing
 * serializes them, which is why the World does not declare `maxConcurrency`;
 * the in-band fence keeps an overlap safe.
 *
 * `close()` stops receiving, then waits up to `shutdownGraceMs` for the
 * running deliveries. A message whose delivery has not started yet is made
 * visible again at once. One still running at the deadline is abandoned:
 * its heartbeat stops, nothing is acknowledged, and SQS delivers it again
 * once its visibility timeout runs out.
 */
import {
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand,
  type Message,
  ReceiveMessageCommand,
  SendMessageCommand,
  type SQSClient,
} from '@aws-sdk/client-sqs';
import {
  isSingleOrchestratorMessage,
  orchestratorRunIdOf,
  parseQueueName,
  singleOrchestratorRunIdOf,
  type ValidQueueName,
} from '@workflow/world';
import { sleep } from '../storage/table.js';
import type { Delivery, DeliveryResult } from './delivery.js';
import { decodeMessage, encodeMessage, type QueueMessage } from './message.js';
import type { RunGate } from './state.js';

/** SQS's largest per-message delay. */
export const MAX_SQS_DELAY_SECONDS = 900;
/** SQS's largest batch of received messages. */
const MAX_RECEIVE_BATCH = 10;
/** A hop delivered at most this early runs: SQS delays are whole seconds. */
const EARLY_DELIVERY_TOLERANCE_MS = 1_000;
/**
 * The pause after an empty receive without long polling
 * (`pollWaitSeconds: 0`), so an idle worker does not spin on SQS.
 */
const EMPTY_POLL_PAUSE_MS = 100;
/** The longest backoff of a failed delivery. */
export const MAX_RETRY_DELAY_SECONDS = 3_600;

/**
 * The backoff of a failed delivery, by SQS receive count: `e^n` seconds
 * (Graphile Worker's, which world-postgres uses), capped at an hour.
 */
export function defaultRetryDelaySeconds(receiveCount: number): number {
  return Math.min(
    MAX_RETRY_DELAY_SECONDS,
    Math.ceil(Math.exp(Math.min(Math.max(receiveCount, 1), 10)))
  );
}

/**
 * How a delay is sent: SQS delays up to 15 minutes, and a longer one rides
 * as `notBefore` on a message delayed 15 minutes, sent on by each early
 * delivery until it is due.
 */
export function scheduleOf(
  delaySeconds: number | undefined,
  now = Date.now()
): { delaySeconds: number; notBefore?: number } {
  if (delaySeconds === undefined || !(delaySeconds > 0)) {
    return { delaySeconds: 0 };
  }
  if (delaySeconds <= MAX_SQS_DELAY_SECONDS) {
    return { delaySeconds: Math.ceil(delaySeconds) };
  }
  return {
    delaySeconds: MAX_SQS_DELAY_SECONDS,
    notBefore: now + Math.round(delaySeconds * 1000),
  };
}

/** Sends `message` to `queueUrl` after `delaySeconds`, however long. */
export async function sendQueueMessage(
  sqs: SQSClient,
  queueUrl: string,
  message: QueueMessage,
  delaySeconds: number | undefined
): Promise<void> {
  const { notBefore: _previous, ...rest } = message;
  const schedule = scheduleOf(delaySeconds);
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: encodeMessage({
        ...rest,
        ...(schedule.notBefore !== undefined && {
          notBefore: schedule.notBefore,
        }),
      }),
      ...(schedule.delaySeconds > 0 && { DelaySeconds: schedule.delaySeconds }),
    })
  );
}

export interface ConsumerOptions {
  sqs: SQSClient;
  queueUrls: readonly string[];
  concurrency: number;
  visibilityTimeoutSeconds: number;
  pollWaitSeconds: number;
  shutdownGraceMs: number;
  runGate: RunGate;
  deliver: (delivery: Delivery) => Promise<DeliveryResult>;
  /** Backoff of a failed delivery; {@link defaultRetryDelaySeconds}. */
  retryDelaySeconds?: (receiveCount: number) => number;
  /** How often a running delivery renews its visibility. */
  heartbeatIntervalMs?: number;
  log?: (message: string, detail?: Record<string, unknown>) => void;
}

/** A counting semaphore whose waiters can be woken to give up. */
class Slots {
  private waiters: (() => void)[] = [];

  constructor(private available: number) {}

  /** Takes between 1 and `max` slots, or 0 once `stopped()` is true. */
  async take(max: number, stopped: () => boolean): Promise<number> {
    while (this.available === 0 && !stopped()) {
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    }
    if (stopped()) return 0;
    const taken = Math.min(max, this.available);
    this.available -= taken;
    return taken;
  }

  give(count: number): void {
    if (count <= 0) return;
    this.available += count;
    this.wakeAll();
  }

  wakeAll(): void {
    const waiters = this.waiters;
    this.waiters = [];
    for (const wake of waiters) wake();
  }
}

export class Consumer {
  /** Stops the receive loops. */
  private readonly abort = new AbortController();
  /** Aborts HTTP deliveries still running when the grace period ends. */
  private readonly abandon = new AbortController();
  private readonly loops: Promise<void>[] = [];
  private readonly running = new Set<Promise<void>>();
  private readonly slots: Slots[] = [];
  private readonly heartbeats = new Set<() => void>();
  private stopping = false;
  /** Past the grace period: no more SQS calls for running deliveries. */
  private abandoned = false;
  private closing: Promise<void> | undefined;

  constructor(private readonly options: ConsumerOptions) {}

  start(): void {
    const { concurrency, queueUrls } = this.options;
    const pollers = Math.max(1, Math.ceil(concurrency / MAX_RECEIVE_BATCH));
    for (const queueUrl of queueUrls) {
      const slots = new Slots(concurrency);
      this.slots.push(slots);
      for (let i = 0; i < pollers; i++) {
        this.loops.push(this.poll(queueUrl, slots));
      }
    }
  }

  /** Idempotent; see the module comment. */
  close(): Promise<void> {
    this.closing ??= (async () => {
      this.stopping = true;
      this.abort.abort();
      for (const slots of this.slots) slots.wakeAll();
      await Promise.allSettled(this.loops);
      const running = Promise.allSettled([...this.running]);
      const grace = this.options.shutdownGraceMs;
      let timer: ReturnType<typeof setTimeout> | undefined;
      const deadline = new Promise<'deadline'>((resolve) => {
        timer = setTimeout(() => resolve('deadline'), grace);
      });
      const outcome = await Promise.race([running, deadline]);
      clearTimeout(timer);
      this.abandoned = true;
      this.abandon.abort();
      for (const stop of this.heartbeats) stop();
      this.heartbeats.clear();
      if (outcome === 'deadline' && this.running.size > 0) {
        this.log(
          `close() gave up on ${this.running.size} running deliveries after ${grace} ms; SQS delivers them again after their visibility timeout`
        );
      }
    })();
    return this.closing;
  }

  /** Deliveries in progress (received and not yet settled). */
  get inFlight(): number {
    return this.running.size;
  }

  private log(message: string, detail?: Record<string, unknown>): void {
    if (this.options.log) this.options.log(message, detail);
    else console.warn(`[world-aws] ${message}`, detail ?? '');
  }

  private async poll(queueUrl: string, slots: Slots): Promise<void> {
    const { sqs, pollWaitSeconds, visibilityTimeoutSeconds } = this.options;
    let failures = 0;
    while (!this.stopping) {
      const taken = await slots.take(MAX_RECEIVE_BATCH, () => this.stopping);
      if (taken === 0) break;
      let messages: Message[];
      try {
        const result = await sqs.send(
          new ReceiveMessageCommand({
            QueueUrl: queueUrl,
            MaxNumberOfMessages: taken,
            WaitTimeSeconds: pollWaitSeconds,
            VisibilityTimeout: visibilityTimeoutSeconds,
            MessageSystemAttributeNames: ['ApproximateReceiveCount'],
          }),
          { abortSignal: this.abort.signal }
        );
        messages = result.Messages ?? [];
        failures = 0;
      } catch (error) {
        slots.give(taken);
        if (this.stopping) break;
        failures++;
        this.log('ReceiveMessage failed; polling again after a backoff', {
          queueUrl,
          error: describe(error),
        });
        await sleep(
          Math.min(30_000, 250 * 2 ** Math.min(failures, 7)),
          this.abort.signal
        );
        continue;
      }
      slots.give(taken - messages.length);
      if (messages.length === 0 && pollWaitSeconds === 0) {
        await sleep(EMPTY_POLL_PAUSE_MS, this.abort.signal);
      }
      for (const message of messages) {
        const task = this.handle(queueUrl, message)
          .catch((error: unknown) => {
            this.log('A queue message could not be handled', {
              queueUrl,
              error: describe(error),
            });
          })
          .finally(() => {
            this.running.delete(task);
            slots.give(1);
          });
        this.running.add(task);
      }
    }
  }

  private async handle(queueUrl: string, received: Message): Promise<void> {
    const receiveCount = Math.max(
      1,
      Number(received.Attributes?.ApproximateReceiveCount ?? 1) || 1
    );
    const receipt = received.ReceiptHandle ?? '';
    let message: QueueMessage;
    try {
      message = decodeMessage(received.Body);
      parseQueueName(message.queueName as ValidQueueName);
    } catch (error) {
      // Not something this World sent: back off, and let the redrive policy
      // move it to the dead-letter queue.
      this.log('Received a message that is not a World message', {
        queueUrl,
        messageId: received.MessageId,
        error: describe(error),
      });
      await this.retryLater(queueUrl, receipt, receiveCount);
      return;
    }

    if (
      message.notBefore !== undefined &&
      message.notBefore - Date.now() > EARLY_DELIVERY_TOLERANCE_MS
    ) {
      await sendQueueMessage(
        this.options.sqs,
        queueUrl,
        message,
        (message.notBefore - Date.now()) / 1000
      );
      await this.acknowledge(queueUrl, receipt);
      return;
    }

    const stopHeartbeat = this.heartbeat(queueUrl, receipt);
    const gateKey = runGateKey(message);
    const release = gateKey
      ? await this.options.runGate.acquire(gateKey)
      : undefined;
    try {
      if (this.stopping) {
        // Received but not started: give it back at once.
        await this.changeVisibility(queueUrl, receipt, 0);
        return;
      }
      const attempt = receiveCount + (message.attemptOffset ?? 0);
      const deliveryCount = receiveCount + (message.deliveryOffset ?? 0);
      let result: DeliveryResult;
      try {
        result = await this.options.deliver({
          message,
          attempt,
          deliveryCount,
          invocationStartedAt: Date.now(),
          signal: this.abandon.signal,
        });
      } catch (error) {
        result = { type: 'error', status: 0, text: describe(error) };
      }
      if (this.abandoned) return;

      switch (result.type) {
        case 'completed':
          await this.acknowledge(queueUrl, receipt);
          return;
        case 'reschedule':
          await sendQueueMessage(
            this.options.sqs,
            queueUrl,
            followUp(message, attempt, deliveryCount, result.timeoutSeconds),
            result.timeoutSeconds
          );
          await this.acknowledge(queueUrl, receipt);
          return;
        case 'error':
          this.log('Queue delivery failed; retrying after a backoff', {
            queueName: message.queueName,
            messageId: message.messageId,
            attempt,
            status: result.status,
            error: result.text.slice(0, 1_000),
          });
          await this.retryLater(queueUrl, receipt, receiveCount);
          return;
      }
    } finally {
      stopHeartbeat();
      release?.();
    }
  }

  /** Renews the message's visibility until the returned stop is called. */
  private heartbeat(queueUrl: string, receipt: string): () => void {
    const { visibilityTimeoutSeconds, heartbeatIntervalMs } = this.options;
    const interval =
      heartbeatIntervalMs ??
      Math.max(250, (visibilityTimeoutSeconds * 1000) / 3);
    const timer = setInterval(() => {
      this.changeVisibility(queueUrl, receipt, visibilityTimeoutSeconds).catch(
        () => {}
      );
    }, interval);
    const stop = () => {
      clearInterval(timer);
      this.heartbeats.delete(stop);
    };
    this.heartbeats.add(stop);
    return stop;
  }

  private async retryLater(
    queueUrl: string,
    receipt: string,
    receiveCount: number
  ): Promise<void> {
    const delay = (this.options.retryDelaySeconds ?? defaultRetryDelaySeconds)(
      receiveCount
    );
    await this.changeVisibility(queueUrl, receipt, delay);
  }

  private async changeVisibility(
    queueUrl: string,
    receipt: string,
    seconds: number
  ): Promise<void> {
    if (this.abandoned) return;
    try {
      await this.options.sqs.send(
        new ChangeMessageVisibilityCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: receipt,
          VisibilityTimeout: Math.max(0, Math.min(43_200, Math.ceil(seconds))),
        })
      );
    } catch (error) {
      // The message is redelivered when its current visibility runs out.
      this.log('ChangeMessageVisibility failed', {
        queueUrl,
        error: describe(error),
      });
    }
  }

  private async acknowledge(queueUrl: string, receipt: string): Promise<void> {
    try {
      await this.options.sqs.send(
        new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: receipt })
      );
    } catch (error) {
      // Delivered again later: the queue is at-least-once.
      this.log('DeleteMessage failed; the message will be delivered again', {
        queueUrl,
        error: describe(error),
      });
    }
  }
}

/**
 * The message that continues `message` after a `{ timeoutSeconds }` answer,
 * with the attempt and delivery offsets world-postgres gives its follow-up
 * job.
 */
export function followUp(
  message: QueueMessage,
  attempt: number,
  deliveryCount: number,
  timeoutSeconds: number
): QueueMessage {
  const singleOrchestrator = isSingleOrchestratorMessage(message.payload);
  // A spec 9 handoff: the next delivery continues this one and keeps its
  // attempt.
  const handoff = timeoutSeconds === 0 && singleOrchestrator;
  // A spec 9 orchestrator's timed redelivery counts as an attempt, so a run
  // refused on every pass reaches the delivery cap.
  const countedRedelivery =
    !handoff &&
    singleOrchestrator &&
    orchestratorRunIdOf(message.payload) !== undefined;
  const attemptOffset = handoff
    ? attempt - 1
    : countedRedelivery
      ? attempt
      : message.attemptOffset;
  const { notBefore: _notBefore, attemptOffset: _a, ...rest } = message;
  return {
    ...rest,
    ...(attemptOffset !== undefined && { attemptOffset }),
    deliveryOffset: deliveryCount,
  };
}

/** The in-process gate key of a spec 9 orchestrator delivery. */
function runGateKey(message: QueueMessage): string | undefined {
  const runId = singleOrchestratorRunIdOf(message.payload);
  if (runId === undefined) return undefined;
  const { prefix } = parseQueueName(message.queueName as ValidQueueName);
  return `${prefix}${runId}`;
}

function describe(error: unknown): string {
  return error instanceof Error
    ? `${error.name}: ${error.message}`
    : String(error);
}
