/**
 * Process-wide queue state, on `globalThis` under a versioned
 * `Symbol.for()` key rather than at module scope, so every copy of this
 * package a bundler puts in one process shares it (see "Process-wide state"
 * in the Building a World guide).
 *
 * Per set of SQS queues (endpoint, workflow queue, step queue) it holds the
 * in-process handlers and routes registered for each prefix, the gate that
 * serializes a spec 9 run's orchestrator deliveries in this process, and the
 * one worker that consumes those queues here: a second World over the same
 * queues in the same process does not start a second set of pollers.
 */
import { monotonicFactory } from 'ulid';

export type RequestHandler = (req: Request) => Promise<Response>;

/** What one World's worker exposes to the others in the process. */
export interface RunningConsumer {
  close(): Promise<void>;
}

export interface SharedQueues {
  handlers: Map<string, RequestHandler>;
  routes: Map<string, string>;
  runGate: RunGate;
  consumer?: RunningConsumer;
}

interface QueueProcessState {
  queues: Map<string, SharedQueues>;
  /** Message id generator; its sequence must not fork within a process. */
  nextUlid: () => string;
}

const STATE_KEY = Symbol.for('@joaquingiorgis/world-aws//queue/v1');

function processState(): QueueProcessState {
  const store = globalThis as typeof globalThis &
    Record<symbol, QueueProcessState | undefined>;
  store[STATE_KEY] ??= { queues: new Map(), nextUlid: monotonicFactory() };
  return store[STATE_KEY];
}

/** The shared state of the queues identified by `key`. */
export function sharedQueues(key: string): SharedQueues {
  const { queues } = processState();
  let shared = queues.get(key);
  if (!shared) {
    shared = { handlers: new Map(), routes: new Map(), runGate: new RunGate() };
    queues.set(key, shared);
  }
  return shared;
}

export function nextUlid(): string {
  return processState().nextUlid();
}

/**
 * At most one holder per key at a time; the others wait in arrival order.
 * Used per run, for a spec 9 run's orchestrator deliveries.
 */
export class RunGate {
  private readonly tails = new Map<string, Promise<void>>();

  async acquire(key: string): Promise<() => void> {
    const previous = this.tails.get(key) ?? Promise.resolve();
    let release!: () => void;
    const mine = new Promise<void>((resolve) => {
      release = resolve;
    });
    const tail = previous.then(() => mine);
    this.tails.set(key, tail);
    await previous;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      release();
      if (this.tails.get(key) === tail) this.tails.delete(key);
    };
  }

  /** Whether `key` has a holder now (an `acquire` of it would wait). */
  isHeld(key: string): boolean {
    return this.tails.has(key);
  }

  /** Keys with a holder or a waiter; for tests. */
  get size(): number {
    return this.tails.size;
  }
}
