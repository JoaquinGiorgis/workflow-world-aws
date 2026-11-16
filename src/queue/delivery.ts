/**
 * One delivery of a message: to the in-process handler registered for its
 * prefix (`registerHandler`), else over HTTP to the prefix's route
 * (`registerRoute`, read at each delivery, then the producer's) or the
 * Workflow SDK's flow route, on the app's base URL.
 *
 * HTTP deliveries go over Node's core HTTP client (`@workflow/world`'s
 * `node-http.js`) without deadlines, as in world-postgres: a delivery runs
 * the workflow inline and answers only when that work is done, and undici's
 * default 300 s header deadline would declare a slow, healthy delivery
 * failed and retry it while it still runs. A crashed worker is covered by
 * the message's visibility timeout instead.
 */
import {
  type MessageId,
  parseQueueName,
  type ValidQueueName,
  WORKFLOW_FLOW_ROUTE,
} from '@workflow/world';
import {
  type NodeHttpAgents,
  nodeHttpFetch,
} from '@workflow/world/node-http.js';
import { DELIVERY_HEADERS } from './handler.js';
import type { QueueMessage } from './message.js';
import type { RequestHandler } from './state.js';
import { serializePayload } from './transport.js';

export type DeliveryResult =
  | { type: 'completed' }
  | { type: 'reschedule'; timeoutSeconds: number }
  | { type: 'error'; status: number; text: string };

export interface DeliveryTarget {
  handlers: ReadonlyMap<string, RequestHandler>;
  routes: ReadonlyMap<string, string>;
  /** The configured base URL, if any; see {@link resolveBaseUrl}. */
  baseUrl?: string;
  agents: NodeHttpAgents;
}

export interface Delivery {
  message: QueueMessage;
  attempt: number;
  deliveryCount: number;
  invocationStartedAt: number;
  signal?: AbortSignal;
}

/**
 * Where HTTP deliveries go: the configured base URL, else the SDK's
 * `WORKFLOW_LOCAL_BASE_URL`, else `http://localhost:$PORT`. Read at each
 * delivery, because an app usually learns its port after creating the
 * World.
 */
export function resolveBaseUrl(
  configured: string | undefined,
  env: Record<string, string | undefined> = process.env
): string | undefined {
  if (configured) return configured;
  if (env.WORKFLOW_LOCAL_BASE_URL) return env.WORKFLOW_LOCAL_BASE_URL;
  if (env.PORT) return `http://localhost:${env.PORT}`;
  return undefined;
}

function joinUrl(baseUrl: string, path: string): string {
  return `${baseUrl.replace(/\/+$/, '')}${path}`;
}

export async function deliver(
  target: DeliveryTarget,
  { message, attempt, deliveryCount, invocationStartedAt, signal }: Delivery
): Promise<DeliveryResult> {
  const { prefix } = parseQueueName(message.queueName as ValidQueueName);
  const headers = new Headers(message.headers);
  headers.set('content-type', 'application/json');
  headers.set(DELIVERY_HEADERS.queueName, message.queueName);
  headers.set(DELIVERY_HEADERS.messageId, message.messageId as MessageId);
  headers.set(DELIVERY_HEADERS.attempt, String(attempt));
  headers.set(DELIVERY_HEADERS.deliveryCount, String(deliveryCount));
  headers.set(DELIVERY_HEADERS.createdAt, message.createdAt);
  headers.set(
    DELIVERY_HEADERS.invocationStartedAt,
    String(invocationStartedAt)
  );
  const body = serializePayload(message.payload);
  const path =
    target.routes.get(prefix) ?? message.route ?? WORKFLOW_FLOW_ROUTE;

  let response: Response;
  const handler = target.handlers.get(prefix);
  if (handler) {
    response = await handler(
      new Request(joinUrl('http://localhost', path), {
        method: 'POST',
        headers,
        body,
      })
    );
  } else {
    const baseUrl = resolveBaseUrl(target.baseUrl);
    if (!baseUrl) {
      throw new Error(
        'Cannot deliver the queue message: no handler is registered for its prefix and no base URL is known (set `baseUrl`, WORKFLOW_AWS_BASE_URL or PORT)'
      );
    }
    response = await nodeHttpFetch(joinUrl(baseUrl, path), {
      method: 'POST',
      headers,
      body,
      agents: target.agents,
      ...(signal && { signal }),
    });
  }

  const text = await response.text();
  if (!response.ok) {
    return { type: 'error', status: response.status, text };
  }
  try {
    const { timeoutSeconds } = JSON.parse(text) as { timeoutSeconds?: unknown };
    if (
      typeof timeoutSeconds === 'number' &&
      Number.isFinite(timeoutSeconds) &&
      timeoutSeconds >= 0
    ) {
      return { type: 'reschedule', timeoutSeconds };
    }
  } catch {}
  return { type: 'completed' };
}
