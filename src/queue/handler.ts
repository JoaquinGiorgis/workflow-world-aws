/**
 * `createQueueHandler`: the route side of a delivery.
 *
 * The worker (`consumer.ts`) turns each SQS message into the request this
 * handler takes, with the delivery's metadata in the `x-vqs-*` headers that
 * world-local and world-postgres use, and reads the answer back: `{ ok }`
 * acknowledges, `{ timeoutSeconds }` asks for a redelivery, and an error
 * status retries. It is what world-postgres gets by delegating to
 * world-local's handler, without the Local World's storage behind it.
 */
import {
  MessageId,
  type Queue,
  type QueuePrefix,
  ValidQueueName,
} from '@workflow/world';
import { deserializePayload } from './transport.js';

export const DELIVERY_HEADERS = {
  queueName: 'x-vqs-queue-name',
  messageId: 'x-vqs-message-id',
  attempt: 'x-vqs-message-attempt',
  deliveryCount: 'x-vqs-message-delivery-count',
  createdAt: 'x-vqs-message-created-at',
  /** When the worker began the delivery (epoch ms): `invocationStartedAt`. */
  invocationStartedAt: 'x-workflow-aws-invocation-started-at',
} as const;

/**
 * The redelivery delay a non-finite `{ timeoutSeconds }` becomes: never a
 * handoff (`0`), which would not count as an attempt and could loop. Vercel
 * Queues' longest delay, as world-vercel does.
 */
export const MAX_REDELIVERY_DELAY_SECONDS = 23 * 60 * 60;

function positiveInteger(value: string | null): number | undefined {
  if (value === null) return undefined;
  const parsed = Number(value);
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : undefined;
}

export const createQueueHandler: Queue['createQueueHandler'] = (
  prefix: QueuePrefix,
  handler
) => {
  return async (req) => {
    const headers = req.headers;
    const queueName = ValidQueueName.safeParse(
      headers.get(DELIVERY_HEADERS.queueName)
    );
    const messageId = MessageId.safeParse(
      headers.get(DELIVERY_HEADERS.messageId)
    );
    const attempt = positiveInteger(headers.get(DELIVERY_HEADERS.attempt));
    if (!queueName.success || !messageId.success || attempt === undefined) {
      return Response.json(
        { error: 'Missing required headers' },
        { status: 400 }
      );
    }
    if (!queueName.data.startsWith(prefix)) {
      return Response.json(
        {
          error: 'Unhandled queue',
          detail: `This route handles queue prefix "${prefix}", not "${queueName.data}".`,
        },
        { status: 400 }
      );
    }
    const text = await req.text();
    if (text === '') {
      return Response.json({ error: 'Missing request body' }, { status: 400 });
    }

    let body: unknown;
    try {
      body = deserializePayload(text);
    } catch {
      return Response.json({ error: 'Invalid request body' }, { status: 400 });
    }
    const deliveryCount = positiveInteger(
      headers.get(DELIVERY_HEADERS.deliveryCount)
    );
    const createdAt = headers.get(DELIVERY_HEADERS.createdAt);
    const invocationStartedAt = positiveInteger(
      headers.get(DELIVERY_HEADERS.invocationStartedAt)
    );

    try {
      const result = await handler(body, {
        attempt,
        deliveryCount: deliveryCount ?? attempt,
        ...(createdAt &&
          !Number.isNaN(Date.parse(createdAt)) && {
            createdAt: new Date(createdAt),
          }),
        ...(invocationStartedAt !== undefined && { invocationStartedAt }),
        queueName: queueName.data,
        messageId: messageId.data,
      });

      if (
        typeof body === 'object' &&
        body !== null &&
        (body as { invoke?: unknown }).invoke === true
      ) {
        return Response.json({ result });
      }
      const timeoutSeconds = (result as { timeoutSeconds?: unknown } | null)
        ?.timeoutSeconds;
      if (typeof timeoutSeconds === 'number') {
        return Response.json({
          timeoutSeconds: Number.isFinite(timeoutSeconds)
            ? timeoutSeconds
            : MAX_REDELIVERY_DELAY_SECONDS,
        });
      }
      return Response.json({ ok: true });
    } catch (error) {
      return Response.json(String(error), { status: 500 });
    }
  };
};
