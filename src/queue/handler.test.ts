import { describe, expect, it, vi } from 'vitest';
import {
  createQueueHandler,
  DELIVERY_HEADERS,
  MAX_REDELIVERY_DELAY_SECONDS,
} from './handler.js';
import { serializePayload } from './transport.js';

const prefix = '__wkf_workflow_';
const messageId = 'msg_01JD0000000000000000000000';

function request(body: unknown, headers: Record<string, string> = {}): Request {
  return new Request('http://localhost/.well-known/workflow/v1/flow', {
    method: 'POST',
    headers: {
      [DELIVERY_HEADERS.queueName]: `${prefix}wf`,
      [DELIVERY_HEADERS.messageId]: messageId,
      [DELIVERY_HEADERS.attempt]: '2',
      [DELIVERY_HEADERS.deliveryCount]: '5',
      [DELIVERY_HEADERS.createdAt]: '2026-11-16T03:00:00.000Z',
      [DELIVERY_HEADERS.invocationStartedAt]: '1763262000000',
      ...headers,
    },
    body: typeof body === 'string' ? body : serializePayload(body),
  });
}

describe('createQueueHandler', () => {
  it('hands the payload and the delivery metadata to the handler', async () => {
    const handler = vi.fn(async () => undefined);
    const response = await createQueueHandler(
      prefix,
      handler
    )(request({ runId: 'wrun_A', input: new Uint8Array([1, 2]) }));

    expect(await response.json()).toEqual({ ok: true });
    expect(handler).toHaveBeenCalledWith(
      { runId: 'wrun_A', input: new Uint8Array([1, 2]) },
      {
        attempt: 2,
        deliveryCount: 5,
        createdAt: new Date('2026-11-16T03:00:00.000Z'),
        invocationStartedAt: 1763262000000,
        queueName: `${prefix}wf`,
        messageId,
      }
    );
  });

  it('answers a redelivery request, never a non-finite one', async () => {
    const answer = async (timeoutSeconds: number) =>
      (
        await createQueueHandler(prefix, async () => ({ timeoutSeconds }))(
          request({ runId: 'wrun_A' })
        )
      ).json();
    expect(await answer(0)).toEqual({ timeoutSeconds: 0 });
    expect(await answer(12.5)).toEqual({ timeoutSeconds: 12.5 });
    expect(await answer(Number.POSITIVE_INFINITY)).toEqual({
      timeoutSeconds: MAX_REDELIVERY_DELAY_SECONDS,
    });
  });

  it('returns the result of an invocation as data', async () => {
    const response = await createQueueHandler(prefix, async () => ({
      timeoutSeconds: 3,
    }))(request({ runId: 'wrun_A', invoke: true, requestId: 'r' }));
    expect(await response.json()).toEqual({ result: { timeoutSeconds: 3 } });
  });

  it('answers 500 when the handler throws, so the queue retries', async () => {
    const response = await createQueueHandler(prefix, async () => {
      throw new Error('storage is down');
    })(request({ runId: 'wrun_A' }));
    expect(response.status).toBe(500);
    expect(await response.json()).toContain('storage is down');
  });

  it('refuses a message of another prefix', async () => {
    const handler = vi.fn();
    const response = await createQueueHandler(
      '__agent_wkf_workflow_',
      handler
    )(request({ runId: 'wrun_A' }));
    expect(response.status).toBe(400);
    expect(handler).not.toHaveBeenCalled();
  });

  it('refuses a request without the delivery headers', async () => {
    const response = await createQueueHandler(
      prefix,
      vi.fn()
    )(request({ runId: 'wrun_A' }, { [DELIVERY_HEADERS.attempt]: 'x' }));
    expect(response.status).toBe(400);
  });
});
