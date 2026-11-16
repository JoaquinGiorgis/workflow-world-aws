/**
 * Lambda mode on your machine, against moto_server or LocalStack.
 *
 * Serves the API as the `api` function would, and stands in for the two
 * SQS event source mappings: it receives batches from the workflow and
 * step queues, invokes the `queue` function's handler with an SQS event
 * built from them, and deletes the records the handler did not report in
 * `batchItemFailures`, as Lambda does. No queue worker runs.
 */
import {
  DeleteMessageBatchCommand,
  GetQueueAttributesCommand,
  type Message,
  ReceiveMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { serve } from '@hono/node-server';
import type { SqsLambdaEvent } from '@joaquingiorgis/world-aws';
import { app } from './app.ts';
import { queue } from './lambda.ts';

const region = process.env.WORKFLOW_AWS_REGION ?? 'us-east-1';
const sqs = new SQSClient({
  region,
  ...(process.env.WORKFLOW_AWS_ENDPOINT && {
    endpoint: process.env.WORKFLOW_AWS_ENDPOINT,
  }),
});
const stop = new AbortController();

function eventOf(messages: Message[], arn: string): SqsLambdaEvent {
  return {
    Records: messages.map((message) => ({
      messageId: message.MessageId ?? '',
      receiptHandle: message.ReceiptHandle ?? '',
      body: message.Body ?? '',
      attributes: { ...message.Attributes },
      messageAttributes: {},
      md5OfBody: message.MD5OfBody ?? '',
      eventSource: 'aws:sqs',
      eventSourceARN: arn,
      awsRegion: region,
    })),
  };
}

/** One event source mapping: receive, invoke, delete what succeeded. */
async function mapping(label: string, queueUrl: string): Promise<void> {
  const { Attributes } = await sqs.send(
    new GetQueueAttributesCommand({
      QueueUrl: queueUrl,
      AttributeNames: ['QueueArn'],
    })
  );
  const arn = Attributes?.QueueArn ?? '';
  while (!stop.signal.aborted) {
    let messages: Message[];
    try {
      ({ Messages: messages = [] } = await sqs.send(
        new ReceiveMessageCommand({
          QueueUrl: queueUrl,
          MaxNumberOfMessages: 10,
          WaitTimeSeconds: 2,
          // No VisibilityTimeout: the queue's own applies, as with Lambda.
          MessageSystemAttributeNames: ['All'],
        }),
        { abortSignal: stop.signal }
      ));
    } catch (error) {
      if (stop.signal.aborted) return;
      throw error;
    }
    if (messages.length === 0) continue;
    const event = eventOf(messages, arn);
    const { batchItemFailures } = await queue(event);
    const failed = new Set(batchItemFailures.map((f) => f.itemIdentifier));
    const done = event.Records.filter((r) => !failed.has(r.messageId));
    console.log(
      `[event source mapping] ${label}: ${event.Records.length} records, ${failed.size} reported as failed`
    );
    if (done.length > 0) {
      await sqs.send(
        new DeleteMessageBatchCommand({
          QueueUrl: queueUrl,
          Entries: done.map((record, i) => ({
            Id: String(i),
            ReceiptHandle: record.receiptHandle,
          })),
        })
      );
    }
  }
}

const mappings = Promise.all([
  mapping('workflow queue', process.env.WORKFLOW_AWS_WORKFLOW_QUEUE_URL ?? ''),
  mapping('step queue', process.env.WORKFLOW_AWS_STEP_QUEUE_URL ?? ''),
]);

const server = serve(
  { fetch: app.fetch, port: Number(process.env.PORT ?? 3000) },
  (info) =>
    console.log(`Lambda mode (local): listening on http://localhost:${info.port}`)
);

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    stop.abort();
    server.close();
    void mappings.finally(() => process.exit(0));
  });
}

await mappings;
