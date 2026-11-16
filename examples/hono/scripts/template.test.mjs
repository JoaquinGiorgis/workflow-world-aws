// Checks template.yaml against what the World needs: no AWS account or
// SAM CLI involved, only the parsed template.
//
//   node --test scripts/template.test.mjs
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { test } from 'node:test';
import { parse } from 'yaml';

const template = parse(
  readFileSync(new URL('../template.yaml', import.meta.url), 'utf8')
);
const resources = template.Resources;
const policy = JSON.parse(
  readFileSync(new URL('../../../docs/iam-policy.json', import.meta.url), 'utf8')
);

test('the table has the keys, indexes, TTL and billing the World uses', () => {
  const { Properties: table } = resources.WorkflowTable;
  assert.equal(table.BillingMode, 'PAY_PER_REQUEST');
  assert.deepEqual(
    table.KeySchema.map((key) => [key.AttributeName, key.KeyType]),
    [
      ['PK', 'HASH'],
      ['SK', 'RANGE'],
    ]
  );
  for (const n of [1, 2, 3]) {
    const index = table.GlobalSecondaryIndexes.find(
      (each) => each.IndexName === `GSI${n}`
    );
    assert.ok(index, `GSI${n}`);
    assert.deepEqual(
      index.KeySchema.map((key) => key.AttributeName),
      [`GSI${n}PK`, `GSI${n}SK`]
    );
    assert.equal(index.Projection.ProjectionType, 'ALL');
  }
  assert.deepEqual(table.TimeToLiveSpecification, {
    AttributeName: 'expiresAt',
    Enabled: true,
  });
});

test('each queue has a dead-letter queue after 73 receives, 14 days kept', () => {
  const timeout = template.Globals.Function.Timeout;
  for (const [queue, dlq] of [
    ['WorkflowQueue', 'WorkflowDeadLetterQueue'],
    ['StepQueue', 'StepDeadLetterQueue'],
  ]) {
    const { Properties: props } = resources[queue];
    assert.equal(props.MessageRetentionPeriod, 1209600, queue);
    assert.equal(props.RedrivePolicy.maxReceiveCount, 73, queue);
    assert.deepEqual(props.RedrivePolicy.deadLetterTargetArn, {
      'Fn::GetAtt': [dlq, 'Arn'],
    });
    assert.equal(resources[dlq].Properties.MessageRetentionPeriod, 1209600);
    // At least the function's timeout; AWS recommends six times it.
    assert.ok(props.VisibilityTimeout >= 6 * timeout, queue);
  }
});

test('the queue function reports batch item failures on both queues', () => {
  const { Properties: fn } = resources.QueueFunction;
  assert.equal(fn.Handler, 'lambda.queue');
  const queues = Object.values(fn.Events).map((event) => {
    assert.equal(event.Type, 'SQS');
    assert.deepEqual(event.Properties.FunctionResponseTypes, [
      'ReportBatchItemFailures',
    ]);
    return event.Properties.Queue['Fn::GetAtt'][0];
  });
  assert.deepEqual(queues.sort(), ['StepQueue', 'WorkflowQueue']);
});

test('the functions get the documented IAM policy, no more', () => {
  const statements = resources.WorldPolicy.Properties.PolicyDocument.Statement;
  assert.deepEqual(
    statements.map((s) => [s.Sid, s.Action]),
    policy.Statement.map((s) => [s.Sid, s.Action])
  );
  for (const name of ['ApiFunction', 'QueueFunction']) {
    assert.deepEqual(resources[name].Properties.Policies, [
      { Ref: 'WorldPolicy' },
    ]);
  }
});

test('the bucket expires the bodies of queue messages', () => {
  const rules =
    resources.PayloadBucket.Properties.LifecycleConfiguration.Rules;
  assert.ok(
    rules.some(
      (rule) =>
        rule.Prefix === 'queue/' &&
        rule.Status === 'Enabled' &&
        rule.ExpirationInDays > 14
    )
  );
});
