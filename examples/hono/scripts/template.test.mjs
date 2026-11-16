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

/** [Sid, actions] of each statement of a managed policy of the template. */
const statementsOf = (name) =>
  resources[name].Properties.PolicyDocument.Statement.map((s) => [
    s.Sid,
    s.Action,
  ]);

/** What only a consumer of the queues does. */
const consumerActions = new Set([
  'sqs:ReceiveMessage',
  'sqs:DeleteMessage',
  'sqs:ChangeMessageVisibility',
]);

test('the queue function gets the documented IAM policy, no more', () => {
  assert.deepEqual(
    statementsOf('WorldPolicy'),
    policy.Statement.map((s) => [s.Sid, s.Action])
  );
  assert.deepEqual(resources.QueueFunction.Properties.Policies, [
    { Ref: 'WorldPolicy' },
  ]);
});

test('the functions that only send get the policy without the consumer actions', () => {
  assert.deepEqual(
    statementsOf('SenderPolicy'),
    policy.Statement.map((s) => [
      s.Sid,
      s.Action.filter((action) => !consumerActions.has(action)),
    ])
  );
  // The same resources as the World's policy.
  assert.deepEqual(
    resources.SenderPolicy.Properties.PolicyDocument.Statement.map(
      (s) => s.Resource
    ),
    resources.WorldPolicy.Properties.PolicyDocument.Statement.map(
      (s) => s.Resource
    )
  );
  for (const name of ['ApiFunction', 'RecoverFunction']) {
    assert.deepEqual(resources[name].Properties.Policies, [
      { Ref: 'SenderPolicy' },
    ]);
  }
});

test('an optional schedule recovers the runs while the queues are idle', () => {
  const { Properties: fn, Condition } = resources.RecoverFunction;
  assert.equal(fn.Handler, 'lambda.recover');
  assert.equal(Condition, 'ScheduledRecovery');
  assert.ok(template.Conditions.ScheduledRecovery);
  const [schedule] = Object.values(fn.Events);
  assert.equal(schedule.Type, 'Schedule');
  assert.deepEqual(schedule.Properties.Schedule, { Ref: 'RecoverySchedule' });
  assert.equal(template.Parameters.RecoverySchedule.Default, 'rate(15 minutes)');
});

test('the functions do not name the World package for the runtime to load', () => {
  // The bundle cannot load it by name; src/world.ts calls setWorld().
  const variables = template.Globals.Function.Environment.Variables;
  assert.equal(variables.WORKFLOW_TARGET_WORLD, undefined);
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
