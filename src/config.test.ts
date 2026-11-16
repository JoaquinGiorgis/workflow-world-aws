import { describe, expect, it } from 'vitest';
import { AwsWorldConfigError, DEFAULTS, resolveConfig } from './config.js';

const validEnv = {
  WORKFLOW_AWS_REGION: 'us-east-1',
  WORKFLOW_AWS_TABLE: 'workflow',
  WORKFLOW_AWS_WORKFLOW_QUEUE_URL:
    'https://sqs.us-east-1.amazonaws.com/000000000000/workflow',
  WORKFLOW_AWS_STEP_QUEUE_URL:
    'https://sqs.us-east-1.amazonaws.com/000000000000/step',
};

function problemsOf(fn: () => unknown): readonly string[] {
  try {
    fn();
  } catch (error) {
    expect(error).toBeInstanceOf(AwsWorldConfigError);
    return (error as AwsWorldConfigError).problems;
  }
  throw new Error('expected resolveConfig to throw');
}

describe('resolveConfig', () => {
  it('reads the required values from the environment', () => {
    expect(resolveConfig(validEnv)).toEqual({
      region: 'us-east-1',
      tableName: 'workflow',
      workflowQueueUrl: validEnv.WORKFLOW_AWS_WORKFLOW_QUEUE_URL,
      stepQueueUrl: validEnv.WORKFLOW_AWS_STEP_QUEUE_URL,
      ...DEFAULTS,
    });
  });

  it('reads the optional bucket and endpoint', () => {
    const config = resolveConfig({
      ...validEnv,
      WORKFLOW_AWS_BUCKET: 'workflow-payloads',
      WORKFLOW_AWS_ENDPOINT: 'http://localhost:4566',
    });
    expect(config.bucket).toBe('workflow-payloads');
    expect(config.endpoint).toBe('http://localhost:4566');
  });

  it('leaves bucket and endpoint unset when absent or empty', () => {
    const config = resolveConfig({ ...validEnv, WORKFLOW_AWS_BUCKET: '  ' });
    expect(config).not.toHaveProperty('bucket');
    expect(config).not.toHaveProperty('endpoint');
  });

  it('prefers explicit options over the environment', () => {
    const config = resolveConfig(validEnv, {
      region: 'eu-west-1',
      tableName: 'other',
      concurrency: 3,
    });
    expect(config.region).toBe('eu-west-1');
    expect(config.tableName).toBe('other');
    expect(config.concurrency).toBe(3);
  });

  it('works from options alone', () => {
    const config = resolveConfig(
      {},
      {
        region: 'us-west-2',
        tableName: 'wf-table',
        workflowQueueUrl: 'http://localhost:4566/000000000000/a',
        stepQueueUrl: 'http://localhost:4566/000000000000/b',
      }
    );
    expect(config.region).toBe('us-west-2');
  });

  it('reports every missing required value with its env var', () => {
    const problems = problemsOf(() => resolveConfig({}));
    expect(problems).toHaveLength(4);
    expect(problems.join('\n')).toContain('WORKFLOW_AWS_REGION');
    expect(problems.join('\n')).toContain('WORKFLOW_AWS_TABLE');
    expect(problems.join('\n')).toContain('WORKFLOW_AWS_WORKFLOW_QUEUE_URL');
    expect(problems.join('\n')).toContain('WORKFLOW_AWS_STEP_QUEUE_URL');
  });

  it('puts every problem in the error message', () => {
    expect(() => resolveConfig({})).toThrowError(
      /Invalid AWS World configuration:\n {2}- AWS region is required/
    );
  });

  it.each([
    ['WORKFLOW_AWS_REGION', 'useast1', 'not an AWS region'],
    ['WORKFLOW_AWS_TABLE', 'a', 'must be 3-255 characters'],
    ['WORKFLOW_AWS_TABLE', 'bad table', 'must be 3-255 characters'],
    ['WORKFLOW_AWS_WORKFLOW_QUEUE_URL', 'not a url', 'is not a valid URL'],
    ['WORKFLOW_AWS_STEP_QUEUE_URL', 'ftp://host/q', 'must use http or https'],
    ['WORKFLOW_AWS_BUCKET', 'Bad_Bucket', 'not a valid S3 bucket name'],
    ['WORKFLOW_AWS_ENDPOINT', 'localhost:4566', 'must use http or https'],
  ])('rejects %s=%j', (name, value, message) => {
    const problems = problemsOf(() =>
      resolveConfig({ ...validEnv, [name]: value })
    );
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain(name);
    expect(problems[0]).toContain(message);
  });

  it('rejects the same queue for workflows and steps', () => {
    const problems = problemsOf(() =>
      resolveConfig({
        ...validEnv,
        WORKFLOW_AWS_STEP_QUEUE_URL: validEnv.WORKFLOW_AWS_WORKFLOW_QUEUE_URL,
      })
    );
    expect(problems).toEqual([
      'WORKFLOW_AWS_WORKFLOW_QUEUE_URL and WORKFLOW_AWS_STEP_QUEUE_URL must be different queues',
    ]);
  });

  it.each([
    ['concurrency', 0],
    ['concurrency', 1.5],
    ['visibilityTimeoutSeconds', 43_201],
    ['shutdownGraceMs', -1],
    ['streamPollIntervalMs', 0],
    ['recoveryIntervalSeconds', -1],
  ] as const)('rejects %s=%d', (key, value) => {
    const problems = problemsOf(() =>
      resolveConfig(validEnv, { [key]: value })
    );
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain(`\`${key}\` must be an integer`);
  });

  it('reads the numeric options from WORKFLOW_AWS_* variables', () => {
    const config = resolveConfig({
      ...validEnv,
      WORKFLOW_AWS_CONCURRENCY: '4',
      WORKFLOW_AWS_VISIBILITY_TIMEOUT_SECONDS: '30',
      WORKFLOW_AWS_SHUTDOWN_GRACE_MS: '0',
      WORKFLOW_AWS_POLL_WAIT_SECONDS: '1',
      WORKFLOW_AWS_STREAM_POLL_INTERVAL_MS: ' 100 ',
      WORKFLOW_AWS_RECOVERY_INTERVAL_SECONDS: '0',
    });
    expect(config).toMatchObject({
      concurrency: 4,
      visibilityTimeoutSeconds: 30,
      shutdownGraceMs: 0,
      pollWaitSeconds: 1,
      streamPollIntervalMs: 100,
      recoveryIntervalSeconds: 0,
    });
  });

  it('recovers a table at most every 15 minutes by default', () => {
    expect(resolveConfig(validEnv).recoveryIntervalSeconds).toBe(900);
  });

  it('prefers an explicit numeric option over its variable', () => {
    const config = resolveConfig(
      { ...validEnv, WORKFLOW_AWS_POLL_WAIT_SECONDS: '1' },
      { pollWaitSeconds: 5 }
    );
    expect(config.pollWaitSeconds).toBe(5);
  });

  it('long polls for 20 seconds by default', () => {
    expect(resolveConfig(validEnv).pollWaitSeconds).toBe(20);
  });

  it.each([
    ['WORKFLOW_AWS_CONCURRENCY', 'ten'],
    ['WORKFLOW_AWS_POLL_WAIT_SECONDS', '21'],
    ['WORKFLOW_AWS_VISIBILITY_TIMEOUT_SECONDS', '0'],
    ['WORKFLOW_AWS_SHUTDOWN_GRACE_MS', '1.5'],
  ])('rejects %s=%j', (name, value) => {
    const problems = problemsOf(() =>
      resolveConfig({ ...validEnv, [name]: value })
    );
    expect(problems).toHaveLength(1);
    expect(problems[0]).toContain(`${name} must be an integer`);
    expect(problems[0]).toContain(JSON.stringify(value));
  });

  it.each(['', '   ', '60', '0.5'])(
    'accepts WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS=%j',
    (value) => {
      expect(() =>
        resolveConfig({
          ...validEnv,
          WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS: value,
        })
      ).not.toThrow();
    }
  );

  it.each(['0', '-1', 'thirty', 'Infinity'])(
    'rejects WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS=%j',
    (value) => {
      const problems = problemsOf(() =>
        resolveConfig({
          ...validEnv,
          WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS: value,
        })
      );
      expect(problems).toEqual([
        `WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS must be a positive number, got ${JSON.stringify(value)}`,
      ]);
    }
  );

  it('reads the delivery base URL and validates it', () => {
    expect(
      resolveConfig({
        ...validEnv,
        WORKFLOW_AWS_BASE_URL: 'http://localhost:3000',
      }).baseUrl
    ).toBe('http://localhost:3000');
    expect(resolveConfig(validEnv)).not.toHaveProperty('baseUrl');
    expect(
      problemsOf(() =>
        resolveConfig({ ...validEnv, WORKFLOW_AWS_BASE_URL: 'localhost' })
      )[0]
    ).toContain('WORKFLOW_AWS_BASE_URL');
  });
});
