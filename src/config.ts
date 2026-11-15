/**
 * Configuration for the AWS World.
 *
 * Every field can be passed to `createWorld()` directly. Fields left out fall
 * back to the `WORKFLOW_AWS_*` environment variables. AWS credentials are not
 * part of the config: the SDK's default provider chain resolves them.
 */
export interface AwsWorldConfig {
  /** AWS region, for example `us-east-1`. Env: `WORKFLOW_AWS_REGION`. */
  region: string;
  /** DynamoDB table holding every entity. Env: `WORKFLOW_AWS_TABLE`. */
  tableName: string;
  /** SQS queue for workflow messages. Env: `WORKFLOW_AWS_WORKFLOW_QUEUE_URL`. */
  workflowQueueUrl: string;
  /** SQS queue for step messages. Env: `WORKFLOW_AWS_STEP_QUEUE_URL`. */
  stepQueueUrl: string;
  /**
   * S3 bucket for payloads over the DynamoDB item limit. Optional: without
   * it, oversized payloads fail with a clear error. Env: `WORKFLOW_AWS_BUCKET`.
   */
  bucket?: string;
  /**
   * Endpoint override for every client, for LocalStack or moto (for example
   * `http://localhost:4566`). Env: `WORKFLOW_AWS_ENDPOINT`.
   */
  endpoint?: string;
  /** Concurrent queue handlers per World in worker mode. */
  concurrency: number;
  /** SQS visibility timeout applied while a handler runs, in seconds. */
  visibilityTimeoutSeconds: number;
  /** How long `close()` waits for running handlers, in milliseconds. */
  shutdownGraceMs: number;
  /** Poll interval for live stream reads, in milliseconds. */
  streamPollIntervalMs: number;
}

export type AwsWorldOptions = Partial<AwsWorldConfig>;

export const ENV = {
  region: 'WORKFLOW_AWS_REGION',
  tableName: 'WORKFLOW_AWS_TABLE',
  workflowQueueUrl: 'WORKFLOW_AWS_WORKFLOW_QUEUE_URL',
  stepQueueUrl: 'WORKFLOW_AWS_STEP_QUEUE_URL',
  bucket: 'WORKFLOW_AWS_BUCKET',
  endpoint: 'WORKFLOW_AWS_ENDPOINT',
} as const;

export const DEFAULTS = {
  concurrency: 10,
  visibilityTimeoutSeconds: 60,
  shutdownGraceMs: 30_000,
  streamPollIntervalMs: 250,
} as const;

/** SQS caps a visibility timeout at 12 hours. */
const MAX_VISIBILITY_TIMEOUT_SECONDS = 43_200;

const REGION_PATTERN = /^[a-z]{2}(-[a-z]+)+-\d+$/;
const TABLE_NAME_PATTERN = /^[a-zA-Z0-9_.-]{3,255}$/;
const BUCKET_PATTERN = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;

/** Thrown by {@link resolveConfig} with every problem it found, one per line. */
export class AwsWorldConfigError extends Error {
  readonly problems: readonly string[];

  constructor(problems: string[]) {
    super(
      `Invalid AWS World configuration:\n${problems
        .map((problem) => `  - ${problem}`)
        .join('\n')}`
    );
    this.name = 'AwsWorldConfigError';
    this.problems = problems;
  }
}

type Env = Record<string, string | undefined>;

/**
 * Builds a validated {@link AwsWorldConfig} from explicit options, falling
 * back to `WORKFLOW_AWS_*` variables in `env`. Empty strings count as unset.
 *
 * @throws {AwsWorldConfigError} listing every missing or invalid value.
 */
export function resolveConfig(
  env: Env = process.env,
  options: AwsWorldOptions = {}
): AwsWorldConfig {
  const problems: string[] = [];

  const pick = (key: keyof typeof ENV): string | undefined => {
    const explicit = options[key];
    if (typeof explicit === 'string' && explicit.trim() !== '') {
      return explicit.trim();
    }
    const fromEnv = env[ENV[key]];
    return fromEnv !== undefined && fromEnv.trim() !== ''
      ? fromEnv.trim()
      : undefined;
  };

  const required = (key: keyof typeof ENV, description: string): string => {
    const value = pick(key);
    if (value === undefined) {
      problems.push(
        `${description} is required: pass \`${key}\` or set ${ENV[key]}`
      );
      return '';
    }
    return value;
  };

  const region = required('region', 'AWS region');
  if (region && !REGION_PATTERN.test(region)) {
    problems.push(
      `${ENV.region} "${region}" is not an AWS region name such as "us-east-1"`
    );
  }

  const tableName = required('tableName', 'DynamoDB table name');
  if (tableName && !TABLE_NAME_PATTERN.test(tableName)) {
    problems.push(
      `${ENV.tableName} "${tableName}" must be 3-255 characters of letters, digits, "_", "-" or "."`
    );
  }

  const workflowQueueUrl = required('workflowQueueUrl', 'Workflow queue URL');
  checkUrl(workflowQueueUrl, ENV.workflowQueueUrl, problems);

  const stepQueueUrl = required('stepQueueUrl', 'Step queue URL');
  checkUrl(stepQueueUrl, ENV.stepQueueUrl, problems);

  if (workflowQueueUrl && stepQueueUrl && workflowQueueUrl === stepQueueUrl) {
    problems.push(
      `${ENV.workflowQueueUrl} and ${ENV.stepQueueUrl} must be different queues`
    );
  }

  const bucket = pick('bucket');
  if (bucket !== undefined && !BUCKET_PATTERN.test(bucket)) {
    problems.push(
      `${ENV.bucket} "${bucket}" is not a valid S3 bucket name (3-63 lowercase letters, digits, "." or "-")`
    );
  }

  const endpoint = pick('endpoint');
  if (endpoint !== undefined) {
    checkUrl(endpoint, ENV.endpoint, problems);
  }

  const concurrency = integerOption(
    options,
    'concurrency',
    1,
    Number.MAX_SAFE_INTEGER,
    problems
  );
  const visibilityTimeoutSeconds = integerOption(
    options,
    'visibilityTimeoutSeconds',
    0,
    MAX_VISIBILITY_TIMEOUT_SECONDS,
    problems
  );
  const shutdownGraceMs = integerOption(
    options,
    'shutdownGraceMs',
    0,
    Number.MAX_SAFE_INTEGER,
    problems
  );
  const streamPollIntervalMs = integerOption(
    options,
    'streamPollIntervalMs',
    1,
    Number.MAX_SAFE_INTEGER,
    problems
  );

  if (problems.length > 0) {
    throw new AwsWorldConfigError(problems);
  }

  return {
    region,
    tableName,
    workflowQueueUrl,
    stepQueueUrl,
    ...(bucket !== undefined && { bucket }),
    ...(endpoint !== undefined && { endpoint }),
    concurrency,
    visibilityTimeoutSeconds,
    shutdownGraceMs,
    streamPollIntervalMs,
  };
}

function checkUrl(value: string, name: string, problems: string[]): void {
  if (!value) return;
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    problems.push(`${name} "${value}" is not a valid URL`);
    return;
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    problems.push(`${name} "${value}" must use http or https`);
  }
}

function integerOption(
  options: AwsWorldOptions,
  key: keyof typeof DEFAULTS,
  min: number,
  max: number,
  problems: string[]
): number {
  const value = options[key] ?? DEFAULTS[key];
  if (!Number.isInteger(value) || value < min || value > max) {
    problems.push(
      `\`${key}\` must be an integer between ${min} and ${max}, got ${String(value)}`
    );
  }
  return value;
}
