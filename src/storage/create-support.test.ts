import { afterEach, describe, expect, it, vi } from 'vitest';
import { hookRetentionLimitMs } from './create-support.js';

const DAY_MS = 24 * 60 * 60 * 1000;

afterEach(() => {
  vi.unstubAllEnvs();
});

describe('hookRetentionLimitMs', () => {
  it('is 30 days by default', () => {
    vi.stubEnv('WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS', undefined);
    expect(hookRetentionLimitMs()).toBe(30 * DAY_MS);
  });

  it.each(['', '  '])('counts %j as unset', (value) => {
    vi.stubEnv('WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS', value);
    expect(hookRetentionLimitMs()).toBe(30 * DAY_MS);
  });

  it('reads the variable', () => {
    vi.stubEnv('WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS', ' 60 ');
    expect(hookRetentionLimitMs()).toBe(60 * DAY_MS);
  });

  it('refuses a value that is not a positive number', () => {
    vi.stubEnv('WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS', '0');
    expect(() => hookRetentionLimitMs()).toThrow(
      'WORKFLOW_AWS_HOOK_RETENTION_LIMIT_DAYS must be a positive number, got "0"'
    );
  });
});
