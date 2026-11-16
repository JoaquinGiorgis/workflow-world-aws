/**
 * Checks docs/iam-policy.json against the code: every AWS SDK command the
 * World sends (`new <Name>Command(` anywhere in src/, tests aside) has to map
 * to IAM actions below, and the policy has to allow exactly those actions,
 * on the resources they touch. A new command fails here until it is mapped
 * and the policy (and the README's IAM section) allow it.
 */
import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const root = path.resolve(fileURLToPath(import.meta.url), '../../..');

type Service = 'dynamodb' | 'sqs' | 's3';

/** The IAM actions each command needs. */
const COMMAND_ACTIONS: Record<string, { service: Service; actions: string[] }> =
  {
    GetCommand: { service: 'dynamodb', actions: ['dynamodb:GetItem'] },
    BatchGetCommand: {
      service: 'dynamodb',
      actions: ['dynamodb:BatchGetItem'],
    },
    PutCommand: { service: 'dynamodb', actions: ['dynamodb:PutItem'] },
    UpdateCommand: { service: 'dynamodb', actions: ['dynamodb:UpdateItem'] },
    DeleteCommand: { service: 'dynamodb', actions: ['dynamodb:DeleteItem'] },
    QueryCommand: { service: 'dynamodb', actions: ['dynamodb:Query'] },
    // A transaction is authorized by the item operations inside it.
    TransactWriteCommand: {
      service: 'dynamodb',
      actions: [
        'dynamodb:PutItem',
        'dynamodb:UpdateItem',
        'dynamodb:DeleteItem',
        'dynamodb:ConditionCheckItem',
      ],
    },
    SendMessageCommand: { service: 'sqs', actions: ['sqs:SendMessage'] },
    SendMessageBatchCommand: { service: 'sqs', actions: ['sqs:SendMessage'] },
    ReceiveMessageCommand: { service: 'sqs', actions: ['sqs:ReceiveMessage'] },
    DeleteMessageCommand: { service: 'sqs', actions: ['sqs:DeleteMessage'] },
    ChangeMessageVisibilityCommand: {
      service: 'sqs',
      actions: ['sqs:ChangeMessageVisibility'],
    },
    PutObjectCommand: { service: 's3', actions: ['s3:PutObject'] },
    GetObjectCommand: { service: 's3', actions: ['s3:GetObject'] },
    DeleteObjectCommand: { service: 's3', actions: ['s3:DeleteObject'] },
  };

const TABLE = 'arn:aws:dynamodb:<region>:<account-id>:table/<table>';
const INDEXES = `${TABLE}/index/*`;

/** The resources the World touches, as the policy's placeholders name them. */
const RESOURCES: Record<Service, string[]> = {
  dynamodb: [TABLE],
  sqs: [
    'arn:aws:sqs:<region>:<account-id>:<workflow-queue>',
    'arn:aws:sqs:<region>:<account-id>:<step-queue>',
  ],
  s3: ['arn:aws:s3:::<bucket>/*'],
};

/** Where an action applies: queries also read GSI1, GSI2 and GSI3. */
function resourcesFor(service: Service, action: string): string[] {
  return action === 'dynamodb:Query'
    ? [...RESOURCES[service], INDEXES]
    : RESOURCES[service];
}

interface Statement {
  Sid?: string;
  Effect: string;
  Action: string | string[];
  Resource: string | string[];
}

const list = (value: string | string[]) =>
  Array.isArray(value) ? value : [value];

/** Every `.ts` file under `dir`, tests aside. */
function sources(dir: string): string[] {
  return readdirSync(dir, { recursive: true, encoding: 'utf8' })
    .filter((file) => file.endsWith('.ts') && !file.endsWith('.test.ts'))
    .map((file) => path.join(dir, file));
}

/** The command classes the World's code sends, by file. */
function commandsInSource(): Map<string, string[]> {
  const found = new Map<string, string[]>();
  for (const file of sources(path.join(root, 'src'))) {
    const text = readFileSync(file, 'utf8');
    for (const [, name] of text.matchAll(/new (\w+Command)\(/g)) {
      const files = found.get(name as string) ?? [];
      files.push(path.relative(root, file));
      found.set(name as string, files);
    }
  }
  return found;
}

const policy = JSON.parse(
  readFileSync(path.join(root, 'docs/iam-policy.json'), 'utf8')
) as { Version: string; Statement: Statement[] };

function allows(action: string, resource: string): boolean {
  return policy.Statement.some(
    (statement) =>
      statement.Effect === 'Allow' &&
      list(statement.Action).includes(action) &&
      list(statement.Resource).includes(resource)
  );
}

describe('docs/iam-policy.json', () => {
  const used = commandsInSource();

  it('finds the commands the World sends', () => {
    // A sanity check of the scan itself.
    expect(used.has('TransactWriteCommand')).toBe(true);
    expect(used.has('ReceiveMessageCommand')).toBe(true);
    expect(used.has('PutObjectCommand')).toBe(true);
  });

  it('knows the IAM actions of every command in src/', () => {
    const unknown = [...used.keys()].filter(
      (name) => !(name in COMMAND_ACTIONS)
    );
    expect(
      unknown.map((name) => `${name} (${used.get(name)?.join(', ')})`)
    ).toEqual([]);
  });

  it('allows every action the code needs on every resource it touches', () => {
    const missing: string[] = [];
    for (const name of used.keys()) {
      const needs = COMMAND_ACTIONS[name];
      if (!needs) continue;
      for (const action of needs.actions) {
        for (const resource of resourcesFor(needs.service, action)) {
          if (!allows(action, resource)) {
            missing.push(`${action} on ${resource} (${name})`);
          }
        }
      }
    }
    expect(missing).toEqual([]);
  });

  it('allows nothing else', () => {
    // Every action and resource pair granted is one the code needs.
    const needed = new Set(
      [...used.keys()].flatMap((name) => {
        const needs = COMMAND_ACTIONS[name];
        if (!needs) return [];
        return needs.actions.flatMap((action) =>
          resourcesFor(needs.service, action).map(
            (resource) => `${action} on ${resource}`
          )
        );
      })
    );
    const granted = policy.Statement.flatMap((statement) =>
      list(statement.Action).flatMap((action) =>
        list(statement.Resource).map((resource) => `${action} on ${resource}`)
      )
    );
    expect(granted.filter((pair) => !needed.has(pair))).toEqual([]);
    expect(policy.Version).toBe('2012-10-17');
    expect(policy.Statement.every((s) => s.Effect === 'Allow')).toBe(true);
  });

  it('is the policy the README shows', () => {
    const readme = readFileSync(path.join(root, 'README.md'), 'utf8');
    const shown = /## IAM[\s\S]*?```json\n([\s\S]*?)```/.exec(readme)?.[1];
    expect(shown && JSON.parse(shown)).toEqual(policy);
  });
});
