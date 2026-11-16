import { WorkflowWorldError } from '@workflow/errors';
import type { Storage } from '@workflow/world';
import { PREFIX, runPk, stepKey } from '../keys.js';
import { cursorOf, decodeCursor } from './cursor.js';
import { filterStepData, stepFromItem } from './items.js';
import type { Table } from './table.js';

export function createStepsStorage(table: Table): Storage['steps'] {
  return {
    get: (async (runId, stepId, params) => {
      const item = await table.get(stepKey(runId, stepId));
      if (!item) throw new WorkflowWorldError(`Step not found: ${stepId}`);
      return filterStepData(stepFromItem(item), params?.resolveData ?? 'all');
    }) as Storage['steps']['get'],

    list: (async (params) => {
      const limit = params?.pagination?.limit ?? 20;
      // Newest first by default, like world-postgres (step ids are
      // `step_<ulid>`, so id order is creation order).
      const ascending = params?.pagination?.sortOrder === 'asc';
      const cursor = params?.pagination?.cursor;
      const scope = `table:${runPk(params.runId)}:${PREFIX.step}`;
      const items = await table.collect(
        {
          KeyConditionExpression: 'PK = :pk AND begins_with(SK, :prefix)',
          ExpressionAttributeValues: {
            ':pk': runPk(params.runId),
            ':prefix': PREFIX.step,
          },
          ConsistentRead: true,
          ScanIndexForward: ascending,
          ...(cursor && { ExclusiveStartKey: decodeCursor(scope, cursor) }),
        },
        limit + 1
      );
      const page = items.slice(0, limit);
      const resolveData = params?.resolveData ?? 'all';
      const last = page.at(-1);
      return {
        data: page.map((item) =>
          filterStepData(stepFromItem(item), resolveData)
        ),
        hasMore: items.length > limit,
        cursor: last ? cursorOf(scope, last) : null,
      };
    }) as Storage['steps']['list'],
  };
}
