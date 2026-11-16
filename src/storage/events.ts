import { WorkflowWorldError } from '@workflow/errors';
import type {
  AnyEventRequest,
  CreateEventParams,
  Event,
  EventListResponse,
  EventResult,
  EventsResolveData,
  ListEventsParams,
  PaginatedResponse,
  Storage,
} from '@workflow/world';
import {
  eventIdToSlot,
  FIRST_EVENT_SLOT,
  getMaxEventsPerRun,
  SPEC_VERSION_CURRENT,
  slotToEventId,
  stripEventDataRefs,
  validateUlidTimestamp,
} from '@workflow/world';
import { monotonicFactory } from 'ulid';
import { eventKey, eventSk, PREFIX, runMetaKey, runPk } from '../keys.js';
import { readRun } from './commit.js';
import type { StorageContext } from './context.js';
import { createEventForRun } from './create.js';
import {
  hookRetentionLimitMs,
  type Outcome,
  retentionLimitError,
} from './create-support.js';
import { createHookResume } from './hook-events.js';
import { eventFromItem, type Item } from './items.js';
import { createRun, type RunCreationData } from './run-creation.js';
import type { Table } from './table.js';

/** Largest page of the inline delta (`sinceCursor`). */
const DELTA_PAGE = 100;

export function createEventsStorage(
  context: StorageContext
): Storage['events'] {
  const { table } = context;
  const ulid = monotonicFactory();

  const list = async (params: ListEventsParams): Promise<EventListResponse> => {
    // Read before listing: every event the counters include is committed.
    const run = await table.get(runMetaKey(params.runId));
    const snapshot =
      typeof run?.seq === 'number'
        ? {
            seq: run.seq,
            seqInBand: typeof run.seqInBand === 'number' ? run.seqInBand : 0,
          }
        : undefined;
    const limit = params.pagination?.limit ?? getMaxEventsPerRun();
    const ascending = (params.pagination?.sortOrder ?? 'asc') === 'asc';
    const resolveData = params.resolveData ?? 'all';
    const items = await table.collect(
      {
        KeyConditionExpression: 'PK = :pk AND begins_with(SK, :prefix)',
        ExpressionAttributeValues: {
          ':pk': runPk(params.runId),
          ':prefix': PREFIX.event,
        },
        ConsistentRead: true,
        ScanIndexForward: ascending,
        ...(params.pagination?.cursor && {
          ExclusiveStartKey: eventKey(params.runId, params.pagination.cursor),
        }),
      },
      limit + 1
    );
    const data = items
      .slice(0, limit)
      .map((item) => stripEventDataRefs(eventFromItem(item), resolveData));
    return {
      data,
      cursor: data.at(-1)?.eventId ?? null,
      hasMore: items.length > limit,
      ...(snapshot && { snapshot }),
    };
  };

  async function create(
    runId: string | null,
    data: AnyEventRequest,
    params?: CreateEventParams
  ): Promise<EventResult> {
    const limitMs = hookRetentionLimitMs();
    if (
      data.eventType === 'hook_created' &&
      data.eventData.tokenRetentionUntil !== undefined &&
      new Date(data.eventData.tokenRetentionUntil).getTime() >
        Date.now() + limitMs
    ) {
      throw retentionLimitError(limitMs, false);
    }
    const startHook =
      data.eventType === 'run_created' || data.eventType === 'run_started'
        ? data.eventData?.startHook
        : undefined;
    if (
      startHook?.tokenRetentionUntil !== undefined &&
      new Date(startHook.tokenRetentionUntil).getTime() > Date.now() + limitMs
    ) {
      throw retentionLimitError(limitMs, true);
    }

    let effectiveRunId: string;
    if (data.eventType === 'run_created' && !runId) {
      effectiveRunId = `wrun_${ulid()}`;
    } else if (!runId) {
      throw new Error('runId is required for non-run_created events');
    } else {
      effectiveRunId = runId;
    }
    const specVersion = data.specVersion ?? SPEC_VERSION_CURRENT;
    const resolveData: EventsResolveData = params?.resolveData ?? 'all';

    if (data.eventType === 'run_created') {
      if (runId) {
        const invalid = validateUlidTimestamp(effectiveRunId, 'wrun_');
        if (invalid) {
          throw new WorkflowWorldError(invalid, {
            ...(startHook && {
              status: 400,
              code: 'START_HOOK_ADMISSION_REJECTED',
              reason: 'expired',
            }),
          });
        }
      }
      const created = await createRun(
        context,
        effectiveRunId,
        data.eventData as RunCreationData,
        specVersion,
        'run_created'
      );
      const event = created.event ? eventFromItem(created.event) : undefined;
      return {
        ...(event && { event: stripEventDataRefs(event, resolveData) }),
        run: created.run,
      } as EventResult;
    }

    // Resilient start: a run_started carrying the run's creation data
    // creates the run first when its run_created never landed.
    if (data.eventType === 'run_started' && data.eventData) {
      const creation = data.eventData as Partial<RunCreationData>;
      if (
        creation.deploymentId &&
        creation.workflowName &&
        creation.input !== undefined &&
        !(await readRun(table, effectiveRunId)).item
      ) {
        await createRun(
          context,
          effectiveRunId,
          creation as RunCreationData,
          specVersion,
          'run_started'
        ).catch((error: unknown) => {
          // A concurrent run_created won: start that run.
          if ((error as Error)?.name !== 'EntityConflictError') throw error;
        });
      }
    }

    const request = { runId: effectiveRunId, data, params, specVersion };
    const outcome: Outcome =
      data.eventType === 'hook_received' && params?.resumeId
        ? await createHookResume(context, {
            ...request,
            data,
            resumeId: params.resumeId,
          })
        : await createEventForRun(context, request);
    return respond(table, effectiveRunId, data, params, outcome, list);
  }

  return {
    create: create as Storage['events']['create'],

    async get(runId, eventId, params) {
      const item = await table.get(eventKey(runId, eventId));
      if (!item) throw new WorkflowWorldError(`Event not found: ${eventId}`);
      return stripEventDataRefs(
        eventFromItem(item),
        params?.resolveData ?? 'all'
      );
    },

    list,

    async listByCorrelationId(params) {
      const limit = params.pagination?.limit ?? 100;
      const ascending = (params.pagination?.sortOrder ?? 'asc') === 'asc';
      const items = await table.collect(
        {
          KeyConditionExpression: 'PK = :pk AND begins_with(SK, :prefix)',
          FilterExpression: 'correlationId = :c',
          ExpressionAttributeValues: {
            ':pk': runPk(params.runId),
            ':prefix': PREFIX.event,
            ':c': params.correlationId,
          },
          ConsistentRead: true,
          ScanIndexForward: ascending,
          ...(params.pagination?.cursor && {
            ExclusiveStartKey: eventKey(params.runId, params.pagination.cursor),
          }),
        },
        limit + 1
      );
      const resolveData = params.resolveData ?? 'all';
      const data = items
        .slice(0, limit)
        .map((item) => stripEventDataRefs(eventFromItem(item), resolveData));
      return {
        data,
        cursor: data.at(-1)?.eventId ?? null,
        hasMore: items.length > limit,
      } satisfies PaginatedResponse<Event>;
    },
  } as Storage['events'];
}

/**
 * The response of a committed create: the event and entity, the in-band
 * allocation, and at most one event page, as world-postgres answers it:
 *
 * - `sinceCursor`: the page `events.list` would return after that cursor;
 * - else `eventCount`: bump and report, the events on the slots between the
 *   one the writer asked for and the one it landed on. Slots are allocated
 *   at the commit, so every one of them is committed and the report is
 *   complete (`hasMore: false`);
 * - and a `run_started` preloads the whole log unless `skipPreload`.
 */
async function respond(
  table: Table,
  runId: string,
  data: AnyEventRequest,
  params: CreateEventParams | undefined,
  outcome: Outcome,
  list: (params: ListEventsParams) => Promise<EventListResponse>
): Promise<EventResult> {
  const resolveData: EventsResolveData = params?.resolveData ?? 'all';
  const event = outcome.event
    ? stripEventDataRefs(eventFromItem(outcome.event), resolveData)
    : undefined;
  const result: EventResult = {
    ...(event && { event }),
    run: outcome.run,
    step: outcome.step,
    hook: outcome.hook,
    wait: outcome.wait,
    ...(outcome.stepCreated && { stepCreated: true as const }),
    ...(params?.inBand && { allocated: outcome.allocated }),
  };

  let page: PaginatedResponse<Event> | undefined;
  if (
    params?.eventCount !== undefined &&
    typeof params.sinceCursor !== 'string' &&
    event
  ) {
    page = await skippedSlots(
      table,
      runId,
      event.eventId,
      params.eventCount,
      resolveData
    );
  }
  if (
    data.eventType === 'run_started' &&
    outcome.run &&
    event &&
    !params?.skipPreload
  ) {
    const all = await list({ runId, resolveData });
    page = {
      data: all.data,
      cursor: all.data.at(-1)?.eventId ?? null,
      hasMore: false,
    };
  }
  if (typeof params?.sinceCursor === 'string') {
    const delta = await list({
      runId,
      resolveData,
      pagination: {
        cursor: params.sinceCursor,
        limit: DELTA_PAGE,
        sortOrder: 'asc',
      },
    });
    page = { data: delta.data, cursor: delta.cursor, hasMore: delta.hasMore };
  }
  if (!page) return result;
  return {
    ...result,
    events: page.data,
    cursor: page.cursor,
    hasMore: page.hasMore,
  };
}

async function skippedSlots(
  table: Table,
  runId: string,
  committedEventId: string,
  askedFor: number,
  resolveData: EventsResolveData
): Promise<PaginatedResponse<Event> | undefined> {
  const committedSlot = eventIdToSlot(committedEventId);
  if (
    committedSlot === null ||
    askedFor < FIRST_EVENT_SLOT ||
    committedSlot <= askedFor + 1
  ) {
    return undefined;
  }
  const items = await table.collect(
    {
      KeyConditionExpression: 'PK = :pk AND SK BETWEEN :from AND :to',
      ExpressionAttributeValues: {
        ':pk': runPk(runId),
        ':from': eventSk(slotToEventId(askedFor + 1)),
        ':to': eventSk(slotToEventId(committedSlot - 1)),
      },
      ConsistentRead: true,
    },
    committedSlot - askedFor
  );
  const events = items.map((item: Item) =>
    stripEventDataRefs(eventFromItem(item), resolveData)
  );
  // Deliberately no cursor: the report is not a page the caller has read
  // to the end of.
  return {
    data: events,
    cursor: null,
    hasMore: events.length < committedSlot - askedFor - 1,
  };
}
