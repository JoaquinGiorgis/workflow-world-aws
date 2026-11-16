/** Where the World reports what an operator should see. */
export type WarnLog = (
  message: string,
  detail?: Record<string, unknown>
) => void;

/** The default {@link WarnLog}: `console.warn`, prefixed `[world-aws]`. */
export const warnLog: WarnLog = (message, detail) => {
  if (detail === undefined) console.warn(`[world-aws] ${message}`);
  else console.warn(`[world-aws] ${message}`, detail);
};

/**
 * A diagnostic line, only under `DEBUG=workflow:*` (or `DEBUG=*`), the gate
 * `@workflow/utils`' `debugLog` uses for the other Worlds.
 */
export function debugLog(message: string, detail: Record<string, unknown>) {
  const debug = process.env.DEBUG;
  if (typeof debug !== 'string') return;
  if (!debug.includes('workflow:') && debug !== '*') return;
  console.debug(`[world-aws] ${message}`, detail);
}
