/**
 * A diagnostic line, only under `DEBUG=workflow:*` (or `DEBUG=*`), the gate
 * `@workflow/utils`' `debugLog` uses for the other Worlds.
 */
export function debugLog(message: string, detail: Record<string, string>) {
  const debug = process.env.DEBUG;
  if (typeof debug !== 'string') return;
  if (!debug.includes('workflow:') && debug !== '*') return;
  console.debug(`[world-aws] ${message}`, detail);
}
