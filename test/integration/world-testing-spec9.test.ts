/**
 * The `@workflow/world-testing` suite at spec 9 (single orchestrator, in-band
 * fence), as world-postgres runs it in `test/spec9.test.ts`. See
 * `world-testing-suite.ts`.
 */
import { registerWorldTestingSuite } from './world-testing-suite.js';

await registerWorldTestingSuite(9);
