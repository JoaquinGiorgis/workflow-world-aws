// Declares the flow route bundle `workflow build` writes, so the app's
// TypeScript can import its POST handler.
import { writeFileSync } from 'node:fs';

writeFileSync(
  new URL('../.well-known/workflow/v1/flow.d.mts', import.meta.url),
  'export declare function POST(request: Request): Promise<Response>;\n'
);
