import { defineConfig } from 'tsup';

export default defineConfig({
  entry: ['src/index.ts'],
  format: ['esm'],
  target: 'node22',
  platform: 'node',
  // TypeScript 7 ships no compiler API for rollup-plugin-dts, so
  // declarations come from `tsc -p tsconfig.build.json` instead.
  dts: false,
  sourcemap: true,
  clean: true,
  splitting: false,
});
