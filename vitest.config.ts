import { configDefaults, defineConfig } from 'vitest/config';

// Unit tests only. They mock every AWS call and never need a network.
// Integration tests live in test/integration and use
// vitest.integration.config.ts.
export default defineConfig({
  test: {
    include: [
      'src/**/*.test.ts',
      'scripts/**/*.test.ts',
      'test/unit/**/*.test.ts',
    ],
    exclude: [...configDefaults.exclude, 'test/integration/**'],
  },
});
