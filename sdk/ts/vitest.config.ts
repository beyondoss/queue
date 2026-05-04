import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    globalSetup: ["./src/__tests__/global-setup.ts"],
    testTimeout: 60_000,
    hookTimeout: 60_000,
    include: ["src/__tests__/**/*.test.ts"],
    forceExit: true,
  },
});
