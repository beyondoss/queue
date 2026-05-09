import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["__tests__/**/*.test-d.ts"],
    typecheck: {
      include: ["__tests__/**/*.test-d.ts"],
    },
  },
});
