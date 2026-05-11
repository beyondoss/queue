import { createServer } from "node:http";
import { describe, expect, it } from "vitest";
import { createCronClient } from "../src/client.js";
import { findFreePort } from "./harness.js";

type MockResponse = { status: number; body: string };

async function withMockServer(
  responses: MockResponse[],
  fn: (url: string, requestCount: () => number) => Promise<void>,
) {
  const port = await findFreePort();
  let count = 0;
  const server = createServer((_, res) => {
    const r = responses[Math.min(count++, responses.length - 1)]!;
    res.writeHead(r.status, { "content-type": "application/json" });
    res.end(r.body);
  });
  await new Promise<void>((r) => server.listen(port, "127.0.0.1", r));
  try {
    await fn(`http://127.0.0.1:${port}`, () => count);
  } finally {
    await new Promise<void>((r) => server.close(() => r()));
  }
}

describe("buildFetch — retry behavior", () => {
  it("retries on 5xx and returns the successful response", async () => {
    await withMockServer(
      [
        {
          status: 503,
          body: "{\"error\":{\"code\":\"unavailable\",\"message\":\"down\"}}",
        },
        { status: 200, body: "{\"name\":\"t\",\"status\":\"active\"}" },
      ],
      async (url, requestCount) => {
        const client = createCronClient({ url, retries: 2 });
        const { data, error } = await client.schedules.get("t");
        expect(requestCount()).toBe(2);
        expect(error).toBeUndefined();
        expect(data?.name).toBe("t");
      },
    );
  });

  it("returns error after exhausting retries", async () => {
    await withMockServer(
      [{
        status: 503,
        body: "{\"error\":{\"code\":\"unavailable\",\"message\":\"down\"}}",
      }],
      async (url, requestCount) => {
        const client = createCronClient({ url, retries: 2 });
        const { error } = await client.schedules.get("t");
        expect(requestCount()).toBe(3); // 1 initial + 2 retries
        expect(error?.status).toBe(503);
        expect(error?.code).toBe("unavailable");
      },
    );
  });

  it("does not retry on 4xx errors", async () => {
    await withMockServer(
      [{
        status: 404,
        body: "{\"error\":{\"code\":\"not_found\",\"message\":\"missing\"}}",
      }],
      async (url, requestCount) => {
        const client = createCronClient({ url, retries: 2 });
        const { error } = await client.schedules.get("t");
        expect(requestCount()).toBe(1);
        expect(error?.status).toBe(404);
      },
    );
  });
});
