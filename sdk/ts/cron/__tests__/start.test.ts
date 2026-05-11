import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { start } from "../src/client.js";
import { cronClient, findFreePort, uniqueName } from "./harness.js";

describe("start() — worker lifecycle", () => {
  const client = cronClient();
  const serverUrl = process.env["QUEUE_TEST_URL"]!;
  const cleanupNames: string[] = [];
  let originalInternalUrl: string | undefined;

  beforeEach(() => {
    originalInternalUrl = process.env["BEYOND_INTERNAL_URL"];
    process.env["BEYOND_INTERNAL_URL"] = "http://127.0.0.1";
  });

  afterEach(async () => {
    if (originalInternalUrl === undefined) {
      delete process.env["BEYOND_INTERNAL_URL"];
    } else {
      process.env["BEYOND_INTERNAL_URL"] = originalInternalUrl;
    }
    for (const name of cleanupNames.splice(0)) {
      await client.schedules.delete(name);
    }
  });

  it("registers the schedule and HTTP subscription on startup", async () => {
    const name = uniqueName("srv");
    cleanupNames.push(name);
    const port = await findFreePort();
    const ac = new AbortController();

    const done = start(
      [{ name, spec: { name, every: "1h" }, handler: async () => {} }],
      { url: serverUrl, port, signal: ac.signal },
    );

    await new Promise<void>((r) => setTimeout(r, 200));

    try {
      const { data: sched } = await client.schedules.get(name);
      expect(sched?.name).toBe(name);
      const target = sched?.target as Record<string, unknown> | undefined;
      expect(target?.["topic"]).toBe(`__cron_${name}`);

      const { data: subs } = await (
        await import("openapi-fetch")
      ).default({ baseUrl: serverUrl }).GET(
        "/v1/events/{pattern}/subscriptions" as never,
        { params: { path: { pattern: `__cron_${name}` } } } as never,
      ) as unknown as {
        data: Array<{ endpoint: string; protocol: string }> | undefined;
      };
      expect(
        subs?.some(
          (s) =>
            s.endpoint === `http://127.0.0.1:${port}/__cron/${name}`
            && (s.protocol === "http" || s.protocol === "https"),
        ),
      ).toBe(true);
    } finally {
      ac.abort();
      await done;
    }
  });

  it("routes a manual run to the handler", async () => {
    const name = uniqueName("run");
    cleanupNames.push(name);
    const port = await findFreePort();
    const ac = new AbortController();

    let fired = false;
    const done = start(
      [
        {
          name,
          spec: { name, every: "1h" },
          handler: async (ctx) => {
            expect(ctx.name).toBe(name);
            fired = true;
          },
        },
      ],
      { url: serverUrl, port, signal: ac.signal },
    );

    await new Promise<void>((r) => setTimeout(r, 200));

    try {
      const res = await fetch(`http://127.0.0.1:${port}/__cron/${name}`, {
        method: "POST",
        body: "{}",
        headers: { "content-type": "application/json" },
      });
      expect(res.status).toBe(200);
      expect(fired).toBe(true);
    } finally {
      ac.abort();
      await done;
    }
  });

  it("returns 500 when the handler throws", async () => {
    const name = uniqueName("err");
    cleanupNames.push(name);
    const port = await findFreePort();
    const ac = new AbortController();

    const done = start(
      [
        {
          name,
          spec: { name, every: "1h" },
          handler: async () => {
            throw new Error("boom");
          },
        },
      ],
      { url: serverUrl, port, signal: ac.signal },
    );

    await new Promise<void>((r) => setTimeout(r, 200));

    try {
      const res = await fetch(`http://127.0.0.1:${port}/__cron/${name}`, {
        method: "POST",
        body: "{}",
        headers: { "content-type": "application/json" },
      });
      expect(res.status).toBe(500);
    } finally {
      ac.abort();
      await done;
    }
  });

  it("returns 404 for unknown schedule names", async () => {
    const name = uniqueName("404");
    cleanupNames.push(name);
    const port = await findFreePort();
    const ac = new AbortController();

    const done = start(
      [{ name, spec: { name, every: "1h" }, handler: async () => {} }],
      { url: serverUrl, port, signal: ac.signal },
    );

    await new Promise<void>((r) => setTimeout(r, 200));

    try {
      const res = await fetch(
        `http://127.0.0.1:${port}/__cron/does-not-exist`,
        {
          method: "POST",
          body: "{}",
        },
      );
      expect(res.status).toBe(404);
    } finally {
      ac.abort();
      await done;
    }
  });

  it("deletes stale subscriptions from a previous deployment URL", async () => {
    const name = uniqueName("stale");
    cleanupNames.push(name);
    const stalePort = await findFreePort();
    const freshPort = await findFreePort();
    const serverFetch = (await import("openapi-fetch")).default({
      baseUrl: serverUrl,
    });

    await client.schedules.upsert({
      name,
      every: "1h",
      target: { topic: `__cron_${name}`, message: {} },
    });
    await serverFetch.POST("/v1/events/{pattern}/subscriptions" as never, {
      params: { path: { pattern: `__cron_${name}` } },
      body: {
        protocol: "http",
        endpoint: `http://127.0.0.1:${stalePort}/__cron/${name}`,
        envelope: false,
      },
    } as never);

    const ac = new AbortController();
    const done = start(
      [{ name, spec: { name, every: "1h" }, handler: async () => {} }],
      { url: serverUrl, port: freshPort, signal: ac.signal },
    );

    await new Promise<void>((r) => setTimeout(r, 300));

    try {
      const { data: subs } = await serverFetch.GET(
        "/v1/events/{pattern}/subscriptions" as never,
        { params: { path: { pattern: `__cron_${name}` } } } as never,
      ) as unknown as {
        data: Array<{ endpoint: string; protocol: string }> | undefined;
      };

      expect(subs?.some((s) => s.endpoint.includes(`:${stalePort}`))).toBe(
        false,
      );
      expect(
        subs?.some((s) =>
          s.endpoint === `http://127.0.0.1:${freshPort}/__cron/${name}`
        ),
      ).toBe(true);
    } finally {
      ac.abort();
      await done;
    }
  });

  it("removes SDK-managed schedules not in the jobs list", async () => {
    const orphan = uniqueName("orphan");
    const keep = uniqueName("keep");
    cleanupNames.push(keep);

    await client.schedules.upsert({
      name: orphan,
      every: "1h",
      target: { topic: `__cron_${orphan}`, message: {} },
    });

    const port = await findFreePort();
    const ac = new AbortController();

    const done = start(
      [{
        name: keep,
        spec: { name: keep, every: "1h" },
        handler: async () => {},
      }],
      { url: serverUrl, port, signal: ac.signal },
    );

    await new Promise<void>((r) => setTimeout(r, 300));

    try {
      const { error } = await client.schedules.get(orphan);
      expect(error?.status).toBe(404);
    } finally {
      ac.abort();
      await done;
    }
  });

  it("shuts down cleanly on signal abort", async () => {
    const name = uniqueName("shut");
    cleanupNames.push(name);
    const port = await findFreePort();
    const ac = new AbortController();

    const done = start(
      [{ name, spec: { name, every: "1h" }, handler: async () => {} }],
      { url: serverUrl, port, signal: ac.signal },
    );

    await new Promise<void>((r) => setTimeout(r, 200));
    ac.abort();
    await expect(done).resolves.toBeUndefined();
  });
});
