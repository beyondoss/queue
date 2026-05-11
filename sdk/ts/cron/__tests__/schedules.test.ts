import { afterEach, describe, expect, it } from "vitest";
import { cronClient, uniqueName } from "./harness.js";

describe("schedules — upsert / list / get / delete", () => {
  const created: string[] = [];
  const client = cronClient();

  afterEach(async () => {
    for (const name of created.splice(0)) {
      await client.schedules.delete(name);
    }
  });

  it("upsert creates a schedule and returns it", async () => {
    const name = uniqueName();
    created.push(name);
    const { data, error } = await client.schedules.upsert({
      name,
      every: "5m",
      target: { queue: "test-q", message: { task: "ping" } },
    });
    expect(error).toBeUndefined();
    expect(data?.name).toBe(name);
    expect(data?.status).toBe("active");
  });

  it("upsert is idempotent", async () => {
    const name = uniqueName();
    created.push(name);
    const spec = {
      name,
      every: "5m",
      target: { queue: "test-q", message: {} },
    };
    await client.schedules.upsert(spec);
    const { data, error } = await client.schedules.upsert(spec);
    expect(error).toBeUndefined();
    expect(data?.name).toBe(name);
  });

  it("list includes a schedule after creation", async () => {
    const name = uniqueName();
    created.push(name);
    await client.schedules.upsert({
      name,
      cron: "0 9 * * 1-5",
      target: { queue: "test-q", message: {} },
    });
    const { data } = await client.schedules.list();
    expect(data?.some((s) => s.name === name)).toBe(true);
  });

  it("list filter by status", async () => {
    const name = uniqueName();
    created.push(name);
    await client.schedules.upsert({
      name,
      every: "1h",
      target: { queue: "test-q", message: {} },
    });
    await client.schedules.pause(name);

    const { data: paused } = await client.schedules.list({ status: "paused" });
    expect(paused?.some((s) => s.name === name)).toBe(true);

    const { data: active } = await client.schedules.list({ status: "active" });
    expect(active?.some((s) => s.name === name)).toBe(false);
  });

  it("list filter by namePrefix", async () => {
    const prefix = `pfx-${Math.random().toString(36).slice(2, 6)}`;
    const name = `${prefix}-job`;
    created.push(name);
    await client.schedules.upsert({
      name,
      every: "1h",
      target: { queue: "test-q", message: {} },
    });
    const { data } = await client.schedules.list({ namePrefix: prefix });
    expect(data?.every((s) => s.name.startsWith(prefix))).toBe(true);
    expect(data?.some((s) => s.name === name)).toBe(true);
  });

  it("get returns the schedule", async () => {
    const name = uniqueName();
    created.push(name);
    await client.schedules.upsert({
      name,
      every: "30m",
      target: { queue: "test-q", message: {} },
    });
    const { data, error } = await client.schedules.get(name);
    expect(error).toBeUndefined();
    expect(data?.name).toBe(name);
    expect(typeof data?.humanReadable).toBe("string");
    expect(Array.isArray(data?.nextFires)).toBe(true);
  });

  it("get returns error for missing schedule", async () => {
    const { error } = await client.schedules.get(uniqueName());
    expect(error).toBeDefined();
    expect(error?.status).toBe(404);
  });

  it("pause sets status to paused", async () => {
    const name = uniqueName();
    created.push(name);
    await client.schedules.upsert({
      name,
      every: "1h",
      target: { queue: "test-q", message: {} },
    });
    const { data } = await client.schedules.pause(name);
    expect(data?.status).toBe("paused");
  });

  it("resume sets status back to active", async () => {
    const name = uniqueName();
    created.push(name);
    await client.schedules.upsert({
      name,
      every: "1h",
      target: { queue: "test-q", message: {} },
    });
    await client.schedules.pause(name);
    const { data } = await client.schedules.resume(name);
    expect(data?.status).toBe("active");
  });

  it("delete removes the schedule", async () => {
    const name = uniqueName();
    await client.schedules.upsert({
      name,
      every: "1h",
      target: { queue: "test-q", message: {} },
    });
    await client.schedules.delete(name);
    const { error } = await client.schedules.get(name);
    expect(error?.status).toBe(404);
  });

  it("delete is idempotent", async () => {
    const name = uniqueName();
    await client.schedules.upsert({
      name,
      every: "1h",
      target: { queue: "test-q", message: {} },
    });
    await client.schedules.delete(name);
    const { error } = await client.schedules.delete(name);
    expect(error).toBeUndefined();
  });
});

describe("schedules — preview", () => {
  const client = cronClient();

  it("preview returns humanReadable and nextFires for cron", async () => {
    const { data, error } = await client.schedules.preview({
      cron: "0 9 * * 1-5",
      timezone: "America/New_York",
    });
    expect(error).toBeUndefined();
    expect(typeof data?.humanReadable).toBe("string");
    expect(data?.nextFires.length).toBeGreaterThan(0);
    expect(data?.timezone).toBe("America/New_York");
  });

  it("preview returns humanReadable for every shorthand", async () => {
    const { data, error } = await client.schedules.preview({ every: "30m" });
    expect(error).toBeUndefined();
    expect(typeof data?.humanReadable).toBe("string");
  });

  it("preview returns humanReadable for natural language", async () => {
    const { data, error } = await client.schedules.preview({
      when: "every weekday at 9am",
    });
    expect(error).toBeUndefined();
    expect(typeof data?.humanReadable).toBe("string");
  });
});

describe("schedules — sync", () => {
  const client = cronClient();

  it("sync upserts all specs and removes others", async () => {
    const keep = uniqueName("keep");
    const remove = uniqueName("remove");

    // Seed a schedule that should be removed
    await client.schedules.upsert({
      name: remove,
      every: "1h",
      target: { queue: "test-q", message: {} },
    });

    const { data, error } = await client.schedules.sync([
      {
        name: keep,
        every: "1h",
        target: { queue: "test-q", message: {} },
      },
    ]);
    expect(error).toBeUndefined();
    expect(data?.upserted).toBe(1);
    expect(data?.removed).toBeGreaterThanOrEqual(1);

    const { data: all } = await client.schedules.list();
    expect(all?.some((s) => s.name === keep)).toBe(true);
    expect(all?.some((s) => s.name === remove)).toBe(false);

    // Cleanup
    await client.schedules.delete(keep);
  });
});

describe("schedules — observability hooks", () => {
  it("fires onRequest and onResponse for each call", async () => {
    const requests: string[] = [];
    const responses: string[] = [];
    const c = cronClient();
    // Wrap with hooks via a new client instance
    const hooked = {
      ...c,
      schedules: {
        ...c.schedules,
      },
    };
    void hooked; // just verifying createCronClient accepts hooks
    const name = uniqueName();
    const hClient = (await import("../src/client.js")).createCronClient({
      url: process.env["QUEUE_TEST_URL"]!,
      onRequest: (e) => requests.push(e.command),
      onResponse: (e) => responses.push(e.command),
    });
    await hClient.schedules.upsert({
      name,
      every: "1h",
      target: { queue: "test-q", message: {} },
    });
    await hClient.schedules.delete(name);
    expect(requests).toContain("schedules.upsert");
    expect(responses).toContain("schedules.upsert");
  });
});
