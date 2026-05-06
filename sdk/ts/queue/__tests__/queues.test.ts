import { describe, expect, it } from "vitest";
import { queueClient, uniqueQueue } from "./harness.js";

describe("queue management — create / list / get / delete", () => {
  it("create returns a queueUrl containing the queue name", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    const { data } = await q.queues.create(name);
    expect(data?.queueUrl).toContain(name);
  });

  it("create is idempotent", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await expect(q.queues.create(name)).resolves.toBeDefined();
  });

  it("list includes a queue after creation", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    const { data: queues } = await q.queues.list();
    expect(queues?.some((qu) => qu.name === name)).toBe(true);
  });

  it("list returns queue objects with expected shape", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    const { data: queues } = await q.queues.list();
    const found = queues?.find((qu) => qu.name === name);
    expect(found).toBeDefined();
    expect(typeof found!.isPartitioned).toBe("boolean");
    expect(typeof found!.createdAt).toBe("string");
  });

  it("get returns stats for an existing queue", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    const { data: stats } = await q.queues.get(name);
    expect(stats?.queueLength).toBeGreaterThanOrEqual(0);
    expect(stats?.totalMessages).toBeGreaterThanOrEqual(0);
    expect(typeof stats?.scrapeTime).toBe("string");
  });

  it("get returns an error for a missing queue", async () => {
    const q = queueClient();
    // queue.metrics() runs dynamic SQL against the queue table; if it doesn't
    // exist the DB raises an error, so the server returns 500, not 404.
    const { error } = await q.queues.get(uniqueQueue());
    expect(error).toBeDefined();
  });

  it("delete removes the queue from list", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.queues.delete(name);
    const { data: queues } = await q.queues.list();
    expect(queues?.some((qu) => qu.name === name)).toBe(false);
  });

  it("delete on a missing queue returns no error", async () => {
    const q = queueClient();
    const { data, error } = await q.queues.delete(uniqueQueue());
    expect(data).toBeUndefined();
    expect(error).toBeUndefined();
  });
});

describe("queue management — purge", () => {
  it("purge returns a deleted count", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.messages.send(name, "a");
    await q.messages.send(name, "b");
    const { data } = await q.queues.purge(name);
    expect(typeof data?.deleted).toBe("number");
  });

  it("purge empties the queue", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.messages.send(name, "x");
    await q.queues.purge(name);
    const { data: messages } = await q.messages.receive(name, { max: 10 });
    expect(messages).toHaveLength(0);
  });
});
