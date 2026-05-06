import { describe, expect, it } from "vitest";
import { queueClient, uniqueQueue } from "./harness.js";

describe("queue management — create / list / get / delete", () => {
  it("createQueue returns a queueUrl containing the queue name", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    const { data } = await q.createQueue(name);
    expect(data?.queueUrl).toContain(name);
  });

  it("createQueue is idempotent", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await expect(q.createQueue(name)).resolves.toBeDefined();
  });

  it("listQueues includes a queue after creation", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    const { data: queues } = await q.listQueues();
    expect(queues?.some((qu) => qu.name === name)).toBe(true);
  });

  it("listQueues returns queue objects with expected shape", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    const { data: queues } = await q.listQueues();
    const found = queues?.find((qu) => qu.name === name);
    expect(found).toBeDefined();
    expect(typeof found!.isPartitioned).toBe("boolean");
    expect(typeof found!.createdAt).toBe("string");
  });

  it("getQueue returns stats for an existing queue", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    const { data: stats } = await q.getQueueStats(name);
    expect(stats?.queueLength).toBeGreaterThanOrEqual(0);
    expect(stats?.totalMessages).toBeGreaterThanOrEqual(0);
    expect(typeof stats?.scrapeTime).toBe("string");
  });

  it("getQueue returns an error for a missing queue", async () => {
    const q = queueClient();
    // queue.metrics() runs dynamic SQL against the queue table; if it doesn't
    // exist the DB raises an error, so the server returns 500, not 404.
    const { error } = await q.getQueueStats(uniqueQueue());
    expect(error).toBeDefined();
  });

  it("deleteQueue removes the queue from listQueues", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.deleteQueue(name);
    const { data: queues } = await q.listQueues();
    expect(queues?.some((qu) => qu.name === name)).toBe(false);
  });

  it("deleteQueue on a missing queue returns no error", async () => {
    const q = queueClient();
    const { data, error } = await q.deleteQueue(uniqueQueue());
    expect(data).toBeUndefined();
    expect(error).toBeUndefined();
  });
});

describe("queue management — purge", () => {
  it("purgeQueue returns a deleted count", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "a");
    await q.sendMessage(name, "b");
    const { data } = await q.purgeQueue(name);
    expect(typeof data?.deleted).toBe("number");
  });

  it("purgeQueue empties the queue", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "x");
    await q.purgeQueue(name);
    const { data: messages } = await q.receiveMessages(name, { max: 10 });
    expect(messages).toHaveLength(0);
  });
});
