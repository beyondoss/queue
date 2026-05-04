import { describe, expect, it } from "vitest";
import { QueueError } from "../errors.js";
import { queueClient, uniqueQueue } from "./harness.js";

describe("queue management — create / list / get / delete", () => {
  it("createQueue returns a queue_url containing the queue name", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    const result = await q.createQueue(name);
    expect(result.queue_url).toContain(name);
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
    const queues = await q.listQueues();
    expect(queues.some((qu) => qu.name === name)).toBe(true);
  });

  it("listQueues returns queue objects with expected shape", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    const queues = await q.listQueues();
    const found = queues.find((qu) => qu.name === name);
    expect(found).toBeDefined();
    expect(typeof found!.is_partitioned).toBe("boolean");
    expect(typeof found!.created_at).toBe("string");
  });

  it("getQueue returns stats for an existing queue", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    const stats = await q.getQueueStats(name);
    expect(stats.queue_length).toBeGreaterThanOrEqual(0);
    expect(stats.total_messages).toBeGreaterThanOrEqual(0);
    expect(typeof stats.scrape_time).toBe("string");
  });

  it("getQueue throws QueueError for a missing queue", async () => {
    const q = queueClient();
    // queue.metrics() runs dynamic SQL against the queue table; if it doesn't
    // exist the DB raises an error, so the server returns 500, not 404.
    await expect(q.getQueueStats(uniqueQueue())).rejects.toBeInstanceOf(
      QueueError,
    );
  });

  it("deleteQueue removes the queue from listQueues", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.deleteQueue(name);
    const queues = await q.listQueues();
    expect(queues.some((qu) => qu.name === name)).toBe(false);
  });

  it("deleteQueue on a missing queue does not throw", async () => {
    const q = queueClient();
    await expect(q.deleteQueue(uniqueQueue())).resolves.toBeUndefined();
  });
});

describe("queue management — purge", () => {
  it("purgeQueue returns a deleted count", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "a");
    await q.sendMessage(name, "b");
    const result = await q.purgeQueue(name);
    expect(typeof result.deleted).toBe("number");
  });

  it("purgeQueue empties the queue", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "x");
    await q.purgeQueue(name);
    const messages = await q.receiveMessages(name, { max: 10 });
    expect(messages).toHaveLength(0);
  });
});
