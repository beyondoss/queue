import { describe, expect, it } from "vitest";
import { createQueueClient } from "../src/client.js";
import { getBaseUrl, queueClient, uniqueQueue } from "./harness.js";

describe("messages — send / receive / delete", () => {
  it("send returns a numeric id", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    const { data } = await q.messages.send(name, "hello");
    expect(typeof data?.id).toBe("number");
  });

  it("receive returns the sent message", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.messages.send(name, "test message");
    const { data: messages } = await q.messages.receive(name);
    expect(messages).toHaveLength(1);
    expect(messages![0]!.message).toBe("test message");
  });

  it("receive returns an empty array when the queue is empty", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    const { data: messages } = await q.messages.receive(name);
    expect(messages).toHaveLength(0);
  });

  it("receive with max > 1 returns multiple messages", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.messages.sendBatch(name, [
      { message: "a" },
      { message: "b" },
      { message: "c" },
    ]);
    const { data: messages } = await q.messages.receive(name, { max: 3 });
    expect(messages).toHaveLength(3);
  });

  it("delete removes the message", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.messages.send(name, "to delete");
    const { data: msgs } = await q.messages.receive(name, {
      visibilityTimeout: 1,
    });
    await q.messages.delete(name, msgs![0]!.id);
    // Message was deleted; after vt the queue should be empty
    await new Promise<void>((r) => setTimeout(r, 1100));
    const { data: after } = await q.messages.receive(name, { max: 1 });
    expect(after).toHaveLength(0);
  });

  it("delete on a missing id returns no error", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    const { data, error } = await q.messages.delete(name, 999_999_999);
    expect(data).toBeUndefined();
    expect(error).toBeUndefined();
  });

  it("deleteBatch removes multiple messages and returns their ids", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.messages.sendBatch(name, [{ message: "x" }, { message: "y" }]);
    const { data: received } = await q.messages.receive(name, { max: 2 });
    const ids = received!.map((m) => m.id);
    const { data } = await q.messages.deleteBatch(name, ids);
    expect(data?.deleted).toEqual(expect.arrayContaining(ids));
  });
});

describe("messages — sendBatch", () => {
  it("sendBatch returns one id per entry", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    const { data } = await q.messages.sendBatch(name, [
      { message: "one" },
      { message: "two" },
      { message: "three" },
    ]);
    expect(data?.ids).toHaveLength(3);
    expect(data?.ids.every((id) => typeof id === "number")).toBe(true);
  });
});

describe("messages — content types", () => {
  it("sends and receives a JSON object", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    const payload = { key: "value", count: 42, nested: { ok: true } };
    await q.messages.send(name, payload);
    const { data: msgs } = await q.messages.receive(name);
    expect(msgs![0]!.message).toEqual(payload);
  });

  it("sends and receives a JSON array", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.messages.send(name, [1, 2, 3]);
    const { data: msgs } = await q.messages.receive(name);
    expect(msgs![0]!.message).toEqual([1, 2, 3]);
  });

  it("sends and receives with headers", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.messages.send(name, "body", {
      headers: { "x-trace-id": "abc123" },
    });
    const { data: msgs } = await q.messages.receive(name);
    expect(
      (msgs![0]!.headers as Record<string, string>)["x-trace-id"],
    ).toBe("abc123");
  });

  it("message shape includes expected fields", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.messages.send(name, "shaped");
    const { data: msgs } = await q.messages.receive(name);
    const msg = msgs![0]!;
    expect(typeof msg.id).toBe("number");
    expect(typeof msg.readCount).toBe("number");
    expect(typeof msg.enqueuedAt).toBe("string");
    expect(typeof msg.visibleAt).toBe("string");
  });

  it("readCount increments on each receive after vt expiry", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.messages.send(name, "reread");
    const { data: first } = await q.messages.receive(name, {
      visibilityTimeout: 1,
    });
    expect(first![0]!.readCount).toBe(1);
    await new Promise<void>((r) => setTimeout(r, 1100));
    const { data: second } = await q.messages.receive(name, {
      visibilityTimeout: 1,
    });
    expect(second![0]!.id).toBe(first![0]!.id);
    expect(second![0]!.readCount).toBe(2);
  });
});

describe("messages — changeVisibility", () => {
  it("changeVisibility returns updated id and visibleAt", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.messages.send(name, "visible test");
    const { data: msgs } = await q.messages.receive(name, {
      visibilityTimeout: 5,
    });
    const { data } = await q.messages.changeVisibility(
      name,
      msgs![0]!.id,
      60,
    );
    expect(data?.id).toBe(msgs![0]!.id);
    expect(typeof data?.visibleAt).toBe("string");
  });
});

describe("messages — delay", () => {
  it("delayed message is not immediately visible", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.messages.send(name, "delayed", { delay: 10 });
    const { data: messages } = await q.messages.receive(name);
    expect(messages).toHaveLength(0);
  });
});

describe("messages — observability hooks", () => {
  it("fires onRequest and onResponse for each request", async () => {
    const commands: string[] = [];
    const responses: string[] = [];
    const q = createQueueClient({
      url: getBaseUrl(),
      onRequest: (e) => commands.push(e.command),
      onResponse: (e) => responses.push(e.command),
    });
    const name = uniqueQueue();
    await q.queues.create(name);
    await q.messages.send(name, "hello");
    await q.messages.receive(name);
    expect(commands).toContain("queues.create");
    expect(commands).toContain("messages.send");
    expect(commands).toContain("messages.receive");
    expect(responses).toEqual(commands);
  });

  it("onResponse includes a non-negative durationMs", async () => {
    const durations: number[] = [];
    const q = createQueueClient({
      url: getBaseUrl(),
      onResponse: (e) => durations.push(e.durationMs),
    });
    const name = uniqueQueue();
    await q.queues.create(name);
    expect(durations[0]).toBeGreaterThanOrEqual(0);
  });
});

describe("messages — retry on 5xx", () => {
  it("retries on 5xx and succeeds on the subsequent attempt", async () => {
    let attempt = 0;
    const name = uniqueQueue();
    const realFetch = globalThis.fetch;
    const mockFetch: typeof fetch = async (input, init) => {
      if (attempt++ === 0) {
        return new Response("Service Unavailable", { status: 503 });
      }
      return realFetch(input, init);
    };
    const q = createQueueClient({
      url: getBaseUrl(),
      fetch: mockFetch,
      retries: 2,
    });
    await q.queues.create(name);
    expect(attempt).toBeGreaterThan(1);
  });
});

describe("messages — timeout", () => {
  it("aborts the request when the timeout elapses", async () => {
    const hangingFetch: typeof fetch = (_input, init) =>
      new Promise((_resolve, reject) => {
        init?.signal?.addEventListener(
          "abort",
          () => reject(new DOMException("aborted", "AbortError")),
        );
      });
    const q = createQueueClient({
      url: getBaseUrl(),
      fetch: hangingFetch,
      timeout: 50,
      retries: 0,
    });
    await expect(q.queues.list()).rejects.toThrow();
  });
});

describe("messages — close", () => {
  it("close() resolves without error", async () => {
    const q = queueClient();
    await expect(q.close()).resolves.toBeUndefined();
  });
});
