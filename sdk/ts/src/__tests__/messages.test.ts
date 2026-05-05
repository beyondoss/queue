import { describe, expect, it } from "vitest";
import { createQueueClient } from "../client.js";
import { getBaseUrl, queueClient, uniqueQueue } from "./harness.js";

describe("messages — send / receive / delete", () => {
  it("sendMessage returns a numeric id", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    const { data } = await q.sendMessage(name, "hello");
    expect(typeof data?.id).toBe("number");
  });

  it("receiveMessages returns the sent message", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "test message");
    const { data: messages } = await q.receiveMessages(name);
    expect(messages).toHaveLength(1);
    expect(messages![0]!.message).toBe("test message");
  });

  it("receiveMessages returns an empty array when the queue is empty", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    const { data: messages } = await q.receiveMessages(name);
    expect(messages).toHaveLength(0);
  });

  it("receiveMessages with max > 1 returns multiple messages", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendBatch(name, [{ message: "a" }, { message: "b" }, {
      message: "c",
    }]);
    const { data: messages } = await q.receiveMessages(name, { max: 3 });
    expect(messages).toHaveLength(3);
  });

  it("deleteMessage removes the message", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "to delete");
    const { data: msgs } = await q.receiveMessages(name, { visibilityTimeout: 1 });
    await q.deleteMessage(name, msgs![0]!.id);
    // Message was deleted; after vt the queue should be empty
    await new Promise<void>((r) => setTimeout(r, 1100));
    const { data: after } = await q.receiveMessages(name, { max: 1 });
    expect(after).toHaveLength(0);
  });

  it("deleteMessage on a missing id returns no error", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    const { data, error } = await q.deleteMessage(name, 999_999_999);
    expect(data).toBeUndefined();
    expect(error).toBeUndefined();
  });

  it("deleteMessages removes multiple messages and returns their ids", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendBatch(name, [{ message: "x" }, { message: "y" }]);
    const { data: received } = await q.receiveMessages(name, { max: 2 });
    const ids = received!.map((m) => m.id);
    const { data } = await q.deleteMessages(name, ids);
    expect(data?.deleted).toEqual(expect.arrayContaining(ids));
  });
});

describe("messages — sendBatch", () => {
  it("sendBatch returns one id per entry", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    const { data } = await q.sendBatch(name, [
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
    await q.createQueue(name);
    const payload = { key: "value", count: 42, nested: { ok: true } };
    await q.sendMessage(name, payload);
    const { data: msgs } = await q.receiveMessages(name);
    expect(msgs![0]!.message).toEqual(payload);
  });

  it("sends and receives a JSON array", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, [1, 2, 3]);
    const { data: msgs } = await q.receiveMessages(name);
    expect(msgs![0]!.message).toEqual([1, 2, 3]);
  });

  it("sends and receives with headers", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "body", { headers: { "x-trace-id": "abc123" } });
    const { data: msgs } = await q.receiveMessages(name);
    expect((msgs![0]!.headers as Record<string, string>)["x-trace-id"]).toBe(
      "abc123",
    );
  });

  it("message shape includes expected fields", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "shaped");
    const { data: msgs } = await q.receiveMessages(name);
    const msg = msgs![0]!;
    expect(typeof msg.id).toBe("number");
    expect(typeof msg.read_count).toBe("number");
    expect(typeof msg.enqueued_at).toBe("string");
    expect(typeof msg.visible_at).toBe("string");
  });

  it("read_count increments on each receive after vt expiry", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "reread");
    const { data: first } = await q.receiveMessages(name, {
      visibilityTimeout: 1,
    });
    expect(first![0]!.read_count).toBe(1);
    await new Promise<void>((r) => setTimeout(r, 1100));
    const { data: second } = await q.receiveMessages(name, {
      visibilityTimeout: 1,
    });
    expect(second![0]!.id).toBe(first![0]!.id);
    expect(second![0]!.read_count).toBe(2);
  });
});

describe("messages — change visibility", () => {
  it("changeVisibility returns updated id and visible_at", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "visible test");
    const { data: msgs } = await q.receiveMessages(name, { visibilityTimeout: 5 });
    const { data } = await q.changeVisibility(name, msgs![0]!.id, 60);
    expect(data?.id).toBe(msgs![0]!.id);
    expect(typeof data?.visible_at).toBe("string");
  });
});

describe("messages — delay", () => {
  it("delayed message is not immediately visible", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "delayed", { delay: 10 });
    const { data: messages } = await q.receiveMessages(name);
    expect(messages).toHaveLength(0);
  });
});

describe("messages — observability hooks", () => {
  it("fires onCommand and onResponse for each request", async () => {
    const commands: string[] = [];
    const responses: string[] = [];
    const q = createQueueClient({
      url: getBaseUrl(),
      onCommand: (e) => commands.push(e.command),
      onResponse: (e) => responses.push(e.command),
    });
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "hello");
    await q.receiveMessages(name);
    expect(commands).toContain("createQueue");
    expect(commands).toContain("sendMessage");
    expect(commands).toContain("receiveMessages");
    expect(responses).toEqual(commands);
  });

  it("onResponse includes a non-negative durationMs", async () => {
    const durations: number[] = [];
    const q = createQueueClient({
      url: getBaseUrl(),
      onResponse: (e) => durations.push(e.durationMs),
    });
    const name = uniqueQueue();
    await q.createQueue(name);
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
    await q.createQueue(name);
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
    await expect(q.listQueues()).rejects.toThrow();
  });
});

describe("messages — close", () => {
  it("close() resolves without error", async () => {
    const q = queueClient();
    await expect(q.close()).resolves.toBeUndefined();
  });
});
