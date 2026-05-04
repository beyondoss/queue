import { describe, expect, it } from "vitest";
import { createQueueClient } from "../client.js";
import { getBaseUrl, queueClient, uniqueQueue } from "./harness.js";

describe("messages — send / receive / delete", () => {
  it("sendMessage returns a numeric id", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    const result = await q.sendMessage(name, "hello");
    expect(typeof result.id).toBe("number");
  });

  it("receiveMessages returns the sent message", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "test message");
    const messages = await q.receiveMessages(name);
    expect(messages).toHaveLength(1);
    expect(messages[0]!.message).toBe("test message");
  });

  it("receiveMessages returns an empty array when the queue is empty", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    const messages = await q.receiveMessages(name);
    expect(messages).toHaveLength(0);
  });

  it("receiveMessages with max > 1 returns multiple messages", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendBatch(name, [{ message: "a" }, { message: "b" }, {
      message: "c",
    }]);
    const messages = await q.receiveMessages(name, { max: 3 });
    expect(messages).toHaveLength(3);
  });

  it("deleteMessage removes the message", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "to delete");
    const [msg] = await q.receiveMessages(name, { vt: 1 });
    await q.deleteMessage(name, msg!.id);
    // Message was deleted; after vt the queue should be empty
    await new Promise<void>((r) => setTimeout(r, 1100));
    const after = await q.receiveMessages(name, { max: 1 });
    expect(after).toHaveLength(0);
  });

  it("deleteMessage on a missing id does not throw", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await expect(q.deleteMessage(name, 999_999_999)).resolves.toBeUndefined();
  });

  it("deleteMessages removes multiple messages and returns their ids", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendBatch(name, [{ message: "x" }, { message: "y" }]);
    const received = await q.receiveMessages(name, { max: 2 });
    const ids = received.map((m) => m.id);
    const result = await q.deleteMessages(name, ids);
    expect(result.deleted).toEqual(expect.arrayContaining(ids));
  });
});

describe("messages — sendBatch", () => {
  it("sendBatch returns one id per entry", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    const result = await q.sendBatch(name, [
      { message: "one" },
      { message: "two" },
      { message: "three" },
    ]);
    expect(result.ids).toHaveLength(3);
    expect(result.ids.every((id) => typeof id === "number")).toBe(true);
  });
});

describe("messages — content types", () => {
  it("sends and receives a JSON object", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    const payload = { key: "value", count: 42, nested: { ok: true } };
    await q.sendMessage(name, payload);
    const [msg] = await q.receiveMessages(name);
    expect(msg!.message).toEqual(payload);
  });

  it("sends and receives a JSON array", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, [1, 2, 3]);
    const [msg] = await q.receiveMessages(name);
    expect(msg!.message).toEqual([1, 2, 3]);
  });

  it("sends and receives with headers", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "body", { headers: { "x-trace-id": "abc123" } });
    const [msg] = await q.receiveMessages(name);
    expect((msg!.headers as Record<string, string>)["x-trace-id"]).toBe(
      "abc123",
    );
  });

  it("message shape includes expected fields", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "shaped");
    const [msg] = await q.receiveMessages(name);
    expect(typeof msg!.id).toBe("number");
    expect(typeof msg!.read_count).toBe("number");
    expect(typeof msg!.enqueued_at).toBe("string");
    expect(typeof msg!.visible_at).toBe("string");
  });

  it("read_count increments on each receive after vt expiry", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "reread");
    const [first] = await q.receiveMessages(name, { vt: 1 });
    expect(first!.read_count).toBe(1);
    await new Promise<void>((r) => setTimeout(r, 1100));
    const [second] = await q.receiveMessages(name, { vt: 1 });
    expect(second!.id).toBe(first!.id);
    expect(second!.read_count).toBe(2);
  });
});

describe("messages — change visibility", () => {
  it("changeVisibility returns updated id and visible_at", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "visible test");
    const [msg] = await q.receiveMessages(name, { vt: 5 });
    const result = await q.changeVisibility(name, msg!.id, 60);
    expect(result.id).toBe(msg!.id);
    expect(typeof result.visible_at).toBe("string");
  });
});

describe("messages — delay", () => {
  it("delayed message is not immediately visible", async () => {
    const q = queueClient();
    const name = uniqueQueue();
    await q.createQueue(name);
    await q.sendMessage(name, "delayed", { delay: 10 });
    const messages = await q.receiveMessages(name);
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
