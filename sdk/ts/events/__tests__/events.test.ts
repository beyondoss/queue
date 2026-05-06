import { describe, expect, it } from "vitest";
import { createEventClient } from "../src/client.js";
import { eventClient, getBaseUrl, uniqueName } from "./harness.js";

// Helper: create a queue via the native REST API so events tests don't depend
// on @beyond.dev/queue being a sibling package reference.
async function createQueue(
  baseUrl: string,
  name: string,
): Promise<void> {
  const res = await fetch(`${baseUrl}/v1/queues`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: "Bearer anon",
    },
    body: JSON.stringify({ name, fifo: false }),
  });
  if (!res.ok && res.status !== 409) {
    throw new Error(`Failed to create queue ${name}: ${res.status}`);
  }
}

async function receiveMessages(
  baseUrl: string,
  queueName: string,
  max = 1,
): Promise<unknown[]> {
  const res = await fetch(
    `${baseUrl}/v1/queues/${encodeURIComponent(queueName)}/messages?max=${max}`,
    { headers: { Authorization: "Bearer anon" } },
  );
  return res.json() as Promise<unknown[]>;
}

// ── publish ───────────────────────────────────────────────────────────────────

describe("events — publish", () => {
  it("publish with no subscribers returns queuesMatched = 0", async () => {
    const e = eventClient();
    const { data } = await e.publish(
      `no.subscribers.${uniqueName()}`,
      "message",
    );
    expect(data?.queuesMatched).toBe(0);
  });

  it("publish routes message to a subscribed queue", async () => {
    const baseUrl = getBaseUrl();
    const e = eventClient();
    const qName = uniqueName("q");
    const pattern = `events.${qName}`;
    await createQueue(baseUrl, qName);
    await e.subscriptions.create(pattern, { type: "queue", name: qName });

    const { data } = await e.publish(pattern, "hello");
    expect(data?.queuesMatched).toBe(1);

    const messages = await receiveMessages(baseUrl, qName);
    expect(messages).toHaveLength(1);
  });

  it("publish routes to multiple subscribed queues", async () => {
    const baseUrl = getBaseUrl();
    const e = eventClient();
    const suffix = uniqueName();
    const qa = `wa_${suffix}`;
    const qb = `wb_${suffix}`;
    const pattern = `multi.${suffix}`;
    await createQueue(baseUrl, qa);
    await createQueue(baseUrl, qb);
    await e.subscriptions.create(pattern, { type: "queue", name: qa });
    await e.subscriptions.create(pattern, { type: "queue", name: qb });

    const { data } = await e.publish(pattern, "broadcast");
    expect(data?.queuesMatched).toBe(2);
  });

  it("publish with headers delivers them to the queue", async () => {
    const baseUrl = getBaseUrl();
    const e = eventClient();
    const qName = uniqueName("q");
    const pattern = `hdr.${qName}`;
    await createQueue(baseUrl, qName);
    await e.subscriptions.create(pattern, { type: "queue", name: qName });

    await e.publish(pattern, "with-headers", {
      headers: { "x-event-type": "test" },
    });
    const msgs = await receiveMessages(baseUrl, qName);
    const msg = msgs[0] as { headers: Record<string, string> };
    expect(msg.headers["x-event-type"]).toBe("test");
  });
});

// ── subscriptions ─────────────────────────────────────────────────────────────

describe("events — subscriptions", () => {
  it("subscriptions.create returns a Subscription object", async () => {
    const baseUrl = getBaseUrl();
    const e = eventClient();
    const qName = uniqueName("q");
    const pattern = `sub.${qName}`;
    await createQueue(baseUrl, qName);
    const { data: sub } = await e.subscriptions.create(pattern, {
      type: "queue",
      name: qName,
    });
    expect(sub?.pattern).toBe(pattern);
    expect(typeof sub?.boundAt).toBe("string");
  });

  it("subscriptions.create is idempotent", async () => {
    const baseUrl = getBaseUrl();
    const e = eventClient();
    const qName = uniqueName("q");
    const pattern = `idem.${qName}`;
    await createQueue(baseUrl, qName);
    await e.subscriptions.create(pattern, { type: "queue", name: qName });
    await expect(
      e.subscriptions.create(pattern, { type: "queue", name: qName }),
    ).resolves.toBeDefined();
  });

  it("subscriptions.list returns subscriptions for the pattern", async () => {
    const baseUrl = getBaseUrl();
    const e = eventClient();
    const qName = uniqueName("q");
    const pattern = `list.${qName}`;
    await createQueue(baseUrl, qName);
    await e.subscriptions.create(pattern, { type: "queue", name: qName });
    const { data: subs } = await e.subscriptions.list(pattern);
    expect(subs?.some((s) => s.pattern === pattern)).toBe(true);
  });

  it("subscriptions.delete removes the binding", async () => {
    const baseUrl = getBaseUrl();
    const e = eventClient();
    const qName = uniqueName("q");
    const pattern = `unsub.${qName}`;
    await createQueue(baseUrl, qName);
    const { data: sub } = await e.subscriptions.create(pattern, {
      type: "queue",
      name: qName,
    });
    await e.subscriptions.delete(sub!.id);
    const { data: subs } = await e.subscriptions.list(pattern);
    expect(subs?.some((s) => s.id === sub!.id)).toBe(false);
  });

  it("subscriptions.delete on a missing binding returns no error", async () => {
    const e = eventClient();
    const { data, error } = await e.subscriptions.delete(999_999_999);
    expect(data).toBeUndefined();
    expect(error).toBeUndefined();
  });

  it("after delete, publish no longer routes to the queue", async () => {
    const baseUrl = getBaseUrl();
    const e = eventClient();
    const qName = uniqueName("q");
    const pattern = `ghost.${qName}`;
    await createQueue(baseUrl, qName);
    const { data: sub } = await e.subscriptions.create(pattern, {
      type: "queue",
      name: qName,
    });
    await e.subscriptions.delete(sub!.id);
    const { data } = await e.publish(pattern, "ghost message");
    expect(data?.queuesMatched).toBe(0);
  });
});

// ── observability hooks ───────────────────────────────────────────────────────

describe("events — observability hooks", () => {
  it("fires onRequest and onResponse for each request", async () => {
    const commands: string[] = [];
    const responses: string[] = [];
    const e = createEventClient({
      url: getBaseUrl(),
      onRequest: (ev) => commands.push(ev.command),
      onResponse: (ev) => responses.push(ev.command),
    });
    await e.publish(`noop.${uniqueName()}`, "ping");
    expect(commands).toContain("publish");
    expect(responses).toEqual(commands);
  });

  it("onResponse includes a non-negative durationMs", async () => {
    const durations: number[] = [];
    const e = createEventClient({
      url: getBaseUrl(),
      onResponse: (ev) => durations.push(ev.durationMs),
    });
    await e.publish(`noop.${uniqueName()}`, "ping");
    expect(durations[0]).toBeGreaterThanOrEqual(0);
  });
});

// ── close ─────────────────────────────────────────────────────────────────────

describe("events — close", () => {
  it("close() resolves without error", async () => {
    const e = eventClient();
    await expect(e.close()).resolves.toBeUndefined();
  });
});
