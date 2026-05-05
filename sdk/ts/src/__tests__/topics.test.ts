import { describe, expect, it } from "vitest";
import { queueClient, uniqueQueue } from "./harness.js";

describe("topics — publish", () => {
  it("publish with no subscribers returns queues_matched = 0", async () => {
    const q = queueClient();
    const { data } = await q.publish(
      `no.subscribers.${uniqueQueue()}`,
      "message",
    );
    expect(data?.queues_matched).toBe(0);
  });

  it("publish routes message to a subscribed queue", async () => {
    const q = queueClient();
    const qName = uniqueQueue();
    const pattern = `events.${qName}`;
    await q.createQueue(qName);
    await q.subscribe(pattern, qName);

    const { data } = await q.publish(pattern, "hello");
    expect(data?.queues_matched).toBe(1);

    const { data: messages } = await q.receiveMessages(qName);
    expect(messages).toHaveLength(1);
    expect(messages![0]!.message).toBe("hello");
  });

  it("publish routes to multiple subscribed queues", async () => {
    const q = queueClient();
    const suffix = uniqueQueue();
    const qa = `wa_${suffix}`;
    const qb = `wb_${suffix}`;
    const pattern = `multi.${suffix}`;
    await q.createQueue(qa);
    await q.createQueue(qb);
    await q.subscribe(pattern, qa);
    await q.subscribe(pattern, qb);

    const { data } = await q.publish(pattern, "broadcast");
    expect(data?.queues_matched).toBe(2);
  });

  it("publish with headers delivers them to the queue", async () => {
    const q = queueClient();
    const qName = uniqueQueue();
    const pattern = `hdr.${qName}`;
    await q.createQueue(qName);
    await q.subscribe(pattern, qName);

    await q.publish(pattern, "with-headers", {
      headers: { "x-event-type": "test" },
    });
    const { data: msgs } = await q.receiveMessages(qName);
    expect((msgs![0]!.headers as Record<string, string>)["x-event-type"]).toBe(
      "test",
    );
  });
});

describe("topics — subscriptions", () => {
  it("subscribe returns a Subscription object", async () => {
    const q = queueClient();
    const qName = uniqueQueue();
    const pattern = `sub.${qName}`;
    await q.createQueue(qName);
    const { data: sub } = await q.subscribe(pattern, qName);
    expect(sub?.pattern).toBe(pattern);
    expect(sub?.queue_name).toBe(qName);
    expect(typeof sub?.bound_at).toBe("string");
  });

  it("subscribe is idempotent", async () => {
    const q = queueClient();
    const qName = uniqueQueue();
    const pattern = `idem.${qName}`;
    await q.createQueue(qName);
    await q.subscribe(pattern, qName);
    await expect(q.subscribe(pattern, qName)).resolves.toBeDefined();
  });

  it("listTopicSubscriptions returns subscriptions for the pattern", async () => {
    const q = queueClient();
    const qName = uniqueQueue();
    const pattern = `list.${qName}`;
    await q.createQueue(qName);
    await q.subscribe(pattern, qName);
    const { data: subs } = await q.listTopicSubscriptions(pattern);
    expect(subs?.some((s) => s.queue_name === qName && s.pattern === pattern))
      .toBe(true);
  });

  it("listQueueSubscriptions returns subscriptions for the queue", async () => {
    const q = queueClient();
    const qName = uniqueQueue();
    const pattern = `qsub.${qName}`;
    await q.createQueue(qName);
    await q.subscribe(pattern, qName);
    const { data: subs } = await q.listQueueSubscriptions(qName);
    expect(subs?.some((s) => s.queue_name === qName)).toBe(true);
  });

  it("unsubscribe removes the binding", async () => {
    const q = queueClient();
    const qName = uniqueQueue();
    const pattern = `unsub.${qName}`;
    await q.createQueue(qName);
    const { data: sub } = await q.subscribe(pattern, qName);
    await q.unsubscribe(sub!.id);
    const { data: subs } = await q.listTopicSubscriptions(pattern);
    expect(subs?.some((s) => s.queue_name === qName)).toBe(false);
  });

  it("unsubscribe on a missing binding returns no error", async () => {
    const q = queueClient();
    const { data, error } = await q.unsubscribe(999_999_999);
    expect(data).toBeUndefined();
    expect(error).toBeUndefined();
  });

  it("after unsubscribe, publish no longer routes to the queue", async () => {
    const q = queueClient();
    const qName = uniqueQueue();
    const pattern = `ghost.${qName}`;
    await q.createQueue(qName);
    const { data: sub } = await q.subscribe(pattern, qName);
    await q.unsubscribe(sub!.id);
    const { data } = await q.publish(pattern, "ghost message");
    expect(data?.queues_matched).toBe(0);
  });
});
