# @beyond.dev/queue

Send and receive messages from a [beyond-queue](https://github.com/beyond-dev/queue) instance.

## Install

```sh
npm install @beyond.dev/queue
```

## Quick Start

```ts
import { queue } from "@beyond.dev/queue";

// Send a message
const { data, error } = await queue.messages.send("jobs", {
  userId: 42,
  action: "welcome-email",
});
if (error) throw error;
console.log(data.id); // message ID

// Receive and delete
const { data: messages } = await queue.messages.receive("jobs", { max: 10 });
for (const msg of messages ?? []) {
  await process(msg.message);
  await queue.messages.delete("jobs", msg.id);
}
```

Configure via environment variables:

```sh
BEYOND_QUEUE_URL=http://localhost:9324
BEYOND_QUEUE_TOKEN=your-token   # optional, defaults to "anon"
```

Or pass options directly:

```ts
import { createQueueClient } from "@beyond.dev/queue";

const queue = createQueueClient({
  url: "http://localhost:9324",
  token: "your-token",
});
```

## Queues

```ts
// Create
await queue.queues.create("jobs");
await queue.queues.create("ordered-jobs", { fifo: true }); // FIFO ordering per group

// List
const { data } = await queue.queues.list();

// Stats
const { data } = await queue.queues.get("jobs");
// { name, queueLength, totalMessages, oldestMsgAgeSec, newestMsgAgeSec, scrapeTime }

// Delete
await queue.queues.delete("jobs");

// Purge all messages
const { data } = await queue.queues.purge("jobs");
// { deleted: 42 }
```

## Messages

### Send

```ts
// Single message
await queue.messages.send("jobs", { userId: 42 });

// With options
await queue.messages.send("jobs", { userId: 42 }, {
  delay: 60, // deliver after 60s
  headers: { source: "api" },
  groupId: "user-42", // FIFO ordering key
  asyncCommit: true, // skip WAL fsync — fast, not crash-durable
});

// Batch
await queue.messages.sendBatch("jobs", [
  { message: { userId: 1 } },
  { message: { userId: 2 }, delay: 30 },
]);
```

### Receive

```ts
const { data: messages } = await queue.messages.receive("jobs", {
  max: 10, // up to 10 messages (default: 1)
  wait: 20, // long-poll up to 20s if queue is empty
  visibilityTimeout: 60, // hide messages for 60s (default: 30)
  fifo: true, // FIFO order within group (FIFO queues only)
});

// Message shape:
// { id, message, headers, readCount, enqueuedAt, visibleAt }
```

### Delete

```ts
// Single
await queue.messages.delete("jobs", msg.id);

// Batch
const { data } = await queue.messages.deleteBatch("jobs", [1, 2, 3]);
// { deleted: [1, 2, 3] }
```

### Change visibility

Extend or shrink the visibility timeout on an in-flight message:

```ts
const { data } = await queue.messages.changeVisibility("jobs", msg.id, 120);
// { id, visibleAt }
```

## Typed messages

Pass a schema map to get typed `send` and `receive` calls. Any object with a `.parse()` method works — Zod, Valibot, or a plain validator.

```ts
import { createQueueClient } from "@beyond.dev/queue";
import { z } from "zod";

const queue = createQueueClient({
  url: process.env.BEYOND_QUEUE_URL,
  schema: {
    jobs: z.object({ userId: z.number(), action: z.string() }),
  },
});

// Type-safe send — TypeScript error if payload doesn't match
await queue.messages.send("jobs", { userId: 42, action: "welcome-email" });

// Receive returns typed messages
const { data: messages } = await queue.messages.receive("jobs");
messages?.[0].message.userId; // number
```

## Error handling

All methods return `{ data, error, response }` — errors are never thrown.

```ts
const { data, error } = await queue.messages.send("jobs", payload);
if (error) {
  console.error(error.code); // "queue_not_found", "bad_request", ...
  console.error(error.status); // 404, 400, ...
  console.error(error.hint); // actionable guidance, if available
}
```

## Client options

```ts
createQueueClient({
  url: "http://...",
  token: "...",
  timeout: 5000, // per-request timeout in ms
  retries: 2, // retry on 5xx (default: 2)
  fetch: customFetch,

  onRequest: ({ command }) => {/* called before each request */},
  onResponse: ({ command, durationMs }) => {/* called after each response */},
});
```

## Default client

`import { queue } from "@beyond.dev/queue"` returns a lazy singleton initialized from `BEYOND_QUEUE_URL` and `BEYOND_QUEUE_TOKEN` on first use. Use `createQueueClient()` when you need multiple clients or runtime configuration.
