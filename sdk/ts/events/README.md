# @beyond.dev/events

Publish events to routing keys and subscribe queues or webhooks to glob patterns.

## Quick start

```sh
npm install @beyond.dev/events
```

Set `BEYOND_EVENTS_URL` to your beyond-queue server, then use the default client:

```ts
import { events } from "@beyond.dev/events";

// Publish to a routing key
const { data, error } = await events.publish("payments.created", {
  orderId: "123",
  amount: 99.99,
});

if (error) {
  console.error(error.code, error.message);
} else {
  console.log(`Routed to ${data.queuesMatched} queues`);
}

// Subscribe a queue to a glob pattern
await events.subscriptions.create("payments.*", {
  type: "queue",
  name: "payment-events",
});
```

## Install

```sh
npm install @beyond.dev/events
```

**Environment variables:**

| Variable              | Required | Description                          |
| --------------------- | -------- | ------------------------------------ |
| `BEYOND_EVENTS_URL`   | Yes      | Base URL of your beyond-queue server |
| `BEYOND_EVENTS_TOKEN` | No       | Bearer token. Defaults to `"anon"`.  |

## Create a client

For explicit configuration or multiple instances:

```ts
import { createEventClient } from "@beyond.dev/events";

const client = createEventClient({
  url: "http://localhost:9324",
  token: "my-token",
  timeout: 5000,
  retries: 2,
});
```

| Option       | Type       | Default             | Description                           |
| ------------ | ---------- | ------------------- | ------------------------------------- |
| `url`        | `string`   | `BEYOND_EVENTS_URL` | Base URL of the beyond-queue server   |
| `token`      | `string`   | `"anon"`            | Bearer token                          |
| `fetch`      | `function` | `globalThis.fetch`  | Custom fetch (test mocking, pooling)  |
| `timeout`    | `number`   | —                   | Per-request timeout in milliseconds   |
| `retries`    | `number`   | `2`                 | Max retries on transient 5xx failures |
| `onRequest`  | `function` | —                   | Hook called before each request       |
| `onResponse` | `function` | —                   | Hook called after each response       |

## Publish

```ts
const { data, error } = await client.publish(
  "user.created",
  { userId: "u_123" },
  { delay: 30, headers: { "x-trace-id": "abc" } },
);
```

Every subscription whose pattern matches `"user.created"` receives a copy.

`data.messages` contains one entry per matched queue:

```ts
[{ queueName: "user-events", msgId: 42 }];
```

**Options:**

| Option    | Type                     | Description                              |
| --------- | ------------------------ | ---------------------------------------- |
| `delay`   | `number`                 | Delivery delay in seconds. Default: `0`. |
| `headers` | `Record<string, string>` | Metadata attached to each enqueued copy. |

## Subscriptions

### Subscribe a queue

```ts
await client.subscriptions.create("payments.*", {
  type: "queue",
  name: "payment-events",
});
```

### Subscribe a webhook

```ts
await client.subscriptions.create("user.*", {
  type: "https",
  endpoint: "https://webhook.example.com/events",
  envelope: true, // wrap in SNS-compatible envelope
});
```

### List subscriptions

```ts
// By pattern
const { data: subs } = await client.subscriptions.list("payments.*");

// By target queue
const { data: subs } = await client.subscriptions.listByQueue("payment-events");
```

### Delete a subscription

```ts
await client.subscriptions.delete(sub.id); // idempotent
```

## Schema-aware client

Pass a `schema` map to get typed payloads at compile time. Any Zod-compatible schema works.

```ts
import { createEventClient } from "@beyond.dev/events";
import { z } from "zod";

const client = createEventClient({
  url: "http://localhost:9324",
  schema: {
    "user.*": z.object({ userId: z.string() }),
    "order.placed": z.object({ orderId: z.string(), amount: z.number() }),
  },
});

// Payload type is inferred from the schema
await client.publish("user.created", { userId: "u_123" });
await client.publish("order.placed", { orderId: "ord-789", amount: 50.0 });

// Unmatched keys fall back to JsonValue
await client.publish("unknown.event", { anything: "goes" });
```

Glob patterns in schema keys (`user.*`) are matched against routing keys at the type level.

## Observability

```ts
const client = createEventClient({
  url: "http://localhost:9324",
  onRequest: ({ command }) => console.log(`→ ${command}`),
  onResponse: ({ command, durationMs }) =>
    console.log(`← ${command} ${durationMs}ms`),
});
```

## Error handling

Operations never throw. Every method returns `EventResult<T>`:

```ts
const { data, error } = await client.publish("key", payload);

if (error) {
  console.error(error.code); // machine-readable code, e.g. "queue_not_found"
  console.error(error.message); // human-readable description
  console.error(error.status); // HTTP status
  console.error(error.hint); // optional actionable guidance
}
```
