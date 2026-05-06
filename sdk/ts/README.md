# @beyond.dev/queue

Send messages, receive them, delete them. Fan-out routing included.

## Install

```sh
npm install @beyond.dev/queue
```

Requires Node.js 18+.

## Quick Start

```ts
import { createQueueClient } from "@beyond.dev/queue";

const q = createQueueClient({ url: "http://localhost:9324" });

await q.createQueue("jobs");
await q.sendMessage("jobs", { task: "resize-image", id: 42 });

const [msg] = await q.receiveMessages("jobs");
console.log(msg.message); // { task: "resize-image", id: 42 }
await q.deleteMessage("jobs", msg.id);
```

## API

### Client

```ts
createQueueClient(opts: QueueClientOptions): QueueClient
```

| Option       | Type       | Default         | Description                              |
| ------------ | ---------- | --------------- | ---------------------------------------- |
| `url`        | `string`   | —               | Base URL of the beyond-queue server      |
| `auth`       | `string`   | `"Bearer anon"` | Authorization header value               |
| `fetch`      | `function` | global fetch    | Custom fetch (for pooling or test mocks) |
| `timeout`    | `number`   | —               | Per-request timeout in milliseconds      |
| `retries`    | `number`   | `2`             | Max retries on transient 5xx failures    |
| `onRequest`  | `function` | —               | Called before each request               |
| `onResponse` | `function` | —               | Called after each response with duration |

### Queues

```ts
q.createQueue(name: string, opts?: CreateQueueOptions): Promise<{ queue_url: string }>
q.listQueues(): Promise<Queue[]>
q.getQueue(name: string): Promise<QueueStats>        // throws QueueNotFoundError if missing
q.deleteQueue(name: string): Promise<void>           // no-op if already gone
q.purgeQueue(name: string): Promise<{ deleted: number }>
```

`CreateQueueOptions`:

| Option        | Type      | Description                                     |
| ------------- | --------- | ----------------------------------------------- |
| `partitioned` | `boolean` | Partition the queue for higher write throughput |
| `unlogged`    | `boolean` | Skip WAL for faster writes; data lost on crash  |

### Messages

```ts
q.sendMessage(queue: string, message: JsonValue, opts?: SendOptions): Promise<{ id: number }>
q.sendBatch(queue: string, entries: BatchEntry[], opts?: { async_commit?: boolean }): Promise<{ ids: number[] }>
q.receiveMessages(queue: string, opts?: ReceiveOptions): Promise<Message[]>
q.deleteMessage(queue: string, id: number): Promise<void>
q.deleteMessages(queue: string, ids: number[]): Promise<{ deleted: number[] }>
q.changeVisibility(queue: string, id: number, vt: number): Promise<{ id: number; visible_at: string }>
```

`SendOptions`:

| Option         | Type        | Default | Description                                     |
| -------------- | ----------- | ------- | ----------------------------------------------- |
| `headers`      | `JsonValue` | —       | Custom message headers                          |
| `delay`        | `number`    | `0`     | Seconds before message becomes visible          |
| `group_id`     | `string`    | —       | FIFO group ID for ordered processing            |
| `async_commit` | `boolean`   | `false` | Skip WAL fsync; higher throughput, less durable |

`ReceiveOptions`:

| Option | Type      | Default | Description                    |
| ------ | --------- | ------- | ------------------------------ |
| `max`  | `number`  | `1`     | Max messages to return         |
| `wait` | `number`  | `0`     | Long-poll wait time in seconds |
| `vt`   | `number`  | `30`    | Visibility timeout in seconds  |
| `fifo` | `boolean` | `false` | FIFO ordering mode             |

### Topics

Publish to a routing key; any queue subscribed to a matching pattern receives it.

```ts
q.publish(routingKey: string, message: JsonValue, opts?: PublishOptions): Promise<{ queues_matched: number }>
q.subscribe(pattern: string, queueName: string): Promise<Subscription>
q.unsubscribe(pattern: string, queueName: string): Promise<void>
q.listTopicSubscriptions(pattern: string): Promise<Subscription[]>
q.listQueueSubscriptions(queueName: string): Promise<Subscription[]>
```

## Examples

### Batch send

```ts
const { ids } = await q.sendBatch("jobs", [
  { message: { task: "a" } },
  { message: { task: "b" }, delay: 10 },
  { message: { task: "c" }, headers: { priority: "high" } },
]);
```

### Long-poll receive

The server blocks until messages arrive or the timeout expires — no client-side retry loop needed.

```ts
const messages = await q.receiveMessages("jobs", {
  max: 10,
  wait: 20, // block up to 20s; wakes immediately when a message commits
  vt: 60,
});
```

### Fan-out routing

```ts
await q.createQueue("payments");
await q.subscribe("payments.*", "payments");

await q.publish("payments.completed", { orderId: 99 });

const [msg] = await q.receiveMessages("payments");
await q.deleteMessage("payments", msg.id);
```

### Observability

```ts
const q = createQueueClient({
  url: "http://localhost:9324",
  onRequest: (e) => logger.debug({ cmd: e.command }),
  onResponse: (e) =>
    metrics.histogram("queue.latency", e.durationMs, { cmd: e.command }),
});
```

### Error handling

```ts
import { QueueNotFoundError } from "@beyond.dev/queue";

try {
  await q.getQueue("missing");
} catch (err) {
  if (err instanceof QueueNotFoundError) {
    // err.queue holds the queue name
  }
}
```

### Lifecycle

```ts
await q.close(); // release underlying connections
```
