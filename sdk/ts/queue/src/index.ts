import { createQueueClient, type QueueClient } from "./client.js";

let _queue: QueueClient | undefined;

/**
 * Default Queue client configured from environment variables.
 * Reads `BEYOND_QUEUE_URL` (required) and `BEYOND_QUEUE_TOKEN` (optional, defaults to `"anon"`).
 * Initialized lazily on first method call.
 */
export const queue: QueueClient = new Proxy({} as QueueClient, {
  get(_, prop) {
    _queue ??= createQueueClient();
    return (_queue as unknown as Record<string | symbol, unknown>)[prop];
  },
});

export {
  type BatchEntry,
  type BatchOptions,
  createQueueClient,
  type CreateQueueOptions,
  type JsonValue,
  type Message,
  type Queue,
  type QueueBodyType,
  type QueueClient,
  type QueueClientOptions,
  type QueueResult,
  type QueueSchemaClient,
  type QueueSchemaMap,
  type QueueStats,
  type ReceiveOptions,
  type Schema,
  type SendOptions,
} from "./client.js";
export { QueueError } from "./errors.js";
export type { Camelize } from "./utils/camelize.js";
// Re-export ApiResult as a named type — it lives in client.ts but isn't exported from there directly;
// we expose it via the public surface here.
export type { components, operations, paths } from "./types.js";
