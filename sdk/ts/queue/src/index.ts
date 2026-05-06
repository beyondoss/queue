export {
  type BatchEntry,
  type BatchOptions,
  createQueueClient,
  type CreateQueueOptions,
  type Message,
  type Queue,
  type QueueClient,
  type QueueClientOptions,
  type QueueStats,
  type ReceiveOptions,
  type SendOptions,
  type Subscription,
} from "./client.js";
export { QueueError, QueueNotFoundError } from "./errors.js";
export type { Camelize } from "./utils/camelize.js";
// Re-export ApiResult as a named type — it lives in client.ts but isn't exported from there directly;
// we expose it via the public surface here.
export type { components, operations, paths } from "./types.js";
