import { createHttpQueueClient } from "./http.js";
import type {
  BatchEntry,
  CreateQueueOptions,
  JsonValue,
  Message,
  PublishOptions,
  Queue,
  QueueStats,
  ReceiveOptions,
  SendOptions,
  Subscription,
} from "./types.js";

export interface QueueCommandEvent {
  /** Logical command name, e.g. `"sendMessage"`, `"receiveMessages"`. */
  command: string;
}

export interface QueueResponseEvent {
  command: string;
  durationMs: number;
}

/** Unified interface for the beyond-queue HTTP client. */
export interface QueueClient {
  // ── Queue management ──────────────────────────────────────────────────────
  createQueue(
    name: string,
    opts?: CreateQueueOptions,
  ): Promise<{ queue_url: string }>;
  listQueues(): Promise<Queue[]>;
  /** Throws `QueueNotFoundError` if the queue does not exist. */
  getQueue(name: string): Promise<QueueStats>;
  deleteQueue(name: string): Promise<void>;
  purgeQueue(name: string): Promise<{ deleted: number }>;

  // ── Messages ──────────────────────────────────────────────────────────────
  sendMessage(
    queue: string,
    message: JsonValue,
    opts?: SendOptions,
  ): Promise<{ id: number }>;
  sendBatch(
    queue: string,
    entries: BatchEntry[],
    opts?: { async_commit?: boolean },
  ): Promise<{ ids: number[] }>;
  receiveMessages(queue: string, opts?: ReceiveOptions): Promise<Message[]>;
  /** No-op (resolves) if the message id does not exist. */
  deleteMessage(queue: string, id: number): Promise<void>;
  deleteMessages(queue: string, ids: number[]): Promise<{ deleted: number[] }>;
  changeVisibility(
    queue: string,
    id: number,
    vt: number,
  ): Promise<{ id: number; visible_at: string }>;

  // ── Topics & subscriptions ────────────────────────────────────────────────
  publish(
    routingKey: string,
    message: JsonValue,
    opts?: PublishOptions,
  ): Promise<{ queues_matched: number }>;
  subscribe(pattern: string, queueName: string): Promise<Subscription>;
  listTopicSubscriptions(pattern: string): Promise<Subscription[]>;
  listQueueSubscriptions(queueName: string): Promise<Subscription[]>;
  /** No-op (resolves) if the binding does not exist. */
  unsubscribe(pattern: string, queueName: string): Promise<void>;

  /** Release underlying connections. Call when the client is no longer needed. */
  close(): Promise<void>;
}

export interface QueueClientOptions {
  /** Base URL of the beyond-queue server, e.g. `"http://localhost:9324"`. */
  url: string;
  /**
   * Authorization header value. Any non-empty string is accepted by the server.
   * Default: `"Bearer anon"`.
   */
  auth?: string;
  /** Custom `fetch` implementation for connection pooling or test mocking. */
  fetch?: typeof globalThis.fetch;
  /** Per-request timeout in milliseconds. */
  timeout?: number;
  /** Max retry attempts on transient 5xx failures. Default: 2. */
  retries?: number;
  /** Called before each request. */
  onCommand?: (event: QueueCommandEvent) => void;
  /** Called after each response. */
  onResponse?: (event: QueueResponseEvent) => void;
}

/** Creates a queue client backed by the beyond-queue HTTP API. */
export function createQueueClient(opts: QueueClientOptions): QueueClient {
  return createHttpQueueClient(opts);
}
