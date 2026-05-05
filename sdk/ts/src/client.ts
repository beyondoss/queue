import createFetchClient from "openapi-fetch";
import { QueueError, QueueNotFoundError } from "./errors.js";
import type { components, paths } from "./types.js";

export type { components, operations, paths } from "./types.js";

// Types derived from the generated OpenAPI schema
export type Queue = components["schemas"]["QueueResponse"];
export type QueueStats = components["schemas"]["QueueMetricsResponse"];
export type Message = components["schemas"]["MessageResponse"];
export type Subscription = components["schemas"]["TopicSubscription"];
export type BatchEntry = components["schemas"]["SendRequest"];
export type PublishResult = components["schemas"]["TopicSendResponse"];

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

// SDK-level option types (not part of the API schema)
export interface CreateQueueOptions {
  fifo?: boolean;
}

export interface SendOptions {
  headers?: JsonValue;
  delay?: number;
  group_id?: string;
  async_commit?: boolean;
}

export interface ReceiveOptions {
  max?: number;
  wait?: number;
  visibilityTimeout?: number;
  fifo?: boolean;
}

export interface PublishOptions {
  headers?: JsonValue;
  delay?: number;
}

export interface QueueCommandEvent {
  command: string;
}

export interface QueueResponseEvent {
  command: string;
  durationMs: number;
}

export interface QueueClientOptions {
  /** Base URL of the beyond-queue server, e.g. `"http://localhost:9324"`. */
  url: string;
  /**
   * Authorization header value. Any non-empty string is accepted by the server.
   * Default: `"Bearer anon"`.
   */
  auth?: string;
  /** Custom `fetch` implementation for test mocking or connection pooling. */
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

export interface QueueClient {
  // ── Queue management ──────────────────────────────────────────────────────
  createQueue(
    name: string,
    opts?: CreateQueueOptions,
  ): Promise<{ queue_url: string }>;
  listQueues(): Promise<Queue[]>;
  /** Throws `QueueNotFoundError` if the queue does not exist. */
  getQueueStats(name: string): Promise<QueueStats>;
  deleteQueue(name: string): Promise<void>;
  purgeQueue(name: string): Promise<components["schemas"]["PurgeResponse"]>;

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
  deleteMessages(
    queue: string,
    ids: number[],
  ): Promise<components["schemas"]["DeletedResponse"]>;
  changeVisibility(
    queue: string,
    id: number,
    visibilityTimeout: number,
  ): Promise<components["schemas"]["ChangeVisibilityResponse"]>;

  // ── Topics & subscriptions ────────────────────────────────────────────────
  publish(
    routingKey: string,
    message: JsonValue,
    opts?: PublishOptions,
  ): Promise<PublishResult>;
  subscribe(pattern: string, queueName: string): Promise<Subscription>;
  subscribeHttp(
    pattern: string,
    endpoint: string,
    opts?: { envelope?: boolean },
  ): Promise<Subscription>;
  listTopicSubscriptions(pattern: string): Promise<Subscription[]>;
  listQueueSubscriptions(queueName: string): Promise<Subscription[]>;
  /** No-op (resolves) if the subscription does not exist. */
  unsubscribe(subscriptionId: number): Promise<void>;

  /** Release underlying connections. Call when the client is no longer needed. */
  close(): Promise<void>;
}

function throwError(error: unknown, response: Response): never {
  const e = error as components["schemas"]["ErrorResponse"] | undefined;
  throw new QueueError(
    e?.code ?? "internal_error",
    e?.message ?? response.statusText,
    response.status,
  );
}

function buildFetch(
  base: typeof globalThis.fetch | undefined,
  retries: number,
  timeout: number | undefined,
): typeof globalThis.fetch {
  const fetchFn = base ?? globalThis.fetch;
  return async (input, init) => {
    const signal = timeout != null
      ? AbortSignal.timeout(timeout)
      : init?.signal;
    const initWithSignal = signal != null ? { ...init, signal } : init;
    for (let attempt = 0; attempt <= retries; attempt++) {
      if (attempt > 0) {
        await new Promise<void>((r) => setTimeout(r, 100 * 2 ** (attempt - 1)));
      }
      let res: Response;
      try {
        res = await fetchFn(input, initWithSignal);
      } catch (err) {
        if (attempt >= retries) throw err;
        continue;
      }
      if (res.status >= 500 && attempt < retries) {
        await res.body?.cancel();
        continue;
      }
      return res;
    }
    throw new Error("unreachable");
  };
}

/** Creates a queue client backed by the beyond-queue HTTP API. */
export function createQueueClient(opts: QueueClientOptions): QueueClient {
  const base = opts.url.replace(/\/+$/, "");
  const { onCommand, onResponse } = opts;

  const client = createFetchClient<paths>({
    baseUrl: base,
    headers: { Authorization: opts.auth ?? "Bearer anon" },
    fetch: buildFetch(opts.fetch, opts.retries ?? 2, opts.timeout),
  });

  // Wraps a method to fire onCommand/onResponse hooks around it.
  function cmd<A extends unknown[], R>(
    name: string,
    fn: (...args: A) => Promise<R>,
  ): (...args: A) => Promise<R> {
    return async (...args) => {
      onCommand?.({ command: name });
      const start = Date.now();
      try {
        return await fn(...args);
      } finally {
        onResponse?.({ command: name, durationMs: Date.now() - start });
      }
    };
  }

  return {
    createQueue: cmd("createQueue", async (name, qOpts) => {
      const { error, response } = await client.POST("/v1/queues", {
        body: { name, fifo: qOpts?.fifo ?? false },
      });
      if (error) throwError(error, response);
      return { queue_url: `${base}/v1/queues/${encodeURIComponent(name)}` };
    }),

    listQueues: cmd("listQueues", async () => {
      const { data, error, response } = await client.GET("/v1/queues", {});
      if (error) throwError(error, response);
      return data!;
    }),

    getQueueStats: cmd("getQueueStats", async (name) => {
      const { data, error, response } = await client.GET("/v1/queues/{name}", {
        params: { path: { name } },
      });
      if (error) {
        if (response.status === 404) throw new QueueNotFoundError(name);
        throwError(error, response);
      }
      return data!;
    }),

    deleteQueue: cmd("deleteQueue", async (name) => {
      const { error, response } = await client.DELETE("/v1/queues/{name}", {
        params: { path: { name } },
      });
      if (error && response.status !== 404) throwError(error, response);
    }),

    purgeQueue: cmd("purgeQueue", async (name) => {
      const { data, error, response } = await client.POST(
        "/v1/queues/{name}/purge",
        { params: { path: { name } } },
      );
      if (error) throwError(error, response);
      return data!;
    }),

    sendMessage: cmd("sendMessage", async (queue, message, sOpts) => {
      const { data, error, response } = await client.POST(
        "/v1/queues/{name}/messages",
        {
          params: {
            path: { name: queue },
            ...(sOpts?.async_commit && { query: { async_commit: true } }),
          },
          body: {
            message,
            delay: sOpts?.delay ?? 0,
            ...(sOpts?.headers !== undefined && { headers: sOpts.headers }),
            ...(sOpts?.group_id !== undefined && { group_id: sOpts.group_id }),
          },
        },
      );
      if (error) throwError(error, response);
      return data! as { id: number };
    }),

    sendBatch: cmd("sendBatch", async (queue, entries, bOpts) => {
      const { data, error, response } = await client.POST(
        "/v1/queues/{name}/messages",
        {
          params: {
            path: { name: queue },
            ...(bOpts?.async_commit && { query: { async_commit: true } }),
          },
          body: entries,
        },
      );
      if (error) throwError(error, response);
      return data! as { ids: number[] };
    }),

    receiveMessages: cmd("receiveMessages", async (queue, rOpts) => {
      const { data, error, response } = await client.GET(
        "/v1/queues/{name}/messages",
        {
          params: {
            path: { name: queue },
            query: {
              ...(rOpts?.max !== undefined && { max: rOpts.max }),
              ...(rOpts?.wait !== undefined && { wait: rOpts.wait }),
              ...(rOpts?.visibilityTimeout !== undefined
                && { vt: rOpts.visibilityTimeout }),
              ...(rOpts?.fifo !== undefined && { fifo: rOpts.fifo }),
            },
          },
        },
      );
      if (error) throwError(error, response);
      return data!;
    }),

    deleteMessage: cmd("deleteMessage", async (queue, id) => {
      const { error, response } = await client.DELETE(
        "/v1/queues/{name}/messages/{id}",
        { params: { path: { name: queue, id } } },
      );
      if (error && response.status !== 404) throwError(error, response);
    }),

    deleteMessages: cmd("deleteMessages", async (queue, ids) => {
      const { data, error, response } = await client.DELETE(
        "/v1/queues/{name}/messages",
        {
          params: { path: { name: queue } },
          body: { ids },
        },
      );
      if (error) throwError(error, response);
      return data!;
    }),

    changeVisibility: cmd(
      "changeVisibility",
      async (queue, id, visibilityTimeout) => {
        const { data, error, response } = await client.PATCH(
          "/v1/queues/{name}/messages/{id}",
          {
            params: { path: { name: queue, id } },
            body: { vt: visibilityTimeout },
          },
        );
        if (error) throwError(error, response);
        return data!;
      },
    ),

    publish: cmd("publish", async (routingKey, message, pOpts) => {
      const { data, error, response } = await client.POST(
        "/v1/topics/{routing_key}",
        {
          params: { path: { routing_key: routingKey } },
          body: {
            message,
            delay: pOpts?.delay ?? 0,
            ...(pOpts?.headers !== undefined && { headers: pOpts.headers }),
          },
        },
      );
      if (error) throwError(error, response);
      return data!;
    }),

    subscribe: cmd("subscribe", async (pattern, queueName) => {
      const { data, error, response } = await client.POST(
        "/v1/topics/{pattern}/subscriptions",
        {
          params: { path: { pattern } },
          body: { queue_name: queueName },
        },
      );
      if (error) throwError(error, response);
      return data!;
    }),

    subscribeHttp: cmd("subscribeHttp", async (pattern, endpoint, sOpts) => {
      const { data, error, response } = await client.POST(
        "/v1/topics/{pattern}/subscriptions",
        {
          params: { path: { pattern } },
          body: {
            protocol: new URL(endpoint).protocol.replace(":", ""),
            endpoint,
            envelope: sOpts?.envelope ?? false,
          },
        },
      );
      if (error) throwError(error, response);
      return data!;
    }),

    listTopicSubscriptions: cmd("listTopicSubscriptions", async (pattern) => {
      const { data, error, response } = await client.GET(
        "/v1/topics/{pattern}/subscriptions",
        { params: { path: { pattern } } },
      );
      if (error) throwError(error, response);
      return data!;
    }),

    listQueueSubscriptions: cmd("listQueueSubscriptions", async (queueName) => {
      const { data, error, response } = await client.GET(
        "/v1/queues/{name}/subscriptions",
        { params: { path: { name: queueName } } },
      );
      if (error) throwError(error, response);
      return data!;
    }),

    unsubscribe: cmd("unsubscribe", async (subscriptionId) => {
      const { error, response } = await client.DELETE(
        "/v1/topics/{pattern}/subscriptions/{id}",
        { params: { path: { pattern: "_", id: subscriptionId } } },
      );
      if (error && response.status !== 404) throwError(error, response);
    }),

    close: () => Promise.resolve(),
  };
}
