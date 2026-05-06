import createFetchClient from "openapi-fetch";
import type { components, paths } from "./types.js";
import { type Camelize, camelize } from "./utils/camelize.js";

export type { components, operations, paths } from "./types.js";
export type { Camelize } from "./utils/camelize.js";

// ── Types derived from the generated OpenAPI schema ───────────────────────────

export type Queue = Camelize<components["schemas"]["QueueResponse"]>;
export type QueueStats = Camelize<
  components["schemas"]["QueueMetricsResponse"]
>;
export type Message = Camelize<components["schemas"]["MessageResponse"]>;
export type Subscription = Camelize<components["schemas"]["TopicSubscription"]>;
export type PublishResult = Camelize<
  components["schemas"]["TopicSendResponse"]
>;

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

// ── SDK-level option types (not part of the API schema) ───────────────────────

export interface CreateQueueOptions {
  fifo?: boolean;
}

export interface SendOptions {
  headers?: JsonValue;
  delay?: number;
  groupId?: string;
  asyncCommit?: boolean;
}

export interface BatchEntry {
  message: JsonValue;
  delay?: number;
  groupId?: string;
  headers?: JsonValue;
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

type ApiError = components["schemas"]["ErrorResponse"];

type ApiResult<T = undefined> = Promise<
  | { data: T; error: undefined; response: Response }
  | { data: undefined; error: ApiError; response: Response }
>;

export interface QueueClient {
  // ── Queue management ──────────────────────────────────────────────────────
  createQueue(
    name: string,
    opts?: CreateQueueOptions,
  ): ApiResult<{ queueUrl: string }>;
  listQueues(): ApiResult<Queue[]>;
  getQueueStats(name: string): ApiResult<QueueStats>;
  deleteQueue(name: string): ApiResult;
  purgeQueue(
    name: string,
  ): ApiResult<Camelize<components["schemas"]["PurgeResponse"]>>;

  // ── Messages ──────────────────────────────────────────────────────────────
  sendMessage(
    queue: string,
    message: JsonValue,
    opts?: SendOptions,
  ): ApiResult<{ id: number }>;
  sendBatch(
    queue: string,
    entries: BatchEntry[],
    opts?: { asyncCommit?: boolean },
  ): ApiResult<{ ids: number[] }>;
  receiveMessages(queue: string, opts?: ReceiveOptions): ApiResult<Message[]>;
  deleteMessage(queue: string, id: number): ApiResult;
  deleteMessages(
    queue: string,
    ids: number[],
  ): ApiResult<Camelize<components["schemas"]["DeletedResponse"]>>;
  changeVisibility(
    queue: string,
    id: number,
    visibilityTimeout: number,
  ): ApiResult<Camelize<components["schemas"]["ChangeVisibilityResponse"]>>;

  // ── Topics & subscriptions ────────────────────────────────────────────────
  publish(
    routingKey: string,
    message: JsonValue,
    opts?: PublishOptions,
  ): ApiResult<PublishResult>;
  subscribe(pattern: string, queueName: string): ApiResult<Subscription>;
  subscribeHttp(
    pattern: string,
    endpoint: string,
    opts?: { envelope?: boolean },
  ): ApiResult<Subscription>;
  listTopicSubscriptions(pattern: string): ApiResult<Subscription[]>;
  listQueueSubscriptions(queueName: string): ApiResult<Subscription[]>;
  unsubscribe(subscriptionId: number): ApiResult;

  /** Release underlying connections. Call when the client is no longer needed. */
  close(): Promise<void>;
}

function wrap<T>(
  promise: Promise<{ data?: T; error?: ApiError; response: Response }>,
): ApiResult<Camelize<T>> {
  return promise.then(({ data, error, response }) =>
    error !== undefined
      ? { data: undefined, error, response }
      : { data: camelize(data) as Camelize<T>, error: undefined, response }
  ) as unknown as ApiResult<Camelize<T>>;
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
      if (error) return { data: undefined, error, response };
      return {
        data: { queueUrl: `${base}/v1/queues/${encodeURIComponent(name)}` },
        error: undefined,
        response,
      };
    }),

    listQueues: cmd("listQueues", () => wrap(client.GET("/v1/queues", {}))),

    getQueueStats: cmd(
      "getQueueStats",
      (name) =>
        wrap(client.GET("/v1/queues/{name}", { params: { path: { name } } })),
    ),

    deleteQueue: cmd("deleteQueue", async (name) => {
      const { error, response } = await client.DELETE("/v1/queues/{name}", {
        params: { path: { name } },
      });
      if (error && response.status !== 404) {
        return { data: undefined, error, response };
      }
      return { data: undefined, error: undefined, response };
    }),

    purgeQueue: cmd("purgeQueue", (name) =>
      wrap(
        client.POST("/v1/queues/{name}/purge", { params: { path: { name } } }),
      )),

    sendMessage: cmd("sendMessage", async (queue, message, sOpts) => {
      const { data, error, response } = await client.POST(
        "/v1/queues/{name}/messages",
        {
          params: {
            path: { name: queue },
            ...(sOpts?.asyncCommit && { query: { async_commit: true } }),
          },
          body: {
            message,
            delay: sOpts?.delay ?? 0,
            ...(sOpts?.headers !== undefined && { headers: sOpts.headers }),
            ...(sOpts?.groupId !== undefined && { group_id: sOpts.groupId }),
          },
        },
      );
      if (error) return { data: undefined, error, response };
      return { data: data as { id: number }, error: undefined, response };
    }),

    sendBatch: cmd("sendBatch", async (queue, entries, bOpts) => {
      const { data, error, response } = await client.POST(
        "/v1/queues/{name}/messages",
        {
          params: {
            path: { name: queue },
            ...(bOpts?.asyncCommit && { query: { async_commit: true } }),
          },
          body: entries.map((e) => ({
            message: e.message,
            delay: e.delay ?? 0,
            ...(e.headers !== undefined && { headers: e.headers }),
            ...(e.groupId !== undefined && { group_id: e.groupId }),
          })) as components["schemas"]["SendRequest"][],
        },
      );
      if (error) return { data: undefined, error, response };
      return { data: data as { ids: number[] }, error: undefined, response };
    }),

    receiveMessages: cmd("receiveMessages", (queue, rOpts) =>
      wrap(
        client.GET("/v1/queues/{name}/messages", {
          params: {
            path: { name: queue },
            query: {
              ...(rOpts?.max !== undefined && { max: rOpts.max }),
              ...(rOpts?.wait !== undefined && { wait: rOpts.wait }),
              ...(rOpts?.visibilityTimeout !== undefined && {
                vt: rOpts.visibilityTimeout,
              }),
              ...(rOpts?.fifo !== undefined && { fifo: rOpts.fifo }),
            },
          },
        }),
      )),

    deleteMessage: cmd("deleteMessage", async (queue, id) => {
      const { error, response } = await client.DELETE(
        "/v1/queues/{name}/messages/{id}",
        { params: { path: { name: queue, id } } },
      );
      if (error && response.status !== 404) {
        return { data: undefined, error, response };
      }
      return { data: undefined, error: undefined, response };
    }),

    deleteMessages: cmd("deleteMessages", (queue, ids) =>
      wrap(
        client.DELETE("/v1/queues/{name}/messages", {
          params: { path: { name: queue } },
          body: { ids },
        }),
      )),

    changeVisibility: cmd(
      "changeVisibility",
      (queue, id, visibilityTimeout) =>
        wrap(
          client.PATCH("/v1/queues/{name}/messages/{id}", {
            params: { path: { name: queue, id } },
            body: { vt: visibilityTimeout },
          }),
        ),
    ),

    publish: cmd("publish", (routingKey, message, pOpts) =>
      wrap(
        client.POST("/v1/topics/{routing_key}", {
          params: { path: { routing_key: routingKey } },
          body: {
            message,
            delay: pOpts?.delay ?? 0,
            ...(pOpts?.headers !== undefined && { headers: pOpts.headers }),
          },
        }),
      )),

    subscribe: cmd("subscribe", (pattern, queueName) =>
      wrap(
        client.POST("/v1/topics/{pattern}/subscriptions", {
          params: { path: { pattern } },
          body: { queue_name: queueName },
        }),
      )),

    subscribeHttp: cmd("subscribeHttp", (pattern, endpoint, sOpts) =>
      wrap(
        client.POST("/v1/topics/{pattern}/subscriptions", {
          params: { path: { pattern } },
          body: {
            protocol: new URL(endpoint).protocol.replace(":", ""),
            endpoint,
            envelope: sOpts?.envelope ?? false,
          },
        }),
      )),

    listTopicSubscriptions: cmd("listTopicSubscriptions", (pattern) =>
      wrap(
        client.GET("/v1/topics/{pattern}/subscriptions", {
          params: { path: { pattern } },
        }),
      )),

    listQueueSubscriptions: cmd("listQueueSubscriptions", (queueName) =>
      wrap(
        client.GET("/v1/queues/{name}/subscriptions", {
          params: { path: { name: queueName } },
        }),
      )),

    unsubscribe: cmd("unsubscribe", async (subscriptionId) => {
      const { error, response } = await client.DELETE(
        "/v1/topics/{pattern}/subscriptions/{id}",
        { params: { path: { pattern: "_", id: subscriptionId } } },
      );
      if (error && response.status !== 404) {
        return { data: undefined, error, response };
      }
      return { data: undefined, error: undefined, response };
    }),

    close: () => Promise.resolve(),
  };
}
