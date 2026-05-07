import createFetchClient from "openapi-fetch";
import { QueueError } from "./errors.js";
import type { components, paths } from "./types.js";
import { type Camelize, camelize } from "./utils/camelize.js";

export { QueueError } from "./errors.js";
export type { components, operations, paths } from "./types.js";
export type { Camelize } from "./utils/camelize.js";

// ── Types derived from the generated OpenAPI schema ───────────────────────────

export type Queue = Camelize<components["schemas"]["QueueResponse"]>;
export type QueueStats = Camelize<
  components["schemas"]["QueueMetricsResponse"]
>;
export type Message = Camelize<components["schemas"]["MessageResponse"]>;
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

export interface BatchOptions {
  asyncCommit?: boolean;
}

export interface ReceiveOptions {
  max?: number;
  wait?: number;
  visibilityTimeout?: number;
  fifo?: boolean;
}

export interface QueueRequestEvent {
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
   * Bearer token for the `Authorization` header. Default: `"anon"`.
   */
  token?: string;
  /** Custom `fetch` implementation for test mocking or connection pooling. */
  fetch?: typeof globalThis.fetch;
  /** Per-request timeout in milliseconds. */
  timeout?: number;
  /** Max retry attempts on transient 5xx failures. Default: 2. */
  retries?: number;
  /** Called before each request. */
  onRequest?: (event: QueueRequestEvent) => void;
  /** Called after each response. */
  onResponse?: (event: QueueResponseEvent) => void;
}

export type QueueResult<T = undefined> = Promise<
  | { data: T; error: undefined; response: Response }
  | { data: undefined; error: QueueError; response: Response }
>;

export interface QueueClient {
  queues: {
    create(
      name: string,
      opts?: CreateQueueOptions,
    ): QueueResult<{ queueUrl: string }>;
    list(): QueueResult<Queue[]>;
    get(name: string): QueueResult<QueueStats>;
    delete(name: string): QueueResult;
    purge(
      name: string,
    ): QueueResult<Camelize<components["schemas"]["PurgeResponse"]>>;
  };
  messages: {
    send(
      queue: string,
      message: JsonValue,
      opts?: SendOptions,
    ): QueueResult<{ id: number }>;
    sendBatch(
      queue: string,
      entries: BatchEntry[],
      opts?: BatchOptions,
    ): QueueResult<{ ids: number[] }>;
    receive(queue: string, opts?: ReceiveOptions): QueueResult<Message[]>;
    delete(queue: string, id: number): QueueResult;
    deleteBatch(
      queue: string,
      ids: number[],
    ): QueueResult<Camelize<components["schemas"]["DeletedResponse"]>>;
    changeVisibility(
      queue: string,
      id: number,
      visibilityTimeout: number,
    ): QueueResult<Camelize<components["schemas"]["ChangeVisibilityResponse"]>>;
  };
  /** Release underlying connections. Call when the client is no longer needed. */
  close(): Promise<void>;
}

function toQueueError(raw: unknown, status: number): QueueError {
  const inner = raw != null && typeof raw === "object" && "error" in raw
    ? (raw as { error: { code?: string; message?: string; hint?: string } })
      .error
    : (raw as
      | { code?: string; message?: string; hint?: string }
      | undefined);
  const code = inner?.code ?? "internal_error";
  const message = inner?.message ?? "Unknown error";
  const hint = inner?.hint;
  return new QueueError(code, message, status, hint);
}

function wrap<T>(
  promise: Promise<{ data?: T; error?: unknown; response: Response }>,
): QueueResult<Camelize<T>> {
  return promise.then(({ data, error, response }) =>
    error !== undefined
      ? {
        data: undefined,
        error: toQueueError(error, response.status),
        response,
      }
      : { data: camelize(data) as Camelize<T>, error: undefined, response }
  ) as unknown as QueueResult<Camelize<T>>;
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
  const { onRequest, onResponse } = opts;

  const client = createFetchClient<paths>({
    baseUrl: base,
    headers: { Authorization: `Bearer ${opts.token ?? "anon"}` },
    fetch: buildFetch(opts.fetch, opts.retries ?? 2, opts.timeout),
  });

  // Wraps a method to fire onRequest/onResponse hooks around it.
  function cmd<A extends unknown[], R>(
    name: string,
    fn: (...args: A) => Promise<R>,
  ): (...args: A) => Promise<R> {
    return async (...args) => {
      onRequest?.({ command: name });
      const start = Date.now();
      try {
        return await fn(...args);
      } finally {
        onResponse?.({ command: name, durationMs: Date.now() - start });
      }
    };
  }

  return {
    queues: {
      create: cmd("queues.create", async (name, qOpts) => {
        const { error, response } = await client.POST("/v1/queues", {
          body: { name, fifo: qOpts?.fifo ?? false },
        });
        if (error) {
          return {
            data: undefined,
            error: toQueueError(error, response.status),
            response,
          };
        }
        return {
          data: { queueUrl: `${base}/v1/queues/${encodeURIComponent(name)}` },
          error: undefined,
          response,
        };
      }),

      list: cmd("queues.list", () => wrap(client.GET("/v1/queues", {}))),

      get: cmd(
        "queues.get",
        (name) =>
          wrap(
            client.GET("/v1/queues/{name}", { params: { path: { name } } }),
          ),
      ),

      delete: cmd("queues.delete", async (name) => {
        const { error, response } = await client.DELETE("/v1/queues/{name}", {
          params: { path: { name } },
        });
        if (error && response.status !== 404) {
          return {
            data: undefined,
            error: toQueueError(error, response.status),
            response,
          };
        }
        return { data: undefined, error: undefined, response };
      }),

      purge: cmd("queues.purge", (name) =>
        wrap(
          client.POST("/v1/queues/{name}/purge", {
            params: { path: { name } },
          }),
        )),
    },

    messages: {
      send: cmd("messages.send", async (queue, message, sOpts) => {
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
              headers: sOpts?.headers ?? null,
              ...(sOpts?.groupId !== undefined && {
                group_id: sOpts.groupId,
              }),
            },
          },
        );
        if (error) {
          return {
            data: undefined,
            error: toQueueError(error, response.status),
            response,
          };
        }
        return { data: data as { id: number }, error: undefined, response };
      }),

      sendBatch: cmd("messages.sendBatch", async (queue, entries, bOpts) => {
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
              headers: e.headers ?? null,
              ...(e.groupId !== undefined && { group_id: e.groupId }),
            })),
          },
        );
        if (error) {
          return {
            data: undefined,
            error: toQueueError(error, response.status),
            response,
          };
        }
        return { data: data as { ids: number[] }, error: undefined, response };
      }),

      receive: cmd("messages.receive", (queue, rOpts) =>
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

      delete: cmd("messages.delete", async (queue, id) => {
        const { error, response } = await client.DELETE(
          "/v1/queues/{name}/messages/{id}",
          { params: { path: { name: queue, id } } },
        );
        if (error && response.status !== 404) {
          return {
            data: undefined,
            error: toQueueError(error, response.status),
            response,
          };
        }
        return { data: undefined, error: undefined, response };
      }),

      deleteBatch: cmd("messages.deleteBatch", (queue, ids) =>
        wrap(
          client.DELETE("/v1/queues/{name}/messages", {
            params: { path: { name: queue } },
            body: { ids },
          }),
        )),

      changeVisibility: cmd(
        "messages.changeVisibility",
        (queue, id, visibilityTimeout) =>
          wrap(
            client.PATCH("/v1/queues/{name}/messages/{id}", {
              params: { path: { name: queue, id } },
              body: { vt: visibilityTimeout },
            }),
          ),
      ),
    },

    close: () => Promise.resolve(),
  };
}
