import createFetchClient from "openapi-fetch";
import { env } from "std-env";
import { QueueError } from "./errors.js";
import type { components, paths } from "./types.js";
import { type Camelize, camelize } from "./utils/camelize.js";

// ── TLS types ─────────────────────────────────────────────────────────────────

export interface TlsOptions {
  /** PEM-encoded CA certificate(s) to trust. */
  ca?: string | string[];
  /** PEM-encoded client certificate for mTLS. */
  cert?: string;
  /** PEM-encoded client private key for mTLS. */
  key?: string;
}

// ── TLS-aware fetch builder ───────────────────────────────────────────────────

type MaybeFetch = typeof globalThis.fetch;

function buildTlsFetchPromise(tls: TlsOptions): Promise<MaybeFetch> {
  const cas = Array.isArray(tls.ca) ? tls.ca : tls.ca ? [tls.ca] : undefined;

  // Deno
  const gAny = globalThis as Record<string, unknown>;
  if (
    typeof gAny["Deno"] !== "undefined"
    && typeof (gAny["Deno"] as Record<string, unknown>)["createHttpClient"]
      === "function"
  ) {
    const denoNs = gAny["Deno"] as {
      createHttpClient: (opts: Record<string, unknown>) => unknown;
    };
    const client = denoNs.createHttpClient({
      caCerts: cas,
      certChain: tls.cert,
      privateKey: tls.key,
    });
    return Promise.resolve(
      (url: RequestInfo | URL, init?: RequestInit) =>
        globalThis.fetch(url, { ...init, client } as RequestInit),
    );
  }

  // Node / Bun — undici
  const _undici = "undici";
  return (import(_undici) as Promise<any>)
    .then(({ fetch: f, Agent }: any) => {
      const connect: Record<string, unknown> = {};
      if (cas != null) connect["ca"] = cas;
      if (tls.cert != null) connect["cert"] = tls.cert;
      if (tls.key != null) connect["key"] = tls.key;
      const agent = new Agent({ allowH2: true, connect });
      // undici-extended RequestInit that carries the dispatcher
      type UndiciInit = RequestInit & { dispatcher: unknown };
      const withAgent = (init?: RequestInit): UndiciInit => ({
        ...(init ?? {}),
        dispatcher: agent,
      });
      return (url: RequestInfo | URL, init?: RequestInit) => {
        // undici's fetch does not accept a Request object when a dispatcher is
        // set alongside it — extract the URL string and merge the options.
        if (url instanceof Request) {
          const req = url as Request;
          const hasBody = req.method !== "GET" && req.method !== "HEAD";
          const merged: UndiciInit = {
            method: req.method,
            headers: req.headers,
            ...(init ?? {}),
            ...(hasBody && { body: init?.body ?? req.body }),
            dispatcher: agent,
          };
          return f(req.url, merged) as Promise<Response>;
        }
        return f(url, withAgent(init)) as Promise<Response>;
      };
    })
    .catch(() => globalThis.fetch);
}

export { QueueError } from "./errors.js";
export type { components, operations, paths } from "./types.js";
export type { Camelize } from "./utils/camelize.js";

// ── Types derived from the generated OpenAPI schema ───────────────────────────

export type Queue = Camelize<components["schemas"]["QueueResponse"]>;
export type QueueStats = Camelize<
  components["schemas"]["QueueMetricsResponse"]
>;

export type Message<T = JsonValue> = {
  id: number;
  message: T;
  headers: JsonValue | null;
  readCount: number;
  enqueuedAt: string;
  visibleAt: string;
};

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

// ── Schema types ──────────────────────────────────────────────────────────────

/** A value parser. Compatible with Zod schemas and any object with `.parse`. */
export type Schema<T> = { parse: (value: unknown) => T };

/** Maps queue names to their message body schemas. */
export type QueueSchemaMap = Record<string, Schema<unknown>>;

export type QueueBodyType<K extends string, Map extends QueueSchemaMap> =
  K extends keyof Map ? Map[K] extends Schema<infer T> ? T : JsonValue
    : JsonValue;

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

export interface BatchEntry<T = JsonValue> {
  message: T;
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
  /**
   * Base URL of the beyond-queue server, e.g. `"http://localhost:9324"`.
   * Defaults to the `BEYOND_QUEUE_URL` environment variable when omitted.
   */
  url?: string;
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
  /**
   * TLS options for mTLS connections.
   * Node/Bun use undici Agent; Deno uses Deno.createHttpClient;
   * edge/browser runtimes silently ignore these options.
   */
  tls?: TlsOptions;
}

export type QueueResult<T = undefined> = Promise<
  | { data: T; error: undefined; response: Response }
  | { data: undefined; error: QueueError; response: Response }
>;

// ── Client interfaces ─────────────────────────────────────────────────────────

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

/**
 * A queue client with schema-aware message types. Returned by `createQueueClient`
 * when a `schema` map is provided. Queue names in the schema get typed bodies;
 * all other queue names fall back to `JsonValue`.
 */
export interface QueueSchemaClient<Map extends QueueSchemaMap>
  extends Omit<QueueClient, "messages">
{
  messages: {
    send<K extends string>(
      queue: K,
      message: QueueBodyType<K, Map>,
      opts?: SendOptions,
    ): QueueResult<{ id: number }>;
    sendBatch<K extends string>(
      queue: K,
      entries: BatchEntry<QueueBodyType<K, Map>>[],
      opts?: BatchOptions,
    ): QueueResult<{ ids: number[] }>;
    receive<K extends string>(
      queue: K,
      opts?: ReceiveOptions,
    ): QueueResult<Message<QueueBodyType<K, Map>>[]>;
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
}

// ── Internal helpers ──────────────────────────────────────────────────────────

function toQueueError(raw: unknown, response: Response): QueueError {
  const inner = raw != null && typeof raw === "object" && "error" in raw
    ? (raw as { error: { code?: string; message?: string; hint?: string } })
      .error
    : (raw as
      | { code?: string; message?: string; hint?: string }
      | undefined);
  const code = inner?.code ?? "internal_error";
  const message = inner?.message ?? "Unknown error";
  const hint = inner?.hint;
  return new QueueError(code, message, response.status, response, hint);
}

function wrap<T>(
  promise: Promise<{ data?: T; error?: unknown; response: Response }>,
): QueueResult<Camelize<T>> {
  return promise.then(({ data, error, response }) =>
    error !== undefined
      ? {
        data: undefined,
        error: toQueueError(error, response),
        response,
      }
      : { data: camelize(data) as Camelize<T>, error: undefined, response }
  ) as unknown as QueueResult<Camelize<T>>;
}

function buildFetch(
  base: MaybeFetch | undefined,
  tlsFetchPromise: Promise<MaybeFetch> | undefined,
  retries: number,
  timeout: number | undefined,
): MaybeFetch {
  let resolvedTls: MaybeFetch | undefined;
  const tlsInit = tlsFetchPromise?.then((f) => {
    resolvedTls = f;
  });
  return async (input, init) => {
    if (tlsInit) await tlsInit;
    const fetchFn = base ?? resolvedTls ?? globalThis.fetch;
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

// ── Factory ───────────────────────────────────────────────────────────────────

/** Creates a schema-aware queue client. Message bodies are typed and parsed per queue. */
export function createQueueClient<Map extends QueueSchemaMap>(
  opts: QueueClientOptions & { schema: Map },
): QueueSchemaClient<Map>;
/** Creates a queue client backed by the beyond-queue HTTP API. */
export function createQueueClient(opts?: QueueClientOptions): QueueClient;
export function createQueueClient<Map extends QueueSchemaMap>(
  opts?: QueueClientOptions & { schema?: Map },
): QueueClient | QueueSchemaClient<Map> {
  const schema = opts?.schema;
  const url = opts?.url ?? env["BEYOND_QUEUE_URL"];
  if (!url) {
    throw new Error(
      "BEYOND_QUEUE_URL is required (pass `url` or set the BEYOND_QUEUE_URL env var)",
    );
  }
  const base = url.replace(/\/+$/, "");
  const token = opts?.token ?? env["BEYOND_QUEUE_TOKEN"];
  const { onRequest, onResponse } = opts ?? {};

  const tlsFetchPromise = opts?.tls
    ? buildTlsFetchPromise(opts.tls)
    : undefined;

  const client = createFetchClient<paths>({
    baseUrl: base,
    headers: { Authorization: `Bearer ${token ?? "anon"}` },
    fetch: buildFetch(
      opts?.fetch,
      tlsFetchPromise,
      opts?.retries ?? 2,
      opts?.timeout,
    ),
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
            error: toQueueError(error, response),
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
            error: toQueueError(error, response),
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
            error: toQueueError(error, response),
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
            error: toQueueError(error, response),
            response,
          };
        }
        return { data: data as { ids: number[] }, error: undefined, response };
      }),

      receive: cmd("messages.receive", async (queue, rOpts) => {
        const result = await wrap(
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
        ) as unknown as
          | { data: Message[]; error: undefined; response: Response }
          | { data: undefined; error: QueueError; response: Response };
        if (!result.error && schema) {
          const qSchema = schema[queue];
          if (qSchema) {
            return {
              ...result,
              data: result.data.map((msg) => ({
                ...msg,
                message: qSchema.parse(msg.message as unknown),
              })) as Message[],
            };
          }
        }
        return result;
      }),

      delete: cmd("messages.delete", async (queue, id) => {
        const { error, response } = await client.DELETE(
          "/v1/queues/{name}/messages/{id}",
          { params: { path: { name: queue, id } } },
        );
        if (error && response.status !== 404) {
          return {
            data: undefined,
            error: toQueueError(error, response),
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
