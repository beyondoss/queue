import createFetchClient from "openapi-fetch";
import { env } from "std-env";
import { EventError } from "./errors.js";
import type { components, paths } from "./types.js";
import { type Camelize, camelize } from "./utils/camelize.js";

export { EventError } from "./errors.js";
export type { components, paths } from "./types.js";
export type { Camelize } from "./utils/camelize.js";

// ── Shared types ──────────────────────────────────────────────────────────────

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

export type EventResult<T = undefined> = Promise<
  | { data: T; error: undefined; response: Response }
  | { data: undefined; error: EventError; response: Response }
>;

// ── Public SDK types ──────────────────────────────────────────────────────────

export type Subscription = Camelize<components["schemas"]["TopicSubscription"]>;

export type PublishResult = Camelize<
  components["schemas"]["TopicSendResponse"]
>;

/**
 * Identifies the delivery target for a subscription.
 * - `{ type: 'queue', name }` — enqueue into an existing beyond-queue queue.
 * - `{ type: 'http', endpoint }` — POST to an HTTP endpoint.
 * - `{ type: 'https', endpoint }` — POST to an HTTPS endpoint.
 */
export type EventTarget =
  | { type: "queue"; name: string }
  | { type: "http"; endpoint: string; envelope?: boolean }
  | { type: "https"; endpoint: string; envelope?: boolean };

export interface PublishOptions {
  delay?: number;
  headers?: Record<string, string>;
}

export interface EventRequestEvent {
  command: string;
}

export interface EventResponseEvent {
  command: string;
  durationMs: number;
}

export interface EventClientOptions {
  /**
   * Base URL of the beyond-queue server, e.g. `"http://localhost:9324"`.
   * Defaults to the `BEYOND_EVENTS_URL` environment variable when omitted.
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
  onRequest?: (event: EventRequestEvent) => void;
  /** Called after each response. */
  onResponse?: (event: EventResponseEvent) => void;
}

// ── Schema types ──────────────────────────────────────────────────────────────

/** A value parser. Compatible with Zod schemas and any object with `.parse`. */
export type Schema<T> = { parse: (value: unknown) => T };

/** Maps routing key patterns (including globs) to their payload schemas. */
export type EventSchemaMap = Record<string, Schema<unknown>>;

type GlobMatch<K extends string, P extends string> = P extends
  `${infer Pre}*${infer Suf}` ? K extends `${Pre}${string}${Suf}` ? true : false
  : K extends P ? true
  : false;

type MatchedPattern<K extends string, Map extends EventSchemaMap> = {
  [P in keyof Map & string]: GlobMatch<K, P> extends true ? P : never;
}[keyof Map & string];

export type EventPayloadType<K extends string, Map extends EventSchemaMap> =
  [MatchedPattern<K, Map>] extends [never] ? JsonValue
    : Map[MatchedPattern<K, Map> & keyof Map] extends Schema<infer T> ? T
    : JsonValue;

// ── Client interfaces ─────────────────────────────────────────────────────────

export interface EventClient {
  /**
   * Publish a message to a routing key. All subscriptions whose pattern
   * matches the routing key receive a copy of the message.
   */
  publish(
    routingKey: string,
    payload: JsonValue,
    opts?: PublishOptions,
  ): EventResult<PublishResult>;

  subscriptions: {
    /** Subscribe a queue or HTTP endpoint to a glob pattern. */
    create(
      pattern: string,
      target: EventTarget,
    ): EventResult<Subscription>;

    /** List all subscriptions for an exact pattern. */
    list(pattern: string): EventResult<Subscription[]>;

    /** List all subscriptions whose target is a specific queue. */
    listByQueue(queueName: string): EventResult<Subscription[]>;

    /** Remove a subscription by ID. Idempotent — no error if already gone. */
    delete(id: number): EventResult;
  };

  /** Release underlying connections. Call when the client is no longer needed. */
  close(): Promise<void>;
}

/**
 * An event client with schema-aware payload types. Returned by `createEventClient`
 * when a `schema` map is provided. Routing keys matching a schema pattern get typed
 * payloads; unmatched keys fall back to `JsonValue`.
 */
export interface EventSchemaClient<Map extends EventSchemaMap>
  extends Omit<EventClient, "publish">
{
  publish<K extends string>(
    routingKey: K,
    payload: EventPayloadType<K, Map>,
    opts?: PublishOptions,
  ): EventResult<PublishResult>;
}

// ── Internal helpers ──────────────────────────────────────────────────────────

function toEventError(error: unknown, response: Response): EventError {
  const status = response.status;
  if (typeof error === "object" && error !== null && "error" in error) {
    const e =
      (error as { error: { code?: string; message?: string; hint?: string } })
        .error;
    return new EventError(
      e?.code ?? "unknown_error",
      e?.message ?? "Unknown error",
      status,
      response,
      e?.hint ?? undefined,
    );
  }
  return new EventError("unknown_error", String(error), status, response);
}

function wrap<T>(
  promise: Promise<{ data?: T; error?: unknown; response: Response }>,
): EventResult<Camelize<T>> {
  return promise.then(({ data, error, response }) => {
    if (error !== undefined) {
      return {
        data: undefined,
        error: toEventError(error, response),
        response,
      };
    }
    return {
      data: data !== undefined
        ? (camelize(data) as Camelize<T>)
        : (undefined as unknown as Camelize<T>),
      error: undefined,
      response,
    };
  }) as unknown as EventResult<Camelize<T>>;
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

// ── Factory ───────────────────────────────────────────────────────────────────

/** Creates a schema-aware event client. Publish payload types are inferred from the schema map. */
export function createEventClient<Map extends EventSchemaMap>(
  opts: EventClientOptions & { schema: Map },
): EventSchemaClient<Map>;
/** Creates an event client backed by the beyond-queue HTTP API. */
export function createEventClient(opts?: EventClientOptions): EventClient;
export function createEventClient(
  opts?: EventClientOptions & { schema?: EventSchemaMap },
): EventClient {
  const url = opts?.url ?? env["BEYOND_EVENTS_URL"];
  if (!url) {
    throw new Error(
      "BEYOND_EVENTS_URL is required (pass `url` or set the BEYOND_EVENTS_URL env var)",
    );
  }
  const base = url.replace(/\/+$/, "");
  const token = opts?.token ?? env["BEYOND_EVENTS_TOKEN"];
  const { onRequest, onResponse } = opts ?? {};

  const client = createFetchClient<paths>({
    baseUrl: base,
    headers: { Authorization: `Bearer ${token ?? "anon"}` },
    fetch: buildFetch(opts?.fetch, opts?.retries ?? 2, opts?.timeout),
  });

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
    publish: cmd("publish", (routingKey, payload, pOpts) =>
      wrap(
        client.POST("/v1/events/{routing_key}", {
          params: { path: { routing_key: routingKey } },
          body: {
            message: payload,
            delay: pOpts?.delay ?? 0,
            headers: pOpts?.headers ?? null,
          },
        }),
      )),

    subscriptions: {
      create: cmd("subscriptions.create", (pattern, target) => {
        const body: components["schemas"]["SubscribeRequest"] =
          target.type === "queue"
            ? { queue_name: target.name }
            : {
              protocol: target.type,
              endpoint: target.endpoint,
              envelope: target.envelope ?? false,
            };
        return wrap(
          client.POST("/v1/events/{pattern}/subscriptions", {
            params: { path: { pattern } },
            body,
          }),
        );
      }),

      list: cmd("subscriptions.list", (pattern) =>
        wrap(
          client.GET("/v1/events/{pattern}/subscriptions", {
            params: { path: { pattern } },
          }),
        )),

      listByQueue: cmd("subscriptions.listByQueue", (queueName) =>
        wrap(
          client.GET("/v1/queues/{name}/subscriptions", {
            params: { path: { name: queueName } },
          }),
        )),

      delete: cmd("subscriptions.delete", async (id) => {
        const { error, response } = await client.DELETE(
          "/v1/events/{pattern}/subscriptions/{id}",
          { params: { path: { pattern: "_", id } } },
        );
        if (error && response.status !== 404) {
          return {
            data: undefined,
            error: toEventError(error, response),
            response,
          };
        }
        return { data: undefined, error: undefined, response };
      }),
    },

    close: () => Promise.resolve(),
  };
}
