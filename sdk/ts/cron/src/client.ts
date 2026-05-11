import { createServer } from "node:http";
import createFetchClient from "openapi-fetch";
import { env } from "std-env";
import { CronError } from "./errors.js";
import type { components, paths } from "./types.js";
import { type Camelize, camelize, snakenize } from "./utils/camelize.js";

export { CronError } from "./errors.js";
export type { components, paths } from "./types.js";
export type { Camelize } from "./utils/camelize.js";

/** Port beyond listens on to receive schedule fires, workflow callbacks, and other platform events. Never used by user apps. */
const BEYOND_INTERNAL_RECEIVER_PORT = 52000;

/** Path prefix for all cron fire deliveries from beyond-queue. */
const BEYOND_CRON_PATH = "/__cron";

// ── Shared types ──────────────────────────────────────────────────────────────

export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

/**
 * Every client method returns this. Either `data` is set or `error` is set — never both.
 * Check `error` before using `data`. Never throws.
 */
export type CronResult<T = undefined> = Promise<
  | { data: T; error: undefined; response: Response }
  | { data: undefined; error: CronError; response: Response }
>;

// ── Public SDK types ──────────────────────────────────────────────────────────

export type Schedule = Camelize<components["schemas"]["Schedule"]>;
export type Preview = Camelize<components["schemas"]["Preview"]>;
export type RunResult = Camelize<components["schemas"]["RunResult"]>;

/** Camelized schedule spec for use with `cron.schedules.*`. */
export type ScheduleSpec = Camelize<components["schemas"]["ScheduleSpec"]>;

/** Camelized preview input for `cron.schedules.preview()`. */
export type PreviewSpec = Camelize<components["schemas"]["PreviewSpec"]>;

/** Schedule spec without `target` — the SDK manages the internal topic target. */
export type CronJobSpec = Omit<ScheduleSpec, "target">;

/**
 * A job definition passed to `schedule()`. All fields from `CronJobSpec` plus `run`.
 *
 * `name` is globally unique across your entire beyond app — it is the primary key.
 * Changing it creates a new schedule and orphans the old one.
 */
export type CronJobDef = CronJobSpec & {
  run: (ctx: CronContext) => Promise<void>;
};

/** Compiled job returned by `schedule()`. Pass an array of these to `start()`. */
export interface CronJob {
  readonly name: string;
  readonly spec: CronJobSpec;
  readonly handler: (ctx: CronContext) => Promise<void>;
}

export interface CronContext {
  /** The schedule name — the same value you passed to `schedule({ name })`. */
  name: string;
  /**
   * ISO-8601 timestamp of the intended fire time. Use this when computing what
   * data to process (e.g. "orders since last run") rather than `Date.now()`.
   */
  scheduledFor: string;
  /** `true` when triggered via `schedules.run()`, `false` for normal scheduled fires. */
  outOfBand: boolean;
}

/** Filter for `schedules.list()`. All fields are optional and ANDed together. */
export interface ScheduleFilter {
  /** Only return schedules with this status. */
  status?: "active" | "paused";
  /** Only return schedules targeting this delivery kind. */
  targetKind?: "queue" | "topic" | "workflow";
  /** Only return schedules whose name starts with this string. */
  namePrefix?: string;
}

/** Passed to `onRequest` before each API call. */
export interface CronRequestEvent {
  /** Method name, e.g. `"schedules.upsert"`. */
  command: string;
}

/** Passed to `onResponse` after each API call. */
export interface CronResponseEvent {
  /** Method name, e.g. `"schedules.upsert"`. */
  command: string;
  /** Wall-clock milliseconds from request start to response. */
  durationMs: number;
}

/** Observability hooks for the `cron` singleton. Pass to `cron.configure()`. */
export interface CronHooks {
  /** Called before each API request — use for logging, tracing, or metrics. */
  onRequest?: (event: CronRequestEvent) => void;
  /** Called after each API response — use for logging, tracing, or metrics. */
  onResponse?: (event: CronResponseEvent) => void;
}

/** Full client options — for `createCronClient()` only. */
export interface CronClientOptions extends CronHooks {
  url?: string;
  token?: string;
  fetch?: typeof globalThis.fetch;
  timeout?: number;
  retries?: number;
}

export interface StartOptions extends CronClientOptions {
  /** AbortSignal to stop the server and unblock `start()`. */
  signal?: AbortSignal;
  /**
   * Override the listen port. Defaults to `BEYOND_INTERNAL_RECEIVER_PORT` (52000).
   * Only useful in tests — production always uses the platform port.
   */
  port?: number;
}

// ── Client interface ──────────────────────────────────────────────────────────

export interface CronClient {
  schedules: {
    /** Create or replace a schedule (PUT semantics — idempotent). */
    upsert(spec: ScheduleSpec): CronResult<Schedule>;

    /**
     * Upsert all provided specs and delete any schedules not in the list.
     * Always declarative — the provided list is the desired state.
     */
    sync(
      specs: ScheduleSpec[],
    ): CronResult<{ upserted: number; removed: number }>;

    /** Dry-run a schedule expression. No schedule is created. */
    preview(input: PreviewSpec): CronResult<Preview>;

    /** List schedules, optionally filtered. */
    list(filter?: ScheduleFilter): CronResult<Schedule[]>;

    /** Get a single schedule by name. */
    get(name: string): CronResult<Schedule>;

    /** Pause a schedule (stops firing until resumed). */
    pause(name: string): CronResult<Schedule>;

    /** Resume a paused schedule. */
    resume(name: string): CronResult<Schedule>;

    /** Trigger an immediate out-of-band fire. */
    run(name: string): CronResult<RunResult>;

    /** Delete a schedule. Idempotent — no error if already gone. */
    delete(name: string): CronResult;
  };

  /** Release underlying connections. Call when the client is no longer needed. */
  close(): Promise<void>;
}

/**
 * The `cron` singleton. Configure hooks, define jobs, start the worker, and
 * manage schedules — all from one object.
 */
export interface CronSingleton extends CronClient {
  /**
   * Set observability hooks. Call before `cron.start()`.
   * URL and token are always read from platform environment variables.
   */
  configure(hooks: CronHooks): void;
  /** Define a job with a handler. Pass the result to `cron.start()`. */
  schedule(def: CronJobDef): CronJob;
  /** Pass through a full ScheduleSpec (with explicit target) for use with `cron.schedules.*`. */
  schedule(spec: ScheduleSpec): ScheduleSpec;
  /**
   * Start the cron worker. Registers schedules, subscribes this deployment's
   * URL, reconciles stale schedules, then blocks until `signal` aborts or the
   * process receives SIGTERM/SIGINT.
   *
   * The `jobs` array is the complete desired state — SDK-managed schedules not
   * in this list are deleted on startup.
   */
  start(jobs: CronJob[], opts?: { signal?: AbortSignal }): Promise<void>;
}

// ── Internal helpers ──────────────────────────────────────────────────────────

function toCronError(raw: unknown, response: Response): CronError {
  const inner = raw != null && typeof raw === "object" && "error" in raw
    ? (raw as { error: { code?: string; message?: string; hint?: string } })
      .error
    : (raw as { code?: string; message?: string; hint?: string } | undefined);
  const code = inner?.code ?? "internal_error";
  const message = response.status === 409
    ? `Schedule '${
      inner?.message ?? "unknown"
    }' already exists — name is globally unique across your beyond app`
    : (inner?.message ?? "Unknown error");
  return new CronError(code, message, response.status, response, inner?.hint);
}

function wrap<T>(
  promise: Promise<{ data?: T; error?: unknown; response: Response }>,
): CronResult<Camelize<T>> {
  return promise.then(({ data, error, response }) =>
    error !== undefined
      ? { data: undefined, error: toCronError(error, response), response }
      : { data: camelize(data) as Camelize<T>, error: undefined, response }
  ) as unknown as CronResult<Camelize<T>>;
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

// ── `schedule()` ─────────────────────────────────────────────────────────────

/** Define a job with a handler. The SDK manages the internal delivery target. */
export function schedule(def: CronJobDef): CronJob;
/** Pass through a full ScheduleSpec (with explicit target) for use with `cron.schedules.*`. */
export function schedule(spec: ScheduleSpec): ScheduleSpec;
export function schedule(
  def: CronJobDef | ScheduleSpec,
): CronJob | ScheduleSpec {
  if ("run" in def) {
    const { run, ...spec } = def;
    return { name: def.name, spec: spec as CronJobSpec, handler: run };
  }
  return def as ScheduleSpec;
}

// ── `start()` ────────────────────────────────────────────────────────────────

const VALID_JOB_NAME = /^[a-z0-9][a-z0-9_-]{0,63}$/;

/**
 * Start the cron worker: register schedules, subscribe this deployment's URL,
 * reconcile stale schedules from previous deployments, then block until
 * `opts.signal` aborts or the process receives SIGTERM/SIGINT.
 *
 * `jobs` is the **complete desired state**. Any SDK-managed schedule
 * (one with a `__cron_*` topic target) not present in `jobs` is deleted on
 * startup. Remove a job from the array to retire its schedule.
 *
 * @throws if `BEYOND_QUEUE_URL` or `BEYOND_INTERNAL_URL` are unset, if any
 *   job name fails validation (`[a-z0-9][a-z0-9_-]{0,63}`), or if two jobs
 *   share the same name.
 */
export async function start(
  jobs: CronJob[],
  opts?: StartOptions,
): Promise<void> {
  // Validate names early — they appear in topic names and URL paths.
  for (const job of jobs) {
    if (!VALID_JOB_NAME.test(job.name)) {
      throw new Error(
        `Invalid schedule name "${job.name}" — must match [a-z0-9][a-z0-9_-]{0,63}`,
      );
    }
  }
  const seen = new Set<string>();
  for (const job of jobs) {
    if (seen.has(job.name)) {
      throw new Error(`Duplicate schedule name "${job.name}"`);
    }
    seen.add(job.name);
  }

  const serverUrl = opts?.url ?? env["BEYOND_QUEUE_URL"];
  if (!serverUrl) {
    throw new Error(
      "BEYOND_QUEUE_URL is required (pass `url` or set the BEYOND_QUEUE_URL env var)",
    );
  }
  const appUrl = env["BEYOND_INTERNAL_URL"];
  if (!appUrl) {
    throw new Error(
      "BEYOND_INTERNAL_URL is required — the platform sets this automatically",
    );
  }

  const cronPath = BEYOND_CRON_PATH;
  const port = opts?.port ?? BEYOND_INTERNAL_RECEIVER_PORT;
  const serverBase = serverUrl.replace(/\/+$/, "");
  // Subscription endpoints always use the dedicated platform port, not the app port.
  const { protocol, hostname } = new URL(appUrl);
  const platformBase = `${protocol}//${hostname}:${port}`;
  const token = opts?.token ?? env["BEYOND_QUEUE_TOKEN"];

  const fetchFn = buildFetch(opts?.fetch, opts?.retries ?? 2, opts?.timeout);
  const client = createFetchClient<paths>({
    baseUrl: serverBase,
    headers: { Authorization: `Bearer ${token ?? "anon"}` },
    fetch: fetchFn,
  });

  // 1. Register schedules (PUT = upsert)
  for (const job of jobs) {
    const spec = snakenize({
      ...job.spec,
      name: job.name,
      target: { topic: `__cron_${job.name}`, message: {} },
    }) as components["schemas"]["ScheduleSpec"];
    await client.PUT("/v1/schedules/{name}", {
      params: { path: { name: job.name } },
      body: spec,
    });
  }

  // 2. Register subscriptions — one per job, pointing to current BEYOND_INTENRAL_URL
  for (const job of jobs) {
    const topic = `__cron_${job.name}`;
    const endpoint = `${platformBase}${cronPath}/${job.name}`;

    const { data: existing } = await client.GET(
      "/v1/events/{pattern}/subscriptions",
      { params: { path: { pattern: topic } } },
    );

    const subs = existing ?? [];

    // Delete stale subscriptions (HTTP/HTTPS pointing to a different URL)
    for (const sub of subs) {
      if (
        (sub.protocol === "http" || sub.protocol === "https")
        && sub.endpoint !== endpoint
      ) {
        await client.DELETE("/v1/events/{pattern}/subscriptions/{id}", {
          params: { path: { pattern: topic, id: sub.id } },
        });
      }
    }

    // Create subscription if not already present
    const alreadySubscribed = subs.some(
      (s) =>
        (s.protocol === "http" || s.protocol === "https")
        && s.endpoint === endpoint,
    );
    if (!alreadySubscribed) {
      await client.POST("/v1/events/{pattern}/subscriptions", {
        params: { path: { pattern: topic } },
        body: { protocol: "http", endpoint, envelope: false },
      });
    }
  }

  // 3. Reconcile — delete SDK-managed schedules not in this jobs list
  const jobNames = new Set(jobs.map((j) => j.name));
  const { data: allSchedules } = await client.GET("/v1/schedules", {
    params: { query: { target_kind: "topic" } },
  });

  for (const sched of allSchedules ?? []) {
    const target = sched.target;
    if (
      "topic" in target
      && typeof target.topic === "string"
      && target.topic.startsWith("__cron_")
      && !jobNames.has(sched.name)
    ) {
      await client.DELETE("/v1/schedules/{name}", {
        params: { path: { name: sched.name } },
      });
      // Best-effort: clean up the orphaned subscription
      await client.GET("/v1/events/{pattern}/subscriptions", {
        params: { path: { pattern: `__cron_${sched.name}` } },
      }).then(({ data: orphanSubs }) => {
        return Promise.all(
          (orphanSubs ?? []).map((s) =>
            client.DELETE("/v1/events/{pattern}/subscriptions/{id}", {
              params: {
                path: { pattern: `__cron_${sched.name}`, id: s.id },
              },
            })
          ),
        );
      });
    }
  }

  // 4. Start HTTP server
  const jobMap = new Map<string, CronJob>(jobs.map((j) => [j.name, j]));

  const server = createServer((req, res) => {
    const method = req.method ?? "";
    const pathname = req.url?.split("?")[0] ?? "/";

    if (method !== "POST") {
      res.writeHead(405);
      res.end();
      return;
    }

    if (!pathname.startsWith(cronPath + "/")) {
      res.writeHead(404);
      res.end();
      return;
    }

    const name = pathname.slice(cronPath.length + 1);
    const job = jobMap.get(name);
    if (!job) {
      res.writeHead(404);
      res.end();
      return;
    }

    // Consume request body
    const chunks: Buffer[] = [];
    req.on("data", (chunk: Buffer) => chunks.push(chunk));
    req.on("end", () => {
      const ctx: CronContext = {
        name,
        scheduledFor: new Date().toISOString(),
        outOfBand: false,
      };

      job.handler(ctx).then(
        () => {
          res.writeHead(200);
          res.end();
        },
        () => {
          res.writeHead(500);
          res.end();
        },
      );
    });
    req.on("error", () => {
      res.writeHead(500);
      res.end();
    });
  });

  await new Promise<void>((resolveServer, reject) => {
    server.listen(port, "0.0.0.0", () => resolveServer());
    server.on("error", reject);
  });

  // 5. Block until signal or process shutdown
  await new Promise<void>((resolve) => {
    let settled = false;

    const shutdown = () => {
      if (settled) return;
      settled = true;
      process.off("SIGTERM", shutdown);
      process.off("SIGINT", shutdown);
      server.closeAllConnections();
      server.close(() => resolve());
    };

    if (opts?.signal?.aborted) {
      shutdown();
      return;
    }

    opts?.signal?.addEventListener("abort", shutdown, { once: true });
    process.on("SIGTERM", shutdown);
    process.on("SIGINT", shutdown);
  });
}

// ── Factory ───────────────────────────────────────────────────────────────────

/**
 * Create a management client for schedules that target a queue or topic.
 * For handler-based jobs, use `start()` instead.
 *
 * @throws if `BEYOND_QUEUE_URL` is unset and `opts.url` is not provided.
 */
export function createCronClient(opts?: CronClientOptions): CronClient {
  const url = opts?.url ?? env["BEYOND_QUEUE_URL"];
  if (!url) {
    throw new Error(
      "BEYOND_QUEUE_URL is required (pass `url` or set the BEYOND_QUEUE_URL env var)",
    );
  }
  const base = url.replace(/\/+$/, "");
  const token = opts?.token ?? env["BEYOND_QUEUE_TOKEN"];
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
    schedules: {
      upsert: cmd("schedules.upsert", (spec) =>
        wrap(
          client.PUT("/v1/schedules/{name}", {
            params: { path: { name: spec.name } },
            body: snakenize(spec) as components["schemas"]["ScheduleSpec"],
          }),
        )),

      sync: cmd("schedules.sync", async (specs) => {
        const desired = new Map(specs.map((s) => [s.name, s]));

        // Upsert all
        await Promise.all(
          specs.map((spec) =>
            client.PUT("/v1/schedules/{name}", {
              params: { path: { name: spec.name } },
              body: snakenize(spec) as components["schemas"]["ScheduleSpec"],
            })
          ),
        );

        // List all and delete those not in desired set
        const { data: all, error, response } = await client.GET(
          "/v1/schedules",
          {},
        );
        if (error) {
          return {
            data: undefined,
            error: toCronError(error, response),
            response,
          };
        }

        const toDelete = (all ?? []).filter((s) => !desired.has(s.name));
        await Promise.all(
          toDelete.map((s) =>
            client.DELETE("/v1/schedules/{name}", {
              params: { path: { name: s.name } },
            })
          ),
        );

        return {
          data: { upserted: specs.length, removed: toDelete.length },
          error: undefined,
          response,
        };
      }),

      preview: cmd(
        "schedules.preview",
        (input) =>
          wrap(
            client.POST("/v1/previews", {
              body: snakenize(input) as components["schemas"]["PreviewSpec"],
            }),
          ),
      ),

      list: cmd("schedules.list", (filter) =>
        wrap(
          client.GET("/v1/schedules", {
            params: {
              query: {
                ...(filter?.status !== undefined && { status: filter.status }),
                ...(filter?.targetKind !== undefined && {
                  target_kind: filter.targetKind,
                }),
                ...(filter?.namePrefix !== undefined && {
                  name_prefix: filter.namePrefix,
                }),
              },
            },
          }),
        )),

      get: cmd("schedules.get", (name) =>
        wrap(
          client.GET("/v1/schedules/{name}", {
            params: { path: { name } },
          }),
        )),

      pause: cmd("schedules.pause", (name) =>
        wrap(
          client.PATCH("/v1/schedules/{name}", {
            params: { path: { name } },
            body: { status: "paused" },
          }),
        )),

      resume: cmd("schedules.resume", (name) =>
        wrap(
          client.PATCH("/v1/schedules/{name}", {
            params: { path: { name } },
            body: { status: "active" },
          }),
        )),

      run: cmd("schedules.run", (name) =>
        wrap(
          client.POST("/v1/schedules/{name}/runs", {
            params: { path: { name } },
          }),
        )),

      delete: cmd("schedules.delete", async (name) => {
        const { response } = await client.DELETE(
          "/v1/schedules/{name}",
          { params: { path: { name } } },
        );
        return { data: undefined, error: undefined, response };
      }),
    },

    close: () => Promise.resolve(),
  };
}
