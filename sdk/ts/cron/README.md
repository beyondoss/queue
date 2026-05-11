# @beyond.dev/cron

Run scheduled jobs inside your app. Define jobs with handlers, call `cron.start()`, and Beyond fires them on your schedule — waking your function from sleep as needed.

## Install

```sh
npm install @beyond.dev/cron
```

**Environment variables** (set automatically by the platform):

| Variable              | Description                         |
| --------------------- | ----------------------------------- |
| `BEYOND_QUEUE_URL`    | Beyond Queue server URL             |
| `BEYOND_QUEUE_TOKEN`  | Bearer token. Defaults to `"anon"`. |
| `BEYOND_INTERNAL_URL` | This function's own reachable URL   |

## Quick start

```ts
import { cron } from "@beyond.dev/cron";

await cron.start([
  cron.schedule({
    name: "daily-report", // globally unique across your beyond app
    when: "every weekday at 9am",
    timezone: "America/New_York",
    run: async (ctx) => {
      const rows = await db.query(
        "SELECT * FROM orders WHERE created_at >= $1",
        [ctx.scheduledFor],
      );
      await slack.post(`${rows.length} orders since yesterday`);
    },
  }),

  cron.schedule({
    name: "heartbeat",
    every: "30s",
    run: async () => {
      await ping();
    },
  }),
]);
```

That's the whole file. `cron.start()` registers your schedules, wires up HTTP delivery, cleans up after previous deployments, and blocks until the process exits. Any schedules from a previous deployment that aren't in this list are removed automatically.

## Handler context

```ts
cron.schedule({
  name: "report",
  every: "1h",
  run: async (ctx) => {
    ctx.name; // "report"
    ctx.scheduledFor; // ISO-8601 — when this fire was due
    ctx.outOfBand; // true when triggered via cron.schedules.run(), false for scheduled fires
  },
});
```

## Schedule expressions

Four mutually exclusive formats:

| Format           | Example                          |
| ---------------- | -------------------------------- |
| Natural language | `when: "every weekday at 9am"`   |
| Interval         | `every: "30s"` / `"5m"` / `"2h"` |
| Cron             | `cron: "0 9 * * 1-5"`            |
| One-shot         | `fireAt: "2026-06-01T09:00:00Z"` |

Default timezone is UTC. Pass `timezone` with any IANA name to override.

Preview an expression before committing:

```ts
import { cron } from "@beyond.dev/cron";

const { data } = await cron.schedules.preview({
  when: "every weekday at 9am",
  timezone: "America/New_York",
});

data.humanReadable; // "At 09:00 AM, Monday through Friday"
data.nextFires; // array of upcoming UTC timestamps
```

## Managing schedules

For schedules that send to a queue or topic rather than running a handler:

```ts
import { cron } from "@beyond.dev/cron";

const spec = cron.schedule({
  name: "nightly-sync",
  cron: "0 2 * * *",
  target: { queue: "sync-jobs", message: { task: "full-sync" } },
});

await cron.schedules.upsert(spec);

// Read
await cron.schedules.list({ status: "active", namePrefix: "nightly-" });
await cron.schedules.get("nightly-sync");

// Control
await cron.schedules.pause("nightly-sync");
await cron.schedules.resume("nightly-sync");
await cron.schedules.run("nightly-sync"); // immediate out-of-band fire
await cron.schedules.delete("nightly-sync");
```

## Advanced schedule options

```ts
cron.schedule({
  name: "nightly-sync",
  cron: "0 2 * * *",
  catchup: true,        // backfill fires missed while paused (default: false)
  catchupLimit: 5,      // max backfill fires per pass (default: 100)
  failureThreshold: 3,  // auto-pause after N consecutive failures (default: 3)
  jitterSecs: 60,       // spread fires by up to N random seconds (default: 0)
  run: async (ctx) => { ... },
});
```

## Observability

```ts
import { cron } from "@beyond.dev/cron";

cron.configure({
  onRequest: ({ command }) => console.log("→", command),
  onResponse: ({ command, durationMs }) =>
    console.log("←", command, `${durationMs}ms`),
});

await cron.start([...]);
```

## Error handling

Operations never throw. Every method returns `{ data, error }`:

```ts
const { data, error } = await cron.schedules.get("daily-report");

if (error) {
  console.error(error.code); // machine-readable, e.g. "not_found"
  console.error(error.message); // human-readable
  console.error(error.status); // HTTP status
  console.error(error.hint); // actionable guidance when available
} else {
  console.log(data.name, data.nextFires);
}
```

Inputs are camelCase (`catchupLimit`, `failureThreshold`). Responses are automatically camelCased (`next_fires` → `nextFires`, `fire_count` → `fireCount`).
