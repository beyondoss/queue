import {
  createCronClient,
  type CronClient,
  type CronHooks,
  type CronJob,
  type CronJobDef,
  type CronSingleton,
  schedule as _schedule,
  type ScheduleSpec,
  start as _start,
} from "./client.js";

let _hooks: CronHooks = {};
let _client: CronClient | undefined;

function getClient(): CronClient {
  _client ??= createCronClient(_hooks);
  return _client;
}

export const cron: CronSingleton = {
  configure(hooks: CronHooks) {
    _hooks = { ..._hooks, ...hooks };
    _client = undefined;
  },

  schedule(def: CronJobDef | ScheduleSpec): CronJob & ScheduleSpec {
    return _schedule(def as CronJobDef) as CronJob & ScheduleSpec;
  },

  async start(jobs: CronJob[], opts?: { signal?: AbortSignal }) {
    return _start(jobs, { ..._hooks, ...opts });
  },

  get schedules() {
    return getClient().schedules;
  },

  close() {
    return getClient().close();
  },
};

export {
  createCronClient,
  type CronClient,
  type CronContext,
  type CronHooks,
  type CronJob,
  type CronJobDef,
  type CronRequestEvent,
  type CronResponseEvent,
  type CronResult,
  type CronSingleton,
  type JsonValue,
  type Preview,
  type PreviewSpec,
  type RunResult,
  type Schedule,
  type ScheduleFilter,
  type ScheduleSpec,
  type StartOptions,
} from "./client.js";
export { CronError } from "./errors.js";
export type { components, paths } from "./types.js";
export type { Camelize } from "./utils/camelize.js";
