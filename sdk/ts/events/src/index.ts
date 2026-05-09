import { createEventClient, type EventClient } from "./client.js";

let _events: EventClient | undefined;

/**
 * Default Events client configured from environment variables.
 * Reads `BEYOND_EVENTS_URL` (required) and `BEYOND_EVENTS_TOKEN` (optional, defaults to `"anon"`).
 * Initialized lazily on first method call.
 */
export const events: EventClient = new Proxy({} as EventClient, {
  get(_, prop) {
    _events ??= createEventClient();
    return (_events as unknown as Record<string | symbol, unknown>)[prop];
  },
});

export { createEventClient } from "./client.js";
export type {
  EventClient,
  EventClientOptions,
  EventResult,
  EventTarget,
  JsonValue,
  PublishOptions,
  PublishResult,
  Subscription,
} from "./client.js";
export { EventError } from "./errors.js";
