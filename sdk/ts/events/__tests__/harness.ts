import { createEventClient, type EventClient } from "../src/client.js";

export function getBaseUrl(): string {
  const url = process.env["QUEUE_TEST_URL"];
  if (!url) throw new Error("QUEUE_TEST_URL not set — is globalSetup running?");
  return url;
}

/** Event client pointed at the test server. */
export function eventClient(): EventClient {
  return createEventClient({ url: getBaseUrl() });
}

/**
 * Unique name for test isolation (queues, routing keys, patterns).
 * Names must match `[a-z0-9_]`, max 48 chars.
 */
export function uniqueName(prefix = "e"): string {
  return `${prefix}_${Math.random().toString(36).slice(2, 10)}`;
}
