import { createQueueClient, type QueueClient } from "../src/client.js";

export function getBaseUrl(): string {
  const url = process.env["QUEUE_TEST_URL"];
  if (!url) throw new Error("QUEUE_TEST_URL not set — is globalSetup running?");
  return url;
}

/** Queue client pointed at the test server. */
export function queueClient(): QueueClient {
  return createQueueClient({ url: getBaseUrl() });
}

/**
 * Unique queue name for test isolation.
 * Queue names must match `[a-z0-9_]`, max 48 chars.
 */
export function uniqueQueue(prefix = "q"): string {
  return `${prefix}_${Math.random().toString(36).slice(2, 10)}`;
}
