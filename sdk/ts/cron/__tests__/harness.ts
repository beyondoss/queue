import { createServer } from "node:net";
import { createCronClient, type CronClient } from "../src/client.js";

export function getBaseUrl(): string {
  const url = process.env["QUEUE_TEST_URL"];
  if (!url) throw new Error("QUEUE_TEST_URL not set — is globalSetup running?");
  return url;
}

/** Cron client pointed at the test server. */
export function cronClient(): CronClient {
  return createCronClient({ url: getBaseUrl() });
}

/** Unique schedule name for test isolation. */
export function uniqueName(prefix = "s"): string {
  return `${prefix}-${Math.random().toString(36).slice(2, 10)}`;
}

/** Find a free TCP port on localhost. */
export function findFreePort(): Promise<number> {
  return new Promise((res, rej) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address() as { port: number };
      srv.close((err) => (err ? rej(err) : res(port)));
    });
    srv.on("error", rej);
  });
}
