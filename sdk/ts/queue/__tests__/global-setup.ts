import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import { type ChildProcess, spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { createServer } from "node:net";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import postgres from "postgres";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

let serverProcess: ChildProcess | undefined;
let container: StartedPostgreSqlContainer | undefined;

function findFreePort(): Promise<number> {
  return new Promise((res, rej) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const { port } = srv.address() as { port: number };
      srv.close((err) => (err ? rej(err) : res(port)));
    });
    srv.on("error", rej);
  });
}

async function waitForHealthy(url: string, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${url}/livez`);
      if (res.ok) return;
    } catch {
      // server not up yet
    }
    await new Promise<void>((r) => setTimeout(r, 150));
  }
  throw new Error(
    `beyond-queue did not become healthy at ${url} within ${timeoutMs}ms`,
  );
}

export async function setup(): Promise<void> {
  const httpPort = await findFreePort();

  container = await new PostgreSqlContainer("postgres:17").start();
  const databaseUrl = container.getConnectionUri();

  const sql = postgres(databaseUrl, { max: 1 });
  const schemaPath = resolve(
    __dirname,
    "../../../../beyond-queue-extension/sql/schema.sql",
  );
  const hotPathsPath = resolve(
    __dirname,
    "../../../../tests/fixtures/hot_paths.sql",
  );
  await sql.unsafe(readFileSync(schemaPath, "utf8"));
  await sql.unsafe(readFileSync(hotPathsPath, "utf8"));
  await sql.end();

  const binaryPath = process.env["BEYOND_QUEUE_BINARY"]
    ?? resolve(__dirname, "../../../../target/debug/beyond-queue");

  serverProcess = spawn(binaryPath, ["serve"], {
    env: {
      ...process.env,
      DATABASE_URL: databaseUrl,
      ADDRESS: `127.0.0.1:${httpPort}`,
      RUST_LOG: "error",
    },
    stdio: ["pipe", "pipe", "inherit"],
  });

  serverProcess.on("error", (err) => {
    throw new Error(`Failed to start beyond-queue: ${err.message}`);
  });

  const baseUrl = `http://127.0.0.1:${httpPort}`;
  await waitForHealthy(baseUrl);

  process.env["QUEUE_TEST_URL"] = baseUrl;
}

export async function teardown(): Promise<void> {
  serverProcess?.kill("SIGTERM");
  serverProcess = undefined;
  await container?.stop();
  container = undefined;
}
