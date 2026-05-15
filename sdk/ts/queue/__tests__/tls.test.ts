import "reflect-metadata";
import * as x509 from "@peculiar/x509";
import {
  PostgreSqlContainer,
  type StartedPostgreSqlContainer,
} from "@testcontainers/postgresql";
import { type ChildProcess, spawn } from "node:child_process";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import postgres from "postgres";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { createQueueClient } from "../src/client.js";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

// ── Cert generation ───────────────────────────────────────────────────────────

interface TestCerts {
  caPem: string;
  serverCertPem: string;
  serverKeyPem: string;
  clientCertPem: string;
  clientKeyPem: string;
}

function toPem(label: string, der: ArrayBuffer): string {
  const b64 = Buffer.from(der).toString("base64");
  const lines = b64.match(/.{1,64}/g)!.join("\n");
  return `-----BEGIN ${label}-----\n${lines}\n-----END ${label}-----\n`;
}

async function generateTestCerts(): Promise<TestCerts> {
  const alg = { name: "ECDSA", namedCurve: "P-256" };

  const caKeys = await crypto.subtle.generateKey(alg, true, ["sign", "verify"]);
  const caCert = await x509.X509CertificateGenerator.createSelfSigned({
    keys: caKeys,
    name: "CN=Test CA",
    notBefore: new Date("2020-01-01"),
    notAfter: new Date("2099-12-31"),
    signingAlgorithm: { name: "ECDSA", hash: "SHA-256" },
    extensions: [new x509.BasicConstraintsExtension(true, undefined, true)],
  });

  const serverKeys = await crypto.subtle.generateKey(alg, true, [
    "sign",
    "verify",
  ]);
  const serverCert = await x509.X509CertificateGenerator.create({
    subject: "CN=localhost",
    issuer: caCert.subject,
    publicKey: serverKeys.publicKey,
    signingKey: caKeys.privateKey,
    notBefore: new Date("2020-01-01"),
    notAfter: new Date("2099-12-31"),
    signingAlgorithm: { name: "ECDSA", hash: "SHA-256" },
    extensions: [
      new x509.SubjectAlternativeNameExtension([
        { type: "dns", value: "localhost" },
        { type: "ip", value: "127.0.0.1" },
      ]),
      new x509.ExtendedKeyUsageExtension([
        "1.3.6.1.5.5.7.3.1",
        "1.3.6.1.5.5.7.3.2",
      ]),
    ],
  });

  const clientKeys = await crypto.subtle.generateKey(alg, true, [
    "sign",
    "verify",
  ]);
  const clientCert = await x509.X509CertificateGenerator.create({
    subject: "CN=client",
    issuer: caCert.subject,
    publicKey: clientKeys.publicKey,
    signingKey: caKeys.privateKey,
    notBefore: new Date("2020-01-01"),
    notAfter: new Date("2099-12-31"),
    signingAlgorithm: { name: "ECDSA", hash: "SHA-256" },
    extensions: [
      new x509.ExtendedKeyUsageExtension(["1.3.6.1.5.5.7.3.2"]),
    ],
  });

  const serverKeyDer = await crypto.subtle.exportKey(
    "pkcs8",
    serverKeys.privateKey,
  );
  const clientKeyDer = await crypto.subtle.exportKey(
    "pkcs8",
    clientKeys.privateKey,
  );

  return {
    caPem: caCert.toString("pem"),
    serverCertPem: serverCert.toString("pem"),
    serverKeyPem: toPem("PRIVATE KEY", serverKeyDer),
    clientCertPem: clientCert.toString("pem"),
    clientKeyPem: toPem("PRIVATE KEY", clientKeyDer),
  };
}

// ── Port / health helpers ─────────────────────────────────────────────────────

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

async function waitForHealthy(
  url: string,
  fetchFn: typeof globalThis.fetch,
  timeoutMs = 30_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetchFn(`${url}/livez`);
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

// ── Test suite ────────────────────────────────────────────────────────────────

let serverProcess: ChildProcess | undefined;
let container: StartedPostgreSqlContainer | undefined;
let tlsUrl: string;
let certs: TestCerts;
let tmpDir: string;

beforeAll(async () => {
  // Generate certs
  certs = await generateTestCerts();

  // Write certs + key to a temp directory (server needs file paths)
  tmpDir = join(tmpdir(), `beyond-tls-test-${process.pid}`);
  mkdirSync(tmpDir, { recursive: true });
  const caPath = join(tmpDir, "ca.pem");
  const serverCertPath = join(tmpDir, "server.crt");
  const serverKeyPath = join(tmpDir, "server.key");
  writeFileSync(caPath, certs.caPem);
  writeFileSync(serverCertPath, certs.serverCertPem);
  writeFileSync(serverKeyPath, certs.serverKeyPem);

  // Start PostgreSQL container
  container = await new PostgreSqlContainer("postgres:17").start();
  const databaseUrl = container.getConnectionUri();

  // Provision schema
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

  // Find a free port and spawn the server with TLS
  const httpsPort = await findFreePort();
  const binaryPath = process.env["BEYOND_QUEUE_BINARY"]
    ?? resolve(__dirname, "../../../../target/debug/beyond-queue");

  serverProcess = spawn(binaryPath, ["serve"], {
    env: {
      ...process.env,
      DATABASE_URL: databaseUrl,
      ADDRESS: `127.0.0.1:${httpsPort}`,
      RUST_LOG: "error",
      BEYOND_TLS_CERT: serverCertPath,
      BEYOND_TLS_KEY: serverKeyPath,
      BEYOND_TLS_CA: caPath,
    },
    stdio: ["pipe", "pipe", "inherit"],
  });

  serverProcess.on("error", (err) => {
    throw new Error(`Failed to start beyond-queue with TLS: ${err.message}`);
  });

  tlsUrl = `https://127.0.0.1:${httpsPort}`;

  // Health checks also need a full mTLS client (server requires client cert on every conn).
  const { fetch: undiciFetch, Agent } = await import("undici");
  const healthAgent = new Agent({
    allowH2: true,
    connect: {
      ca: [certs.caPem],
      cert: certs.clientCertPem,
      key: certs.clientKeyPem,
    },
  });
  const healthFetch = (url: RequestInfo | URL, init?: RequestInit) =>
    (undiciFetch as unknown as typeof globalThis.fetch)(url, {
      ...(init ?? {}),
      dispatcher: healthAgent,
    } as RequestInit);

  await waitForHealthy(tlsUrl, healthFetch);
}, 120_000);

afterAll(async () => {
  serverProcess?.kill("SIGTERM");
  serverProcess = undefined;
  await container?.stop();
  container = undefined;
  if (tmpDir) {
    try {
      rmSync(tmpDir, { recursive: true, force: true });
    } catch {
      // best-effort cleanup
    }
  }
});

describe("mTLS client", () => {
  it("succeeds when TLS options with valid client cert are provided", async () => {
    const client = createQueueClient({
      url: tlsUrl,
      tls: {
        ca: certs.caPem,
        cert: certs.clientCertPem,
        key: certs.clientKeyPem,
      },
    });

    const result = await client.queues.list();

    expect(result.error).toBeUndefined();
    expect(result.data).toBeInstanceOf(Array);
  });

  it("fails when no TLS options are provided (plain fetch, TLS server)", async () => {
    const client = createQueueClient({ url: tlsUrl });

    await expect(client.queues.list()).rejects.toThrow();
  });
});
