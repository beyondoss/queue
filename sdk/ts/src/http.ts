import type { QueueClient, QueueClientOptions } from "./client.js";
import { QueueError, QueueNotFoundError } from "./errors.js";
import type {
  BatchEntry,
  CreateQueueOptions,
  JsonValue,
  Message,
  PublishOptions,
  PublishResult,
  Queue,
  QueueStats,
  ReceiveOptions,
  SendOptions,
  Subscription,
} from "./types.js";

export function createHttpQueueClient(opts: QueueClientOptions): QueueClient {
  const base = `${opts.url.replace(/\/+$/, "")}/v1`;
  const auth = opts.auth ?? "Bearer anon";
  const retries = opts.retries ?? 2;
  const { timeout, onCommand, onResponse } = opts;
  const fetchFn = opts.fetch ?? globalThis.fetch;

  async function request(
    command: string,
    method: string,
    url: string,
    body?: unknown,
  ): Promise<Response> {
    onCommand?.({ command });
    const start = Date.now();

    const headers: Record<string, string> = { Authorization: auth };
    let bodyInit: BodyInit | undefined;
    if (body !== undefined) {
      headers["Content-Type"] = "application/json";
      bodyInit = JSON.stringify(body);
    }

    for (let attempt = 0; attempt <= retries; attempt++) {
      if (attempt > 0) {
        await new Promise<void>((r) => setTimeout(r, 100 * 2 ** (attempt - 1)));
      }
      const signal = timeout != null ? AbortSignal.timeout(timeout) : undefined;
      let res: Response;
      try {
        res = await fetchFn(url, {
          method,
          headers,
          ...(bodyInit !== undefined && { body: bodyInit }),
          ...(signal != null && { signal }),
        });
      } catch (err) {
        if (attempt >= retries) {
          onResponse?.({ command, durationMs: Date.now() - start });
          throw err;
        }
        continue;
      }
      if (res.status >= 500 && attempt < retries) {
        await res.body?.cancel();
        continue;
      }
      onResponse?.({ command, durationMs: Date.now() - start });
      return res;
    }
    throw new Error("unreachable");
  }

  async function parseError(res: Response): Promise<QueueError> {
    let code = "internal_error";
    let message = res.statusText;
    try {
      const body = (await res.json()) as { code?: string; message?: string };
      if (body.code) code = body.code;
      if (body.message) message = body.message;
    } catch {
      /* ignore */
    }
    return new QueueError(code, message, res.status);
  }

  function queueUrl(name: string): string {
    return `${base}/queues/${encodeURIComponent(name)}`;
  }

  return {
    async createQueue(name: string, qOpts?: CreateQueueOptions) {
      const body: Record<string, unknown> = { name };
      if (qOpts?.fifo != null) body["fifo"] = qOpts.fifo;
      const res = await request("createQueue", "POST", `${base}/queues`, body);
      if (!res.ok) throw await parseError(res);
      // Server returns 201 with no body; derive the URL from the base and name.
      await res.body?.cancel();
      return { queue_url: `${base}/queues/${encodeURIComponent(name)}` };
    },

    async listQueues() {
      const res = await request("listQueues", "GET", `${base}/queues`);
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as Queue[];
    },

    async getQueueStats(name: string) {
      const res = await request("getQueueStats", "GET", queueUrl(name));
      if (res.status === 404) throw new QueueNotFoundError(name);
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as QueueStats;
    },

    async deleteQueue(name: string) {
      const res = await request("deleteQueue", "DELETE", queueUrl(name));
      if (res.status === 204 || res.status === 404) return;
      if (!res.ok) throw await parseError(res);
    },

    async purgeQueue(name: string) {
      const res = await request(
        "purgeQueue",
        "POST",
        `${queueUrl(name)}/purge`,
      );
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as { deleted: number };
    },

    async sendMessage(queue: string, message: JsonValue, sOpts?: SendOptions) {
      const url = sOpts?.async_commit === true
        ? `${queueUrl(queue)}/messages?async_commit=true`
        : `${queueUrl(queue)}/messages`;
      const body: Record<string, unknown> = {
        message,
        delay: sOpts?.delay ?? 0,
      };
      if (sOpts?.headers != null) body["headers"] = sOpts.headers;
      if (sOpts?.group_id != null) body["group_id"] = sOpts.group_id;
      const res = await request("sendMessage", "POST", url, body);
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as { id: number };
    },

    async sendBatch(
      queue: string,
      entries: BatchEntry[],
      bOpts?: { async_commit?: boolean },
    ) {
      const url = bOpts?.async_commit === true
        ? `${queueUrl(queue)}/messages?async_commit=true`
        : `${queueUrl(queue)}/messages`;
      const res = await request("sendBatch", "POST", url, entries);
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as { ids: number[] };
    },

    async receiveMessages(queue: string, rOpts?: ReceiveOptions) {
      const url = new URL(`${queueUrl(queue)}/messages`);
      if (rOpts?.max != null) url.searchParams.set("max", String(rOpts.max));
      if (rOpts?.wait != null) url.searchParams.set("wait", String(rOpts.wait));
      if (rOpts?.visibilityTimeout != null) {
        url.searchParams.set("vt", String(rOpts.visibilityTimeout));
      }
      if (rOpts?.fifo === true) url.searchParams.set("fifo", "true");
      const res = await request("receiveMessages", "GET", url.toString());
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as Message[];
    },

    async deleteMessage(queue: string, id: number) {
      const res = await request(
        "deleteMessage",
        "DELETE",
        `${queueUrl(queue)}/messages/${id}`,
      );
      if (res.status === 204 || res.status === 404) return;
      if (!res.ok) throw await parseError(res);
    },

    async deleteMessages(queue: string, ids: number[]) {
      const res = await request(
        "deleteMessages",
        "DELETE",
        `${queueUrl(queue)}/messages`,
        { ids },
      );
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as { deleted: number[] };
    },

    async changeVisibility(
      queue: string,
      id: number,
      visibilityTimeout: number,
    ) {
      const res = await request(
        "changeVisibility",
        "PATCH",
        `${queueUrl(queue)}/messages/${id}`,
        { vt: visibilityTimeout },
      );
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as { id: number; visible_at: string };
    },

    async publish(
      routingKey: string,
      message: JsonValue,
      pOpts?: PublishOptions,
    ) {
      const body: Record<string, unknown> = {
        message,
        delay: pOpts?.delay ?? 0,
      };
      if (pOpts?.headers != null) body["headers"] = pOpts.headers;
      const res = await request(
        "publish",
        "POST",
        `${base}/topics/${encodeURIComponent(routingKey)}`,
        body,
      );
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as PublishResult;
    },

    async subscribe(pattern: string, queueName: string) {
      const res = await request(
        "subscribe",
        "POST",
        `${base}/topics/${encodeURIComponent(pattern)}/subscriptions`,
        { queue_name: queueName },
      );
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as Subscription;
    },

    async subscribeHttp(
      pattern: string,
      endpoint: string,
      opts?: { envelope?: boolean },
    ) {
      const res = await request(
        "subscribeHttp",
        "POST",
        `${base}/topics/${encodeURIComponent(pattern)}/subscriptions`,
        {
          protocol: new URL(endpoint).protocol.replace(":", ""),
          endpoint,
          envelope: opts?.envelope ?? false,
        },
      );
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as Subscription;
    },

    async listTopicSubscriptions(pattern: string) {
      const res = await request(
        "listTopicSubscriptions",
        "GET",
        `${base}/topics/${encodeURIComponent(pattern)}/subscriptions`,
      );
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as Subscription[];
    },

    async listQueueSubscriptions(queueName: string) {
      const res = await request(
        "listQueueSubscriptions",
        "GET",
        `${base}/queues/${encodeURIComponent(queueName)}/subscriptions`,
      );
      if (!res.ok) throw await parseError(res);
      return (await res.json()) as Subscription[];
    },

    async unsubscribe(subscriptionId: number) {
      const res = await request(
        "unsubscribe",
        "DELETE",
        `${base}/topics/_/subscriptions/${subscriptionId}`,
      );
      if (res.status === 204 || res.status === 404) return;
      if (!res.ok) throw await parseError(res);
    },

    close(): Promise<void> {
      return Promise.resolve();
    },
  };
}
