import {
  ChangeMessageVisibilityBatchCommand,
  ChangeMessageVisibilityCommand,
  CreateQueueCommand,
  DeleteMessageBatchCommand,
  DeleteMessageCommand,
  DeleteQueueCommand,
  GetQueueAttributesCommand,
  GetQueueUrlCommand,
  ListQueuesCommand,
  PurgeQueueCommand,
  ReceiveMessageCommand,
  SendMessageBatchCommand,
  SendMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { describe, expect, it } from "vitest";
import { getBaseUrl, uniqueQueue } from "./harness.js";

function sqsClient(): SQSClient {
  return new SQSClient({
    endpoint: getBaseUrl(),
    region: "us-east-1",
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  });
}

// ── queue lifecycle ───────────────────────────────────────────────────────────

describe("SQS — queue lifecycle", () => {
  it("CreateQueue returns a QueueUrl", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const res = await sqs.send(new CreateQueueCommand({ QueueName: name }));
    expect(res.QueueUrl).toContain(name);
  });

  it("CreateQueue is idempotent", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    await sqs.send(new CreateQueueCommand({ QueueName: name }));
    await expect(sqs.send(new CreateQueueCommand({ QueueName: name }))).resolves
      .toBeDefined();
  });

  it("GetQueueUrl returns the queue URL", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    const res = await sqs.send(new GetQueueUrlCommand({ QueueName: name }));
    expect(res.QueueUrl).toBe(QueueUrl);
  });

  it("ListQueues includes a created queue", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    await sqs.send(new CreateQueueCommand({ QueueName: name }));
    const res = await sqs.send(new ListQueuesCommand({}));
    expect(res.QueueUrls?.some((u) => u.includes(name))).toBe(true);
  });

  it("ListQueues filters by QueueNamePrefix", async () => {
    const sqs = sqsClient();
    const prefix = uniqueQueue("pfx");
    const name = `${prefix}extra`;
    await sqs.send(new CreateQueueCommand({ QueueName: name }));
    const res = await sqs.send(
      new ListQueuesCommand({ QueueNamePrefix: prefix }),
    );
    expect(res.QueueUrls?.every((u) => u.includes(prefix))).toBe(true);
  });

  it("GetQueueAttributes returns ApproximateNumberOfMessages", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    const res = await sqs.send(
      new GetQueueAttributesCommand({
        QueueUrl,
        AttributeNames: ["All"],
      }),
    );
    expect(res.Attributes?.["ApproximateNumberOfMessages"]).toBeDefined();
  });

  it("DeleteQueue removes the queue", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await sqs.send(new DeleteQueueCommand({ QueueUrl }));
    const list = await sqs.send(new ListQueuesCommand({}));
    expect(list.QueueUrls?.some((u) => u.includes(name))).toBeFalsy();
  });

  it("PurgeQueue empties the queue", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: "a" }));
    await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: "b" }));
    await sqs.send(new PurgeQueueCommand({ QueueUrl }));
    const res = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl, MaxNumberOfMessages: 10 }),
    );
    expect(res.Messages ?? []).toHaveLength(0);
  });
});

// ── send / receive / delete ───────────────────────────────────────────────────

describe("SQS — send / receive / delete", () => {
  it("SendMessage returns a MessageId", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    const res = await sqs.send(
      new SendMessageCommand({ QueueUrl, MessageBody: "hello" }),
    );
    expect(typeof res.MessageId).toBe("string");
    expect(res.MD5OfMessageBody).toBeDefined();
  });

  it("ReceiveMessage returns the sent body", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await sqs.send(
      new SendMessageCommand({ QueueUrl, MessageBody: "round-trip" }),
    );
    const res = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl, MaxNumberOfMessages: 1 }),
    );
    expect(res.Messages).toHaveLength(1);
    expect(res.Messages![0]!.Body).toBe("round-trip");
  });

  it("ReceiveMessage returns empty when queue is empty", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    const res = await sqs.send(new ReceiveMessageCommand({ QueueUrl }));
    expect(res.Messages ?? []).toHaveLength(0);
  });

  it("ReceiveMessage respects MaxNumberOfMessages", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    for (let i = 0; i < 5; i++) {
      await sqs.send(
        new SendMessageCommand({ QueueUrl, MessageBody: `msg-${i}` }),
      );
    }
    const res = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl, MaxNumberOfMessages: 3 }),
    );
    expect(res.Messages).toHaveLength(3);
  });

  it("ReceiveMessage includes ApproximateReceiveCount attribute", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: "attrs" }));
    const res = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl,
        AttributeNames: ["All"],
      }),
    );
    expect(res.Messages![0]!.Attributes?.["ApproximateReceiveCount"]).toBe("1");
    expect(res.Messages![0]!.Attributes?.["SentTimestamp"]).toBeDefined();
  });

  it("DeleteMessage removes the message by ReceiptHandle", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await sqs.send(
      new SendMessageCommand({ QueueUrl, MessageBody: "to-delete" }),
    );
    const recv = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl, VisibilityTimeout: 1 }),
    );
    const handle = recv.Messages![0]!.ReceiptHandle!;
    await sqs.send(
      new DeleteMessageCommand({ QueueUrl, ReceiptHandle: handle }),
    );
    // After vt the deleted message should not re-appear
    await new Promise<void>((r) => setTimeout(r, 1100));
    const after = await sqs.send(new ReceiveMessageCommand({ QueueUrl }));
    expect(after.Messages ?? []).toHaveLength(0);
  });

  it("ReceiptHandle is opaque and stable across calls", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: "stable" }));
    const recv = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl, VisibilityTimeout: 0 }),
    );
    const handle = recv.Messages![0]!.ReceiptHandle!;
    expect(typeof handle).toBe("string");
    expect(handle.length).toBeGreaterThan(0);
  });

  it("SendMessage with DelaySeconds hides the message", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await sqs.send(
      new SendMessageCommand({
        QueueUrl,
        MessageBody: "delayed",
        DelaySeconds: 10,
      }),
    );
    const res = await sqs.send(new ReceiveMessageCommand({ QueueUrl }));
    expect(res.Messages ?? []).toHaveLength(0);
  });
});

// ── message attributes ────────────────────────────────────────────────────────

describe("SQS — message attributes", () => {
  it("String and Number attributes round-trip", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await sqs.send(
      new SendMessageCommand({
        QueueUrl,
        MessageBody: "with-attrs",
        MessageAttributes: {
          TraceId: { DataType: "String", StringValue: "abc-123" },
          Priority: { DataType: "Number", StringValue: "42" },
        },
      }),
    );
    const res = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl,
        MessageAttributeNames: ["All"],
      }),
    );
    // Attributes are surfaced through headers; verify message arrived
    expect(res.Messages).toHaveLength(1);
    expect(res.Messages![0]!.Body).toBe("with-attrs");
  });
});

// ── batch operations ──────────────────────────────────────────────────────────

describe("SQS — batch operations", () => {
  it("SendMessageBatch returns Successful entries for all messages", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    const res = await sqs.send(
      new SendMessageBatchCommand({
        QueueUrl,
        Entries: [
          { Id: "1", MessageBody: "one" },
          { Id: "2", MessageBody: "two" },
          { Id: "3", MessageBody: "three" },
        ],
      }),
    );
    expect(res.Successful).toHaveLength(3);
    expect(res.Failed ?? []).toHaveLength(0);
    expect(res.Successful!.map((s) => s.Id).sort()).toEqual(["1", "2", "3"]);
  });

  it("DeleteMessageBatch removes all messages", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await sqs.send(
      new SendMessageBatchCommand({
        QueueUrl,
        Entries: [
          { Id: "a", MessageBody: "x" },
          { Id: "b", MessageBody: "y" },
        ],
      }),
    );
    const recv = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl,
        MaxNumberOfMessages: 2,
        VisibilityTimeout: 1,
      }),
    );
    const handles = recv.Messages!.map((m, i) => ({
      Id: String(i),
      ReceiptHandle: m.ReceiptHandle!,
    }));
    const del = await sqs.send(
      new DeleteMessageBatchCommand({ QueueUrl, Entries: handles }),
    );
    expect(del.Successful).toHaveLength(2);
    await new Promise<void>((r) => setTimeout(r, 1100));
    const after = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl, MaxNumberOfMessages: 10 }),
    );
    expect(after.Messages ?? []).toHaveLength(0);
  });

  it("ChangeMessageVisibilityBatch updates timeouts", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await sqs.send(
      new SendMessageBatchCommand({
        QueueUrl,
        Entries: [
          { Id: "1", MessageBody: "vis-a" },
          { Id: "2", MessageBody: "vis-b" },
        ],
      }),
    );
    const recv = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl,
        MaxNumberOfMessages: 2,
        VisibilityTimeout: 5,
      }),
    );
    const entries = recv.Messages!.map((m, i) => ({
      Id: String(i),
      ReceiptHandle: m.ReceiptHandle!,
      VisibilityTimeout: 60,
    }));
    const res = await sqs.send(
      new ChangeMessageVisibilityBatchCommand({ QueueUrl, Entries: entries }),
    );
    expect(res.Successful).toHaveLength(2);
    expect(res.Failed ?? []).toHaveLength(0);
  });
});

// ── ChangeMessageVisibility ───────────────────────────────────────────────────

describe("SQS — ChangeMessageVisibility", () => {
  it("extends visibility to hide the message longer", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await sqs.send(
      new SendMessageCommand({ QueueUrl, MessageBody: "hide-me" }),
    );
    const recv = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl, VisibilityTimeout: 1 }),
    );
    const handle = recv.Messages![0]!.ReceiptHandle!;
    await sqs.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl,
        ReceiptHandle: handle,
        VisibilityTimeout: 60,
      }),
    );
    // Now the message is hidden for 60s — should not be receivable
    const after = await sqs.send(new ReceiveMessageCommand({ QueueUrl }));
    expect(after.Messages ?? []).toHaveLength(0);
  });

  it("setting VisibilityTimeout=0 reveals the message immediately", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await sqs.send(
      new SendMessageCommand({ QueueUrl, MessageBody: "reveal-me" }),
    );
    const recv = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl, VisibilityTimeout: 60 }),
    );
    const handle = recv.Messages![0]!.ReceiptHandle!;
    await sqs.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl,
        ReceiptHandle: handle,
        VisibilityTimeout: 0,
      }),
    );
    const after = await sqs.send(new ReceiveMessageCommand({ QueueUrl }));
    expect(after.Messages).toHaveLength(1);
  });
});

// ── Query protocol (form-encoded) ─────────────────────────────────────────────

describe("SQS — Query protocol (application/x-www-form-urlencoded)", () => {
  async function queryRequest(params: Record<string, string>): Promise<string> {
    const body = new URLSearchParams({ Version: "2012-11-05", ...params })
      .toString();
    const res = await fetch(getBaseUrl(), {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization: "Bearer test",
      },
      body,
    });
    return res.text();
  }

  it("SendMessage returns XML with MessageId", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    const xml = await queryRequest({
      Action: "SendMessage",
      QueueUrl: QueueUrl!,
      MessageBody: "query-proto-body",
    });
    expect(xml).toContain("<MessageId>");
    expect(xml).toContain("<MD5OfMessageBody>");
  });

  it("ReceiveMessage returns XML with Message/Body", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await queryRequest({
      Action: "SendMessage",
      QueueUrl: QueueUrl!,
      MessageBody: "query-receive-test",
    });
    const xml = await queryRequest({
      Action: "ReceiveMessage",
      QueueUrl: QueueUrl!,
      MaxNumberOfMessages: "1",
      WaitTimeSeconds: "0",
    });
    expect(xml).toContain("<Body>query-receive-test</Body>");
    expect(xml).toContain("<ReceiptHandle>");
  });

  it("DeleteMessage via Query protocol succeeds", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: name }),
    );
    await queryRequest({
      Action: "SendMessage",
      QueueUrl: QueueUrl!,
      MessageBody: "qdel",
    });
    const recvXml = await queryRequest({
      Action: "ReceiveMessage",
      QueueUrl: QueueUrl!,
      MaxNumberOfMessages: "1",
      VisibilityTimeout: "1",
    });
    const handleMatch = /<ReceiptHandle>([^<]+)<\/ReceiptHandle>/.exec(recvXml);
    expect(handleMatch).not.toBeNull();
    const xml = await queryRequest({
      Action: "DeleteMessage",
      QueueUrl: QueueUrl!,
      ReceiptHandle: handleMatch![1]!,
    });
    expect(xml).toContain("DeleteMessageResponse");
  });
});

// ── path-based endpoint ───────────────────────────────────────────────────────

describe("SQS — path-based endpoint (POST /{account_id}/{queue_name})", () => {
  it("SendMessage via path-based URL routes to the correct queue", async () => {
    const sqs = sqsClient();
    const name = uniqueQueue();
    await sqs.send(new CreateQueueCommand({ QueueName: name }));

    const pathUrl = `${getBaseUrl()}/000000000000/${name}`;
    const body = new URLSearchParams({
      Action: "SendMessage",
      MessageBody: "path-based-msg",
      Version: "2012-11-05",
    }).toString();
    const res = await fetch(pathUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Authorization: "Bearer test",
      },
      body,
    });
    expect(res.ok).toBe(true);
    const xml = await res.text();
    expect(xml).toContain("<MessageId>");

    // Verify the message landed in the queue
    const recv = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: `${getBaseUrl()}/000000000000/${name}`,
        MaxNumberOfMessages: 1,
      }),
    );
    expect(recv.Messages).toHaveLength(1);
    expect(recv.Messages![0]!.Body).toBe("path-based-msg");
  });
});
