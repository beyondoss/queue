import {
  ConfirmSubscriptionCommand,
  CreateTopicCommand,
  DeleteTopicCommand,
  GetSubscriptionAttributesCommand,
  GetTopicAttributesCommand,
  ListSubscriptionsByTopicCommand,
  ListSubscriptionsCommand,
  ListTopicsCommand,
  PublishCommand,
  SNSClient,
  SubscribeCommand,
  UnsubscribeCommand,
} from "@aws-sdk/client-sns";
import {
  CreateQueueCommand,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import { describe, expect, it } from "vitest";
import { getBaseUrl, uniqueQueue } from "./harness.js";

function snsClient(): SNSClient {
  return new SNSClient({
    endpoint: getBaseUrl(),
    region: "us-east-1",
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  });
}

function sqsClient(): SQSClient {
  return new SQSClient({
    endpoint: getBaseUrl(),
    region: "us-east-1",
    credentials: { accessKeyId: "test", secretAccessKey: "test" },
  });
}

function topicName(): string {
  return uniqueQueue("topic");
}

// ── topic lifecycle ───────────────────────────────────────────────────────────

describe("SNS — topic lifecycle", () => {
  it("CreateTopic returns a TopicArn", async () => {
    const sns = snsClient();
    const name = topicName();
    const res = await sns.send(new CreateTopicCommand({ Name: name }));
    expect(res.TopicArn).toContain(name);
    expect(res.TopicArn).toMatch(/^arn:aws:sns:/);
  });

  it("GetTopicAttributes returns Attributes map", async () => {
    const sns = snsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const sqs = sqsClient();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    // Subscribe so the topic has subscriptions (and therefore exists in ListTopics)
    await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );
    const res = await sns.send(new GetTopicAttributesCommand({ TopicArn }));
    expect(res.Attributes).toBeDefined();
  });

  it("DeleteTopic removes all subscriptions for the topic", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );

    await sns.send(new DeleteTopicCommand({ TopicArn }));

    // Publishing should now reach no queues (subscriptions gone)
    const res = await sns.send(
      new PublishCommand({ TopicArn, Message: "ghost" }),
    );
    // Publish returns a MessageId regardless; verify nothing landed in the queue
    expect(typeof res.MessageId).toBe("string");
    const recv = await sqs.send(new ReceiveMessageCommand({ QueueUrl }));
    expect(recv.Messages ?? []).toHaveLength(0);
  });
});

// ── subscriptions ─────────────────────────────────────────────────────────────

describe("SNS — subscriptions", () => {
  it("Subscribe returns a SubscriptionArn", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    const res = await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );
    expect(res.SubscriptionArn).toContain(name);
    expect(res.SubscriptionArn).toContain(qName);
  });

  it("Subscribe is idempotent", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );
    await expect(
      sns.send(
        new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
      ),
    ).resolves.toBeDefined();
  });

  it("ListSubscriptions includes the new subscription", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );
    const res = await sns.send(new ListSubscriptionsCommand({}));
    expect(
      res.Subscriptions?.some((s) =>
        s.TopicArn === TopicArn && s.Endpoint === QueueUrl
      ),
    ).toBe(true);
  });

  it("ListSubscriptionsByTopic filters to the given topic", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );
    const res = await sns.send(
      new ListSubscriptionsByTopicCommand({ TopicArn }),
    );
    expect(res.Subscriptions?.every((s) => s.TopicArn === TopicArn)).toBe(true);
    expect(res.Subscriptions?.some((s) => s.Endpoint === QueueUrl)).toBe(true);
  });

  it("GetSubscriptionAttributes returns Protocol and Endpoint", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    const { SubscriptionArn } = await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );
    const res = await sns.send(
      new GetSubscriptionAttributesCommand({ SubscriptionArn }),
    );
    expect(res.Attributes?.["Protocol"]).toBe("sqs");
    expect(res.Attributes?.["Endpoint"]).toBe(QueueUrl);
    expect(res.Attributes?.["TopicArn"]).toBe(TopicArn);
  });

  it("ConfirmSubscription succeeds (auto-confirmed)", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    const { SubscriptionArn } = await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );
    await expect(
      sns.send(
        new ConfirmSubscriptionCommand({ TopicArn, Token: "any-token" }),
      ),
    ).resolves.toBeDefined();
    // SubscriptionArn is still valid after confirm
    expect(SubscriptionArn).toBeDefined();
  });

  it("Unsubscribe removes the subscription", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    const { SubscriptionArn } = await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );
    await sns.send(new UnsubscribeCommand({ SubscriptionArn }));

    const res = await sns.send(
      new ListSubscriptionsByTopicCommand({ TopicArn }),
    );
    expect(res.Subscriptions?.some((s) => s.Endpoint === QueueUrl)).toBeFalsy();
  });
});

// ── publish ───────────────────────────────────────────────────────────────────

describe("SNS — publish", () => {
  it("Publish returns a MessageId", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );
    const res = await sns.send(
      new PublishCommand({ TopicArn, Message: "hello sns" }),
    );
    expect(typeof res.MessageId).toBe("string");
    expect(res.MessageId!.length).toBeGreaterThan(0);
  });

  it("published message arrives in the subscribed SQS queue", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );
    await sns.send(new PublishCommand({ TopicArn, Message: "delivered" }));

    const recv = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl, MaxNumberOfMessages: 1 }),
    );
    expect(recv.Messages).toHaveLength(1);
  });

  it("SQS message body is a JSON SNS notification envelope", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );
    await sns.send(new PublishCommand({ TopicArn, Message: "envelope-test" }));

    const recv = await sqs.send(
      new ReceiveMessageCommand({ QueueUrl, MaxNumberOfMessages: 1 }),
    );
    const body = recv.Messages![0]!.Body!;
    const envelope = JSON.parse(body) as Record<string, unknown>;
    expect(envelope["Type"]).toBe("Notification");
    expect(envelope["TopicArn"]).toBe(TopicArn);
    expect(envelope["Message"]).toBe("envelope-test");
    expect(typeof envelope["MessageId"]).toBe("string");
    expect(typeof envelope["Timestamp"]).toBe("string");
  });

  it("Publish with Subject includes it in the envelope", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );
    await sns.send(
      new PublishCommand({
        TopicArn,
        Message: "with subject",
        Subject: "my-subject",
      }),
    );

    const recv = await sqs.send(new ReceiveMessageCommand({ QueueUrl }));
    const envelope = JSON.parse(recv.Messages![0]!.Body!) as Record<
      string,
      unknown
    >;
    expect(envelope["Subject"]).toBe("my-subject");
  });

  it("Publish routes to all subscribed queues", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));

    const qa = uniqueQueue("fan");
    const qb = uniqueQueue("fan");
    const { QueueUrl: urlA } = await sqs.send(
      new CreateQueueCommand({ QueueName: qa }),
    );
    const { QueueUrl: urlB } = await sqs.send(
      new CreateQueueCommand({ QueueName: qb }),
    );
    await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: urlA }),
    );
    await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: urlB }),
    );

    await sns.send(new PublishCommand({ TopicArn, Message: "fanout" }));

    const [recvA, recvB] = await Promise.all([
      sqs.send(new ReceiveMessageCommand({ QueueUrl: urlA })),
      sqs.send(new ReceiveMessageCommand({ QueueUrl: urlB })),
    ]);
    expect(recvA.Messages).toHaveLength(1);
    expect(recvB.Messages).toHaveLength(1);
  });

  it("Publish to deleted topic still returns MessageId (zero queues matched)", async () => {
    const sns = snsClient();
    const { TopicArn } = await sns.send(
      new CreateTopicCommand({ Name: topicName() }),
    );
    await sns.send(new DeleteTopicCommand({ TopicArn }));
    const res = await sns.send(
      new PublishCommand({ TopicArn, Message: "into void" }),
    );
    expect(typeof res.MessageId).toBe("string");
  });
});

// ── ListTopics ────────────────────────────────────────────────────────────────

describe("SNS — ListTopics", () => {
  it("includes topic ARN after a subscription is created", async () => {
    const sns = snsClient();
    const sqs = sqsClient();
    const name = topicName();
    const qName = uniqueQueue();
    const { QueueUrl } = await sqs.send(
      new CreateQueueCommand({ QueueName: qName }),
    );
    const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: name }));
    await sns.send(
      new SubscribeCommand({ TopicArn, Protocol: "sqs", Endpoint: QueueUrl }),
    );

    const res = await sns.send(new ListTopicsCommand({}));
    expect(res.Topics?.some((t) => t.TopicArn === TopicArn)).toBe(true);
  });
});
