import { describe, expectTypeOf, it } from "vitest";
import {
  createQueueClient,
  type Message,
  type QueueClient,
  type QueueError,
  type QueueSchemaClient,
} from "../src/index.js";

const schema = {
  orders: { parse: (v: unknown) => v as { orderId: string; amount: number } },
  emails: { parse: (v: unknown) => v as { to: string; subject: string } },
};

const q = createQueueClient({ schema, url: "http://localhost" });
const plain = createQueueClient({ url: "http://localhost" });

// Helper type: unwrap a QueueResult<T> to T | undefined (the data side)
type Data<T> = Awaited<T> extends { data: infer D } ? D : never;

describe("createQueueClient overloads", () => {
  it("returns QueueSchemaClient when schema is provided", () => {
    expectTypeOf(q).toMatchTypeOf<QueueSchemaClient<typeof schema>>();
  });

  it("returns QueueClient when no schema is provided", () => {
    expectTypeOf(plain).toMatchTypeOf<QueueClient>();
  });
});

describe("messages.send — body type", () => {
  it("constrains body to schema type for known queue", () => {
    expectTypeOf(q.messages.send<"orders">).parameter(1).toMatchTypeOf<{
      orderId: string;
      amount: number;
    }>();
  });

  it("constrains body to different schema type per queue", () => {
    expectTypeOf(q.messages.send<"emails">).parameter(1).toMatchTypeOf<{
      to: string;
      subject: string;
    }>();
  });

  it("falls back to JsonValue for unrecognized queue", () => {
    expectTypeOf(q.messages.send<"unknown-queue">).parameter(1).toMatchTypeOf<
      string | number | boolean | null | object
    >();
  });

  it("plain client accepts JsonValue for any queue", () => {
    expectTypeOf(plain.messages.send).parameter(1).toMatchTypeOf<
      string | number | boolean | null | object
    >();
  });
});

describe("messages.sendBatch — entry body type", () => {
  it("constrains batch entry message to schema type", () => {
    expectTypeOf(q.messages.sendBatch<"orders">).parameter(1).toMatchTypeOf<
      { message: { orderId: string; amount: number } }[]
    >();
  });

  it("falls back to JsonValue for unrecognized queue", () => {
    expectTypeOf(
      q.messages.sendBatch<"unknown-queue">,
    ).parameter(1).toMatchTypeOf<
      { message: string | number | boolean | null | object }[]
    >();
  });
});

describe("messages.receive — return type", () => {
  it("returns Message<schema type> for known queue (orders)", () => {
    type Result = Data<ReturnType<typeof q.messages.receive<"orders">>>;
    expectTypeOf<Result>().toMatchTypeOf<
      Message<{ orderId: string; amount: number }>[] | undefined
    >();
  });

  it("returns Message<schema type> for known queue (emails)", () => {
    type Result = Data<ReturnType<typeof q.messages.receive<"emails">>>;
    expectTypeOf<Result>().toMatchTypeOf<
      Message<{ to: string; subject: string }>[] | undefined
    >();
  });

  it("returns Message<JsonValue> for unrecognized queue", () => {
    type Result = Data<ReturnType<typeof q.messages.receive<"unknown">>>;
    expectTypeOf<Result>().toMatchTypeOf<
      Message<string | number | boolean | null | object>[] | undefined
    >();
  });

  it("plain client returns Message<JsonValue>", () => {
    type Result = Data<ReturnType<typeof plain.messages.receive>>;
    expectTypeOf<Result>().toMatchTypeOf<
      Message<string | number | boolean | null | object>[] | undefined
    >();
  });
});

describe("error path shape", () => {
  it("error side is always QueueError", () => {
    type Err = Awaited<
      ReturnType<typeof q.messages.receive<"orders">>
    > extends { error: infer E } ? E : never;
    expectTypeOf<Err>().toMatchTypeOf<QueueError | undefined>();
  });
});
