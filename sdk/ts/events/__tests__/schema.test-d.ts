import { describe, expectTypeOf, it } from "vitest";
import {
  createEventClient,
  type EventClient,
  type EventSchemaClient,
} from "../src/index.js";

const schema = {
  "user.*": { parse: (v: unknown) => v as { userId: string } },
  "order.placed": {
    parse: (v: unknown) => v as { orderId: string; amount: number },
  },
};

const ev = createEventClient({ schema, url: "http://localhost" });
const plain = createEventClient({ url: "http://localhost" });

describe("createEventClient overloads", () => {
  it("returns EventSchemaClient when schema is provided", () => {
    expectTypeOf(ev).toMatchTypeOf<EventSchemaClient<typeof schema>>();
  });

  it("returns EventClient when no schema is provided", () => {
    expectTypeOf(plain).toMatchTypeOf<EventClient>();
  });
});

describe("publish — payload type", () => {
  it("infers payload from exact key match", () => {
    expectTypeOf(ev.publish<"order.placed">).parameter(1).toMatchTypeOf<{
      orderId: string;
      amount: number;
    }>();
  });

  it("infers payload via glob match (user.created → user.*)", () => {
    expectTypeOf(ev.publish<"user.created">).parameter(1).toMatchTypeOf<{
      userId: string;
    }>();
  });

  it("infers payload via glob match (user.deleted → user.*)", () => {
    expectTypeOf(ev.publish<"user.deleted">).parameter(1).toMatchTypeOf<{
      userId: string;
    }>();
  });

  it("falls back to JsonValue for unmatched routing key", () => {
    expectTypeOf(ev.publish<"payment.failed">).parameter(1).toMatchTypeOf<
      string | number | boolean | null | object
    >();
  });

  it("plain client accepts JsonValue for any routing key", () => {
    expectTypeOf(plain.publish).parameter(1).toMatchTypeOf<
      string | number | boolean | null | object
    >();
  });
});

describe("subscriptions passthrough", () => {
  it("schema client retains subscriptions interface unchanged", () => {
    expectTypeOf(ev.subscriptions).toMatchTypeOf<
      EventClient["subscriptions"]
    >();
  });
});
