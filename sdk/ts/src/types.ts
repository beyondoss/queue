export type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

export interface Queue {
  name: string;
  is_partitioned: boolean;
  is_unlogged: boolean;
  created_at: string;
}

export interface QueueStats {
  queue_length: number;
  newest_msg_age_sec?: number;
  oldest_msg_age_sec?: number;
  total_messages: number;
  scrape_time: string;
}

export interface Message {
  id: number;
  read_count: number;
  enqueued_at: string;
  visible_at: string;
  message: JsonValue;
  headers?: JsonValue;
}

export interface Subscription {
  id: number;
  pattern: string;
  protocol: string; // "sqs" | "http" | "https"
  endpoint: string; // "sqs://{queue}" or webhook URL
  queue_name: string | null; // null for HTTP subscriptions
  bound_at: string;
  raw_delivery: boolean;
}

export interface PublishResult {
  queues_matched: number;
  messages: { queue_name: string; msg_id: number }[];
}

export interface CreateQueueOptions {
  fifo?: boolean;
}

export interface SendOptions {
  /** Custom message headers (any JSON value). */
  headers?: JsonValue;
  /** Seconds before the message becomes visible. Default: 0. */
  delay?: number;
  /** FIFO group ID. Routes the send through the FIFO path. */
  group_id?: string;
  /** Skip WAL fsync for higher throughput at the cost of durability. Default: false. */
  async_commit?: boolean;
}

export interface BatchEntry {
  message: JsonValue;
  headers?: JsonValue;
  delay?: number;
  group_id?: string;
}

export interface ReceiveOptions {
  /** Max messages to return. Default: 1. */
  max?: number;
  /** Long-poll wait time in seconds. Default: 0. */
  wait?: number;
  /** Visibility timeout in seconds. Default: 30. */
  visibilityTimeout?: number;
  /** FIFO ordering mode. Default: false. */
  fifo?: boolean;
}

export interface PublishOptions {
  /** Custom message headers (any JSON value). */
  headers?: JsonValue;
  /** Seconds before the message becomes visible. Default: 0. */
  delay?: number;
}
