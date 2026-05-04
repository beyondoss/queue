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
  pattern: string;
  queue_name: string;
  bound_at: string;
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
  vt?: number;
  /** FIFO ordering mode. Default: false. */
  fifo?: boolean;
}

export interface PublishOptions {
  /** Custom message headers (any JSON value). */
  headers?: JsonValue;
  /** Seconds before the message becomes visible. Default: 0. */
  delay?: number;
}
