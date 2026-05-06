/**
 * Returned in the `error` field when the queue service returns a non-2xx response.
 *
 * @example
 * ```ts
 * const { error } = await queue.receiveMessages("my-queue")
 * if (error instanceof QueueError) {
 *   console.error(error.code, error.message)
 * }
 * ```
 */
export class QueueError extends Error {
  readonly code: string;
  readonly status: number;
  readonly hint: string | undefined;

  constructor(code: string, message: string, status: number, hint?: string) {
    super(message);
    this.name = "QueueError";
    this.code = code;
    this.status = status;
    this.hint = hint;
  }
}

/**
 * Returned in the `error` field when the target queue does not exist.
 *
 * @example
 * ```ts
 * const { error } = await queue.receiveMessages("my-queue")
 * if (error instanceof QueueNotFoundError) {
 *   console.error(`queue not found: ${error.queueName}`)
 * }
 * ```
 */
export class QueueNotFoundError extends QueueError {
  readonly queueName: string;

  constructor(queueName: string, status: number, hint?: string) {
    super(
      "queue_not_found",
      `Queue '${queueName}' does not exist`,
      status,
      hint,
    );
    this.name = "QueueNotFoundError";
    this.queueName = queueName;
  }
}
