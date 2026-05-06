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
