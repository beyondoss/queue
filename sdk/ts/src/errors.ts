/**
 * Thrown when the queue service returns a non-2xx response.
 *
 * @example
 * ```ts
 * try {
 *   await q.getQueue("missing-queue")
 * } catch (err) {
 *   if (err instanceof QueueError) {
 *     console.error(err.code, err.message)
 *   }
 * }
 * ```
 */
export class QueueError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(code: string, message: string, status: number) {
    super(message);
    this.name = "QueueError";
    this.code = code;
    this.status = status;
  }
}

/**
 * Thrown by `getQueue` when the queue does not exist.
 *
 * @example
 * ```ts
 * try {
 *   const stats = await q.getQueue("my-queue")
 * } catch (err) {
 *   if (err instanceof QueueNotFoundError) {
 *     return new Response("Not Found", { status: 404 })
 *   }
 * }
 * ```
 */
export class QueueNotFoundError extends QueueError {
  readonly queue: string;

  constructor(queue: string) {
    super("not_found", `queue not found: ${queue}`, 404);
    this.name = "QueueNotFoundError";
    this.queue = queue;
  }
}
