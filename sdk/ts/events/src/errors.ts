/**
 * Returned in the `error` field when the event service returns a non-2xx response.
 *
 * @example
 * ```ts
 * const { error } = await events.subscriptions.list("payments.*")
 * if (error instanceof EventError) {
 *   console.error(error.code, error.message)
 * }
 * ```
 */
export class EventError extends Error {
  readonly code: string;
  readonly status: number;
  readonly hint: string | undefined;

  constructor(code: string, message: string, status: number, hint?: string) {
    super(message);
    this.name = "EventError";
    this.code = code;
    this.status = status;
    this.hint = hint;
  }
}

/**
 * Returned in the `error` field when the target subscription does not exist.
 *
 * @example
 * ```ts
 * const { error } = await events.subscriptions.delete(id)
 * if (error instanceof EventNotFoundError) {
 *   console.error("subscription not found")
 * }
 * ```
 */
export class EventNotFoundError extends EventError {
  constructor(hint?: string) {
    super("not_found", "Event subscription not found", 404, hint);
    this.name = "EventNotFoundError";
  }
}
