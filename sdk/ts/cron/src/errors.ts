export class CronError extends Error {
  readonly code: string;
  readonly status: number;
  readonly hint: string | undefined;
  readonly response: Response;

  constructor(
    code: string,
    message: string,
    status: number,
    response: Response,
    hint?: string,
  ) {
    super(message);
    this.name = "CronError";
    this.code = code;
    this.status = status;
    this.hint = hint;
    this.response = response;
  }
}
