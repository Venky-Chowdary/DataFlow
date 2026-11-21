/**
 * Every list screen owns one load state, so a failed read can never render as
 * "nothing here yet" and an in-flight read can never print zeros.
 *
 *  - ready:   rows are on screen (a later refresh that fails keeps them and
 *             reports the failure beside them);
 *  - error:   the first read failed and nothing is on screen;
 *  - loading: the first read has not answered yet;
 *  - empty:   the server answered with zero rows.
 */
export type ListLoadState = "loading" | "error" | "empty" | "ready";

export function listLoadState(opts: {
  /** A read is in flight. */
  loading: boolean;
  /** Message of the last failed read, "" when it succeeded. */
  error: string;
  /** The server has answered at least once. */
  loaded: boolean;
  /** Rows the screen can show (or the server's total for them). */
  count: number;
}): ListLoadState {
  if (opts.count > 0) return "ready";
  if (opts.error) return "error";
  if (!opts.loaded || opts.loading) return "loading";
  return "empty";
}

/** True when counts on screen would be invented (loading) or stale (error before any answer). */
export function countsUnknown(state: ListLoadState): boolean {
  return state === "loading" || state === "error";
}

