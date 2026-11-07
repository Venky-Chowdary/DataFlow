/**
 * A self-rescheduling background read that survives failures.
 *
 * The schedules page and drawer re-armed their timer only when a read
 * succeeded and changed state, so one failed request (a deploy, a network
 * blip) stopped auto-refresh until a manual reload (R7). Here the next read is
 * always scheduled — after a success or a failure — until `stop()`.
 */
export interface PollerOptions<T> {
  read: () => Promise<T>;
  /** Delay before the next read, given the last result (or `undefined` after an error). */
  delayMs: (last: T | undefined) => number;
  onResult: (value: T) => void;
  onError?: (error: unknown) => void;
  /** Stop when this returns true for a result (e.g. a finished run). */
  isDone?: (value: T) => boolean;
  setTimer?: (fn: () => void, ms: number) => unknown;
  clearTimer?: (handle: unknown) => void;
}

export interface Poller {
  stop: () => void;
  /** Read now (e.g. the tab regained focus), then continue on schedule. */
  poke: () => void;
}

export function createPoller<T>(opts: PollerOptions<T>, firstDelayMs?: number): Poller {
  const setTimer = opts.setTimer ?? ((fn, ms) => setTimeout(fn, ms));
  const clearTimer = opts.clearTimer ?? ((h) => clearTimeout(h as ReturnType<typeof setTimeout>));
  let stopped = false;
  let handle: unknown = null;
  let last: T | undefined;
  let inFlight = false;

  const schedule = (ms: number) => {
    if (stopped) return;
    if (handle !== null) clearTimer(handle);
    handle = setTimer(() => void tick(), ms);
  };

  const tick = async () => {
    if (stopped || inFlight) return;
    inFlight = true;
    handle = null;
    try {
      const value = await opts.read();
      if (stopped) return;
      last = value;
      opts.onResult(value);
      if (opts.isDone?.(value)) {
        stopped = true;
        return;
      }
      schedule(opts.delayMs(value));
    } catch (error) {
      if (stopped) return;
      opts.onError?.(error);
      schedule(opts.delayMs(undefined));
    } finally {
      inFlight = false;
    }
  };

  schedule(firstDelayMs ?? opts.delayMs(last));
  return {
    stop: () => {
      stopped = true;
      if (handle !== null) clearTimer(handle);
      handle = null;
    },
    poke: () => {
      if (!stopped) schedule(0);
    },
  };
}
