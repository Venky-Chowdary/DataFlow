/**
 * The job stream is a convenience. The job document is the progress.
 *
 * A proxy can hold the SSE response open and stop forwarding frames. The
 * browser then keeps the last banner (QA 2026-11-06: "10,000 of 1,000,000
 * (1%, 31s)" for the whole source re-read) while the document moves. Silence
 * past this window means read the document. A dropped socket is not a failed
 * transfer.
 */
export const STREAM_CATCHUP_SILENCE_MS = 8_000;

export function streamSilenceNeedsCatchUp(
  lastEventMs: number,
  nowMs: number,
  silenceMs: number = STREAM_CATCHUP_SILENCE_MS,
): boolean {
  if (!Number.isFinite(lastEventMs) || !Number.isFinite(nowMs)) return true;
  return nowMs - lastEventMs >= silenceMs;
}

/** Shown when the EventSource errors. The server status stays as it was. */
export function streamDropNotice(): string {
  return "Live stream interrupted — reconnecting";
}
