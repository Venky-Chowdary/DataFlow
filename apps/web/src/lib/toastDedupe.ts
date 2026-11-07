export type ToastTone = "info" | "success" | "warning" | "error";

export interface ToastFingerprint {
  title: string;
  message?: string;
  tone: ToastTone;
}

/** Exact identity of a toast — used to collapse 2–3 identical notifications. */
export function toastFingerprint(opts: ToastFingerprint): string {
  return `${opts.tone}\n${opts.title.trim()}\n${(opts.message ?? "").trim()}`;
}

export const TOAST_DEDUPE_MS = 2500;
export const TOAST_STACK_MAX = 2;

export function mergeToastStack<T extends { id: string; key: string; createdAt: number }>(
  prev: T[],
  next: T,
  now: number,
  windowMs = TOAST_DEDUPE_MS,
  max = TOAST_STACK_MAX,
): { items: T[]; shownId: string; replaced: boolean } {
  const existing = prev.find((t) => t.key === next.key);
  if (existing && now - existing.createdAt < windowMs) {
    const kept = { ...next, id: existing.id, createdAt: now };
    return {
      items: prev.map((t) => (t.key === next.key ? kept : t)),
      shownId: existing.id,
      replaced: true,
    };
  }
  const withoutDup = prev.filter((t) => t.key !== next.key);
  return { items: [...withoutDup, next].slice(-max), shownId: next.id, replaced: false };
}
