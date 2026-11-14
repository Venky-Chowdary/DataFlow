/**
 * Whether the browser can reach Datawrap right now (UX-17).
 *
 * With the API down, Connectors kept saying "Healthy · Test passed", no banner
 * appeared for minutes, and Jobs printed the browser's raw "Failed to fetch".
 * This module is the one place that knows: the browser's own online/offline
 * signal, network-level request failures (not timeouts, which mean "busy"),
 * and when data last arrived, so screens can label what they show as stale.
 */
import { useEffect, useState } from "react";

export const API_UNREACHABLE_MESSAGE =
  "Datawrap can't be reached over the network. Check your connection; this refreshes when the connection is back.";

/** A request that never got an answer (connection refused, DNS, offline). */
export class ApiUnreachableError extends TypeError {
  constructor() {
    super(API_UNREACHABLE_MESSAGE);
    this.name = "ApiUnreachableError";
  }
}

export interface ConnectivityState {
  /** False while the browser is offline or the API refuses connections. */
  online: boolean;
  /** Why it is offline: the device, or the server. */
  reason: "" | "browser-offline" | "api-unreachable";
  /** Epoch ms of the last API answer; 0 = none this session. */
  lastSyncedAt: number;
}

type Listener = (state: ConnectivityState) => void;

const listeners = new Set<Listener>();
let state: ConnectivityState = {
  online: typeof navigator === "undefined" || navigator.onLine !== false,
  reason: typeof navigator !== "undefined" && navigator.onLine === false ? "browser-offline" : "",
  lastSyncedAt: 0,
};
let networkFailures = 0;
/** Two refused requests in a row: one can be a blip; two is an outage. */
const NETWORK_FAILURES_TO_OFFLINE = 2;

function emit(next: ConnectivityState) {
  if (next.online === state.online && next.reason === state.reason && next.lastSyncedAt === state.lastSyncedAt) return;
  state = next;
  for (const listener of [...listeners]) listener(state);
}

export function connectivity(): ConnectivityState {
  return state;
}

export function subscribeConnectivity(listener: Listener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/** Any API answer (even a 4xx) proves the server is reachable. */
export function noteApiReachable(now: number = Date.now()): void {
  networkFailures = 0;
  emit({ online: state.reason === "browser-offline" ? false : true, reason: state.reason === "browser-offline" ? "browser-offline" : "", lastSyncedAt: now });
}

/** A request failed before any answer. Timeouts are not reported here. */
export function noteApiUnreachable(): void {
  networkFailures += 1;
  if (networkFailures >= NETWORK_FAILURES_TO_OFFLINE && state.reason !== "browser-offline") {
    emit({ ...state, online: false, reason: "api-unreachable" });
  }
}

export function noteBrowserOnline(online: boolean): void {
  if (!online) emit({ ...state, online: false, reason: "browser-offline" });
  else if (state.reason === "browser-offline") emit({ ...state, online: networkFailures < NETWORK_FAILURES_TO_OFFLINE, reason: networkFailures < NETWORK_FAILURES_TO_OFFLINE ? "" : "api-unreachable" });
}

if (typeof window !== "undefined") {
  window.addEventListener("online", () => noteBrowserOnline(true));
  window.addEventListener("offline", () => noteBrowserOnline(false));
}

export function useConnectivity(): ConnectivityState {
  const [value, setValue] = useState(state);
  useEffect(() => {
    setValue(state);
    return subscribeConnectivity(setValue);
  }, []);
  return value;
}

/** "Showing data from 14:05" — or nothing when nothing has loaded yet. */
export function staleDataLabel(lastSyncedAt: number, now: Date = new Date()): string {
  if (!lastSyncedAt) return "Nothing has loaded yet in this session.";
  const at = new Date(lastSyncedAt);
  const sameDay = at.toDateString() === now.toDateString();
  const when = sameDay
    ? at.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" })
    : at.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
  return `Everything on screen is from ${when} and may be out of date.`;
}

/** Test seam. */
export function resetConnectivityForTests(online = true): void {
  networkFailures = 0;
  state = { online, reason: online ? "" : "browser-offline", lastSyncedAt: 0 };
}
