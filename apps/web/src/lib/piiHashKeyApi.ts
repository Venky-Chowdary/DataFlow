/**
 * Settings → Security: the workspace's hash_pii key (QA browser 2 / security 13).
 *
 * Status, rotate and set. Key material goes up once (set) and never comes back:
 * the API answers with versions and dates only.
 */
import { apiErrorFrom, apiFetch } from "./api";
import { API_BASE } from "./types";

export interface PiiHashKeyVersion {
  version: number;
  created_at?: string | null;
  created_by?: string | null;
  retired_at?: string | null;
}

export interface PiiHashKeyStatus {
  workspace_id: string;
  configured: boolean;
  managed_by_deployment: boolean;
  set_by_admin: boolean;
  active_version: number;
  versions: PiiHashKeyVersion[];
}

/** The same minimum the API enforces (services/pii_hash_keys.MIN_KEY_LENGTH). */
export const PII_HASH_KEY_MIN_LENGTH = 32;

const URL = `${API_BASE}/workspace/pii-hash-key`;

async function readStatus(res: Response, fallback: string): Promise<PiiHashKeyStatus> {
  if (!res.ok) throw await apiErrorFrom(res, fallback);
  return (await res.json()) as PiiHashKeyStatus;
}

export async function fetchPiiHashKeyStatus(): Promise<PiiHashKeyStatus> {
  return readStatus(await apiFetch(URL), "Could not read the hashing key status");
}

export async function rotatePiiHashKey(): Promise<PiiHashKeyStatus> {
  return readStatus(await apiFetch(`${URL}/rotate`, { method: "POST" }), "Could not rotate the hashing key");
}

export async function setPiiHashKey(key: string): Promise<PiiHashKeyStatus> {
  return readStatus(
    await apiFetch(URL, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ key }),
    }),
    "Could not set the hashing key",
  );
}

/** One line for the status badge. */
export function piiHashKeyBadge(status: PiiHashKeyStatus | null): string {
  if (!status) return "Unknown";
  if (status.managed_by_deployment) return "Managed by deployment";
  if (!status.configured) return "Created on first use";
  return `Active v${status.active_version}${status.set_by_admin ? " · set by admin" : ""}`;
}

/** Why a key typed into the form would be refused, before it is sent. */
export function piiHashKeyInputProblem(key: string): string {
  if (!key) return "Enter the key.";
  if (key !== key.trim()) return "Remove leading or trailing spaces.";
  if (key.length < PII_HASH_KEY_MIN_LENGTH) return `Use at least ${PII_HASH_KEY_MIN_LENGTH} characters.`;
  return "";
}
