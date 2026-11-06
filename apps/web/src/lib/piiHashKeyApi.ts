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

/** Shortest key worth sending: base64 of 16 random bytes. The API decides the rest. */
export const PII_HASH_KEY_MIN_LENGTH = 24;

/** The API's rule, word for word (services/pii_hash_keys.KEY_RULE). */
export const PII_HASH_KEY_RULE =
  "Paste random key material: 32 or more hex characters, or base64 of 16 or more "
  + "random bytes (for example the output of openssl rand -hex 32). Passwords, "
  + "phrases and hashes of them are refused; press Rotate to have Datawrap "
  + "generate a key.";

/** What changes when a new key version becomes active (Rotate and Save alike). */
export const PII_HASH_KEY_CHANGE_WARNING =
  "Every hash_pii digest written from now on changes. Digests already written keep the old "
  + "key's value, so rows upserted or joined on a hashed column will not match earlier rows; "
  + "Validate warns on each route whose last run used the old version. A running job keeps "
  + "its key until it ends; a continuous CDC job adopts the new key at its next batch. The old "
  + "version stays listed as retired.";

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
  if (key.length < PII_HASH_KEY_MIN_LENGTH) return PII_HASH_KEY_RULE;
  return "";
}
