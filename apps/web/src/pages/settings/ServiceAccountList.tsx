import { useState } from "react";
import type { ServiceAccount } from "../../lib/api";

export const ROTATION_OVERLAPS = [
  { seconds: 0, label: "No overlap" },
  { seconds: 3600, label: "1 hour" },
  { seconds: 86400, label: "24 hours" },
  { seconds: 604800, label: "7 days" },
];

export function ServiceAccountList({
  accounts,
  onRotate,
  onRevoke,
  busyId,
}: {
  accounts: ServiceAccount[];
  onRotate: (account: ServiceAccount, overlapSeconds: number) => void;
  onRevoke: (account: ServiceAccount) => void;
  busyId?: string;
}) {
  const [overlaps, setOverlaps] = useState<Record<string, number>>({});
  return (
    <div className="df2-api-key-list" role="list" aria-label="Service accounts and SCIM tokens" data-testid="service-account-list">
      {accounts.map((account) => {
        const expired = account.expired === true
          || Boolean(account.expires_at && Date.parse(account.expires_at) < Date.now());
        return (
          <article key={account.id} className="df2-api-key-card df2-iam-service-account" role="listitem">
            <div className="df2-api-key-card-main">
              <div className="df2-api-key-card-copy">
                <div className="df2-api-key-card-title">
                  <strong>{account.name}</strong>
                  <span className="df2-badge df2-badge-muted" data-testid="service-account-kind">
                    {account.kind === "scim" ? "SCIM" : "service account"}
                  </span>
                </div>
                <div className="df2-api-key-card-meta-row">
                  <span className="df2-api-key-card-meta">Role: {account.role || "—"}</span>
                  <span className="df2-api-key-card-meta">
                    Scopes: {account.scopes?.length ? account.scopes.join(", ") : "None"}
                  </span>
                  <span className="df2-api-key-card-meta">Prefix: <code>{account.prefix}…</code></span>
                  <span className="df2-api-key-card-meta" data-testid="service-account-expiry">
                    {expired
                      ? "Expired"
                      : account.expires_at
                        ? `Expires ${new Date(account.expires_at).toLocaleString()}`
                        : "No expiration"}
                  </span>
                </div>
              </div>
            </div>
            <div className="df2-api-key-card-actions df2-iam-service-account-actions">
              <label className="df2-iam-overlap-field">
                <span>Overlap</span>
                <select
                  className="df2-select"
                  value={overlaps[account.id] ?? 86400}
                  disabled={busyId === account.id}
                  onChange={(event) => setOverlaps((current) => ({
                    ...current,
                    [account.id]: Number(event.target.value),
                  }))}
                  aria-label={`Rotation overlap for ${account.name}`}
                >
                  {ROTATION_OVERLAPS.map((option) => (
                    <option key={option.seconds} value={option.seconds}>{option.label}</option>
                  ))}
                </select>
              </label>
              <button
                type="button"
                className="df2-btn df2-btn-secondary df2-btn-sm"
                disabled={busyId === account.id}
                data-testid={`service-account-rotate-${account.id}`}
                onClick={() => onRotate(account, overlaps[account.id] ?? 86400)}
              >
                Rotate
              </button>
              <button
                type="button"
                className="df2-btn df2-btn-danger df2-btn-sm"
                disabled={busyId === account.id}
                data-testid={`service-account-revoke-${account.id}`}
                onClick={() => onRevoke(account)}
              >
                Revoke
              </button>
            </div>
          </article>
        );
      })}
    </div>
  );
}
