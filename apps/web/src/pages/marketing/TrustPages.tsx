import { useCallback, useEffect, useState } from "react";
import { MarketingInkHero } from "../../components/marketing/MarketingInkHero";
import { MarketingReveal } from "../../components/marketing/MarketingReveal";
import { API_BASE } from "../../lib/types";

type ComponentState = "operational" | "degraded" | "outage";

type StatusPayload = {
  status: ComponentState;
  checked_at: string;
  cache_seconds: number;
  components: { id: string; name: string; description: string; status: ComponentState }[];
};

const STATE_LABEL: Record<ComponentState, string> = {
  operational: "Operational",
  degraded: "Degraded",
  outage: "Outage",
};

const OVERALL_LABEL: Record<ComponentState, string> = {
  operational: "All systems operational",
  degraded: "Some systems are degraded",
  outage: "Service disruption",
};

const REFRESH_MS = 30_000;

export function StatusPage() {
  const [data, setData] = useState<StatusPayload | null>(null);
  const [unreachable, setUnreachable] = useState(false);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    try {
      const res = await fetch(`${API_BASE}/status`, { cache: "no-store" });
      if (!res.ok) throw new Error(String(res.status));
      setData((await res.json()) as StatusPayload);
      setUnreachable(false);
    } catch {
      setUnreachable(true);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
    const timer = window.setInterval(() => void load(), REFRESH_MS);
    return () => window.clearInterval(timer);
  }, [load]);

  const overall: ComponentState = unreachable ? "outage" : data?.status ?? "operational";
  const headline = loading
    ? "Checking systems…"
    : unreachable
      ? "Status API unreachable"
      : OVERALL_LABEL[overall];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingInkHero
        tone="doc"
        kicker="Status"
        title="Service status"
        meta={
          data?.checked_at && !unreachable
            ? `Last checked ${new Date(data.checked_at).toLocaleString()} · refreshes every 30 seconds`
            : "Live readiness of the hosted Datawrap service"
        }
        lead="Read live from the same readiness checks that gate traffic to the API — not a manually updated page."
      />
      <MarketingReveal>
        <section className="lp-mkt-body lp-status-body" aria-live="polite">
          <div className={`lp-status-banner is-${loading ? "checking" : overall}`} role="status">
            <span className="lp-status-dot" aria-hidden="true" />
            <strong>{headline}</strong>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => void load()}>
              Refresh
            </button>
          </div>
          {unreachable && (
            <p className="lp-status-note">
              The status endpoint did not answer. If this persists, the hosted API is likely down — email
              {" "}<a href="mailto:contact@datawrap.io">contact@datawrap.io</a>.
            </p>
          )}
          <ul className="lp-status-list">
            {(data?.components ?? []).map((c) => {
              const state: ComponentState = unreachable ? "outage" : c.status;
              return (
                <li key={c.id} className={`lp-status-row is-${state}`}>
                  <div>
                    <strong>{c.name}</strong>
                    <small>{c.description}</small>
                  </div>
                  <span className="lp-status-pill">
                    <span className="lp-status-dot" aria-hidden="true" />
                    {STATE_LABEL[state]}
                  </span>
                </li>
              );
            })}
          </ul>
          <p className="lp-mkt-footnote">
            Shows component state only — no hostnames or error details are published. Incident updates for
            enterprise customers go to the contacts on their order form.
          </p>
        </section>
      </MarketingReveal>
    </div>
  );
}

const SUBPROCESSORS = [
  {
    name: "Railway Corporation",
    purpose: "Hosts the Datawrap web app, API, and transfer workers (control plane)",
    data: "Account, workspace, job metadata; data in transit during transfers",
    when: "All hosted workspaces",
  },
  {
    name: "MongoDB (Railway-managed or MongoDB Atlas)",
    purpose: "Control-plane database: jobs, schedules, audit chain, encrypted connector configs",
    data: "Account, workspace, job metadata; encrypted credentials",
    when: "All hosted workspaces",
  },
  {
    name: "Resend",
    purpose: "Transactional email: verification, password reset, job notifications",
    data: "Recipient name and email address, message content",
    when: "When email delivery is enabled",
  },
  {
    name: "OpenAI and Anthropic",
    purpose: "Optional cloud models for Pilot. Pilot runs on Datawrap's local engine by default",
    data: "Prompts and the schema context sent with them",
    when: "Only if a workspace admin enables a cloud model",
  },
  {
    name: "Amazon Web Services",
    purpose: "KMS key wrapping for customer-managed keys; optional object storage for uploads",
    data: "Wrapped key material; uploaded files and proof artifacts",
    when: "Enterprise BYOK or object storage enabled",
  },
];

export function SubprocessorsPage() {
  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingInkHero
        tone="doc"
        kicker="Legal"
        title="Subprocessors"
        meta="Draft pending legal review · Last updated November 2026 · contact@datawrap.io"
        lead="Third parties that process personal data on Datawrap's behalf to run the hosted service."
      />
      <MarketingReveal>
        <section className="lp-mkt-body">
          <div className="lp-status-table-wrap">
            <table className="lp-status-table">
              <thead>
                <tr>
                  <th scope="col">Subprocessor</th>
                  <th scope="col">Purpose</th>
                  <th scope="col">Data processed</th>
                  <th scope="col">Used when</th>
                </tr>
              </thead>
              <tbody>
                {SUBPROCESSORS.map((s) => (
                  <tr key={s.name}>
                    <th scope="row">{s.name}</th>
                    <td>{s.purpose}</td>
                    <td>{s.data}</td>
                    <td>{s.when}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="lp-mkt-footnote">
            Your connected sources and destinations, and your own identity provider, are not Datawrap
            subprocessors — you choose and contract with them. Self-hosted deployments use none of the
            services above unless you configure them. We give customers 30 days' notice before adding a
            subprocessor; to subscribe or object, email contact@datawrap.io.
          </p>
        </section>
      </MarketingReveal>
    </div>
  );
}
