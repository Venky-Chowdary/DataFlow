import { useCallback, useEffect, useState } from "react";
import { DocHeader } from "./LegalPages";
import { API_BASE } from "../../lib/types";
import { hashForPublicRoute } from "../../lib/publicNavigation";

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

/**
 * The components the API's status check reports (apps/api status_router.py,
 * COMPONENTS). Listed before the check answers, and when it cannot be
 * reached, so the page always says what it covers.
 */
const CHECKED_COMPONENTS: { id: string; name: string; description: string }[] = [
  { id: "api", name: "API", description: "Transfer, connector, and job endpoints" },
  { id: "mongodb", name: "Control plane", description: "Jobs, schedules, audit, and workspace state" },
  { id: "storage", name: "File storage", description: "Uploads, quarantine extracts, and proof packs" },
  { id: "connectors", name: "Connector runtime", description: "Database and SaaS drivers used by transfers" },
];

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

  const overall: ComponentState = data?.status ?? "operational";
  const headline = loading
    ? "Checking systems…"
    : unreachable
      ? "Could not load the status check"
      : OVERALL_LABEL[overall];

  const state: DisplayState = loading ? "checking" : unreachable ? "unknown" : overall;
  const checked =
    data?.checked_at && !unreachable ? new Date(data.checked_at).toLocaleString() : "";
  return (
    <div className="mk-page mk-doc mk-trust">
      <DocHeader
        title="Service status"
        titleId="mk-trust-title"
        lead="Read from the same readiness checks that decide whether the API takes traffic."
      />
      <section className="mk-trust-body" aria-label="Components">
        <div className="mk-wrap">
          <div className={`mk-trust-readout is-${state}`} role="status" aria-live="polite">
            <p className="mk-trust-line">
              <StateGlyph state={state} />
              <strong className="mk-trust-state">{headline}</strong>
            </p>
            <p className="mk-trust-checked">
              {checked ? `Last checked ${checked}. Refreshes every 30 seconds.` : "Live readiness of the hosted Datawrap service."}
            </p>
            <button type="button" className="mk-btn mk-btn--secondary mk-btn--sm" onClick={() => void load()}>
              Check again
            </button>
            {unreachable ? (
              <p className="mk-trust-note">
                Your browser asks the API for its readiness check, and that request failed. That can be your network
                as well as ours. If the app is not working for you, email{" "}
                <a href="mailto:contact@datawrap.io">contact@datawrap.io</a>.
              </p>
            ) : null}
          </div>
          {loading || unreachable ? (
            <table className="mk-trust-ledger">
              <caption className="mk-sr-only">Components the status check covers</caption>
              <thead>
                <tr>
                  <th scope="col">Component</th>
                  <th scope="col">What it covers</th>
                  <th scope="col">State</th>
                </tr>
              </thead>
              <tbody>
                {CHECKED_COMPONENTS.map((c) => (
                  <tr key={c.id}>
                    <th scope="row">{c.name}</th>
                    <td>{c.description}</td>
                    <td>
                      <span className="mk-trust-pill is-muted">
                        <StateGlyph state={loading ? "checking" : "unknown"} />
                        {loading ? "Checking" : "Not loaded"}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : null}
          {!unreachable && data?.components?.length ? (
            <table className="mk-trust-ledger">
              <caption className="mk-sr-only">Component state</caption>
              <thead>
                <tr>
                  <th scope="col">Component</th>
                  <th scope="col">What it covers</th>
                  <th scope="col">State</th>
                </tr>
              </thead>
              <tbody>
                {data.components.map((c) => (
                  <tr key={c.id} className={`is-${c.status}`}>
                    <th scope="row">{c.name}</th>
                    <td>{c.description}</td>
                    <td>
                      <span className="mk-trust-pill">
                        <StateGlyph state={c.status} />
                        {STATE_LABEL[c.status]}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : null}
          <p className="mk-trust-caption">Shows component state only. No hostnames or error details are published.</p>
        </div>
      </section>
    </div>
  );
}

type DisplayState = ComponentState | "checking" | "unknown";

/** A shape per state, so status never rests on colour alone. */
function StateGlyph({ state }: { state: DisplayState }) {
  return (
    <svg className={`mk-state-glyph is-${state}`} viewBox="0 0 12 12" width="12" height="12" aria-hidden focusable="false">
      {state === "operational" ? <circle cx="6" cy="6" r="4.5" fill="currentColor" /> : null}
      {state === "degraded" ? <path d="M6 1.5 10.8 10H1.2Z" fill="currentColor" /> : null}
      {state === "outage" ? <rect x="1.8" y="1.8" width="8.4" height="8.4" rx="1" fill="currentColor" /> : null}
      {state === "unknown" || state === "checking" ? (
        <circle cx="6" cy="6" r="4.2" fill="none" stroke="currentColor" strokeWidth="1.4" strokeDasharray="2 1.6" />
      ) : null}
    </svg>
  );
}

/**
 * Who processes data, by category, as the privacy policy describes it. The
 * vendors behind the first two are named in the list we email, not on this
 * page (the public bundle names no hosting provider); the last two are
 * systems the customer chooses.
 */
const PROCESSING: { purpose: string; who: string; named: string; plan: string }[] = [
  { purpose: "Hosting the control plane", who: "Infrastructure providers", named: "In the list we email you", plan: "Every plan" },
  { purpose: "Sending transactional email", who: "An email delivery provider", named: "In the list we email you", plan: "Every plan" },
  { purpose: "Single sign-on", who: "Your identity provider", named: "You choose it", plan: "Enterprise" },
  { purpose: "Wrapping connector secrets", who: "Your KMS", named: "You choose it", plan: "Enterprise" },
];

/** The subprocessor list is shared on request; the page is noindex and linked from the footer only. */
export function SubprocessorsPage() {
  return (
    <div className="mk-page mk-doc mk-legal">
      <DocHeader
        title="Subprocessors"
        lead="Third parties that process personal data on Datawrap's behalf to run the hosted service."
      />
      <section className="mk-legal-request" aria-labelledby="mk-sub-ledger">
        <div className="mk-wrap mk-trust-split">
          <div>
            <h2 className="mk-trust-ledger-title" id="mk-sub-ledger">
              What is shared, and with whom
            </h2>
            <p className="mk-trust-intro">
              Four categories, as the <a href={hashForPublicRoute("privacy")}>privacy policy</a> describes them. We do
              not share connected datasets with other customers.
            </p>
            <table className="mk-trust-ledger mk-trust-ledger--sub">
              <thead>
                <tr>
                  <th scope="col">Purpose</th>
                  <th scope="col">Who</th>
                  <th scope="col">Plan</th>
                  <th scope="col">Where it is named</th>
                </tr>
              </thead>
              <tbody>
                {PROCESSING.map((row) => (
                  <tr key={row.purpose}>
                    <th scope="row">{row.purpose}</th>
                    <td>{row.who}</td>
                    <td>{row.plan}</td>
                    <td>{row.named}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <aside className="mk-trust-readout mk-trust-request" aria-labelledby="mk-sub-get">
            <h2 id="mk-sub-get">Get the named list</h2>
            <p>
              Our current subprocessor list is available on request. Email{" "}
              <a href="mailto:contact@datawrap.io?subject=Subprocessor%20list">contact@datawrap.io</a> with the subject
              “Subprocessor list” and we send it, with each provider named.
            </p>
            <a className="mk-btn mk-btn--primary" href="mailto:contact@datawrap.io?subject=Subprocessor%20list">
              Request the list
            </a>
            <p className="mk-trust-caption">
              Reviewing processor terms? <a href={hashForPublicRoute("dpa")}>See the data processing addendum</a>.
            </p>
          </aside>
        </div>
      </section>
    </div>
  );
}
