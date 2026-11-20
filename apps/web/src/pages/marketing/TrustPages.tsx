import { useCallback, useEffect, useState } from "react";
import { DocHeader } from "./LegalPages";
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

  const overall: ComponentState = data?.status ?? "operational";
  const headline = loading
    ? "Checking systems…"
    : unreachable
      ? "Status unknown"
      : OVERALL_LABEL[overall];

  const state: DisplayState = loading ? "checking" : unreachable ? "unknown" : overall;
  const checked =
    data?.checked_at && !unreachable ? new Date(data.checked_at).toLocaleString() : "";
  return (
    <div className="mk-page mk-doc mk-trust">
      <section className="mk-trust-hero" aria-labelledby="mk-trust-title">
        <div className="mk-wrap mk-trust-hero-grid">
          <div className="mk-trust-hero-copy">
            <h1 id="mk-trust-title">Service status</h1>
            <p className="mk-trust-lead">
              Read from the same readiness checks that decide whether the API takes traffic.
            </p>
          </div>
          <div className={`mk-trust-readout is-${state}`} role="status" aria-live="polite">
            <StateGlyph state={state} />
            <strong className="mk-trust-state">{headline}</strong>
            <p className="mk-trust-checked">
              {checked ? `Last checked ${checked}. Refreshes every 30 seconds.` : "Live readiness of the hosted Datawrap service."}
            </p>
            <button type="button" className="mk-btn mk-btn--secondary mk-btn--sm" onClick={() => void load()}>
              Check again
            </button>
          </div>
        </div>
      </section>
      <section className="mk-trust-body" aria-label="Components">
        <div className="mk-wrap">
          {unreachable ? (
            <p className="mk-trust-note">
              Your browser could not reach the status service, so the current state is unknown. This can be your network
              as well as ours. If the app is not working for you, email <a href="mailto:contact@datawrap.io">contact@datawrap.io</a>.
            </p>
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

/** Who processes data, by category, as the privacy policy describes it. */
const PROCESSING: { purpose: string; who: string; named: string }[] = [
  { purpose: "Hosting the control plane", who: "Infrastructure providers", named: "On request" },
  { purpose: "Sending transactional email", who: "An email delivery provider", named: "On request" },
  { purpose: "Single sign-on, on Enterprise", who: "Your identity provider", named: "Chosen by you" },
  { purpose: "Wrapping connector secrets, on Enterprise", who: "Your KMS", named: "Chosen by you" },
];

/** The subprocessor list is shared on request; the page is noindex and linked from the footer only. */
export function SubprocessorsPage() {
  return (
    <div className="mk-page mk-doc mk-legal">
      <DocHeader
        title="Subprocessors"
        lead="Third parties that process personal data on Datawrap's behalf to run the hosted service."
        plate={[
          ["Document", "Subprocessor list"],
          ["Availability", "On request"],
          ["Request from", <a href="mailto:contact@datawrap.io?subject=Subprocessor%20list">contact@datawrap.io</a>],
        ]}
      />
      <section className="mk-legal-request" aria-labelledby="mk-sub-ledger">
        <div className="mk-wrap">
          <div className="mk-legal-request-sheet">
            <p>
              Our current subprocessor list is available on request — email{" "}
              <a href="mailto:contact@datawrap.io?subject=Subprocessor%20list">contact@datawrap.io</a>.
            </p>
            <a className="mk-btn mk-btn--primary" href="mailto:contact@datawrap.io?subject=Subprocessor%20list">
              Request the list
            </a>
          </div>
          <h2 className="mk-trust-ledger-title" id="mk-sub-ledger">
            What is shared, and with whom
          </h2>
          <table className="mk-trust-ledger mk-trust-ledger--sub">
            <thead>
              <tr>
                <th scope="col">Purpose</th>
                <th scope="col">Who</th>
                <th scope="col">Named</th>
              </tr>
            </thead>
            <tbody>
              {PROCESSING.map((row) => (
                <tr key={row.purpose}>
                  <th scope="row">{row.purpose}</th>
                  <td>{row.who}</td>
                  <td>{row.named}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <p className="mk-trust-caption">
            Categories as the privacy policy describes them. We do not share connected datasets with other customers.
          </p>
        </div>
      </section>
    </div>
  );
}
