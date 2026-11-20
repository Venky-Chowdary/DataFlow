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
  return (
    <div className="mk-page mk-doc">
      <DocHeader
        kicker="Status"
        title="Service status"
        meta={
          data?.checked_at && !unreachable
            ? `Last checked ${new Date(data.checked_at).toLocaleString()}. Refreshes every 30 seconds.`
            : "Live readiness of the hosted Datawrap service."
        }
        lead="Read from the same readiness checks that decide whether the API takes traffic."
      />
      <section className="mk-section mk-section--tight">
        <div className="mk-wrap mk-doc-body">
          <div className={`mk-status-banner is-${state}`} role="status" aria-live="polite">
            <StateGlyph state={state} />
            <strong>{headline}</strong>
            <button type="button" className="mk-btn mk-btn--secondary mk-btn--sm" onClick={() => void load()}>
              Check again
            </button>
          </div>
          {unreachable ? (
            <p className="mk-status-note">
              Your browser could not reach the status service, so the current state is unknown. This can be your network
              as well as ours. If the app is not working for you, email <a href="mailto:contact@datawrap.io">contact@datawrap.io</a>.
            </p>
          ) : null}
          {!unreachable && data?.components?.length ? (
            <ul className="mk-status-list">
              {data.components.map((c) => (
                <li key={c.id} className={`is-${c.status}`}>
                  <div>
                    <strong>{c.name}</strong>
                    <span>{c.description}</span>
                  </div>
                  <span className="mk-status-pill">
                    <StateGlyph state={c.status} />
                    {STATE_LABEL[c.status]}
                  </span>
                </li>
              ))}
            </ul>
          ) : null}
          <p className="mk-caption">Shows component state only. No hostnames or error details are published.</p>
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

/** The subprocessor list is shared on request; the page is noindex and linked from the footer only. */
export function SubprocessorsPage() {
  return (
    <div className="mk-page mk-doc">
      <DocHeader
        kicker="Legal"
        title="Subprocessors"
        lead="Third parties that process personal data on Datawrap's behalf to run the hosted service."
      />
      <section className="mk-section mk-section--tight">
        <div className="mk-wrap mk-doc-body mk-prose">
          <p>
            Our current subprocessor list is available on request — email{" "}
            <a href="mailto:contact@datawrap.io?subject=Subprocessor%20list">contact@datawrap.io</a>.
          </p>
        </div>
      </section>
    </div>
  );
}
