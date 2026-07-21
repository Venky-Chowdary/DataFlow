import { useEffect, useState, type CSSProperties } from "react";

type PreviewKind = "studio" | "theater" | "mapping" | "connectors" | "pilot";

/**
 * Animated product UI previews for Help — live app surfaces, not icon cards.
 */
export function HelpAppPreview({ kind, bare = false }: { kind: PreviewKind; bare?: boolean }) {
  const [tick, setTick] = useState(0);

  useEffect(() => {
    if (window.matchMedia("(prefers-reduced-motion: reduce)").matches) return;
    const id = window.setInterval(() => setTick((t) => t + 1), 900);
    return () => window.clearInterval(id);
  }, []);

  const gates = Math.min(8, 2 + (tick % 7));
  const phase = tick % 4;

  const body = (() => {
    if (kind === "connectors") {
      return (
        <div className="lp-help-preview-body lp-help-preview-body--grid">
          {["PostgreSQL", "Snowflake", "BigQuery", "MongoDB", "S3", "MySQL"].map((name, i) => (
            <div key={name} className={`lp-help-conn ${i === tick % 6 ? "is-live" : ""}`} style={{ "--i": i } as CSSProperties}>
              <strong>{name}</strong>
              <span>transfer-ready</span>
            </div>
          ))}
        </div>
      );
    }

    if (kind === "mapping") {
      const rows = [
        ["order_amt", "payment_amount", "0.93"],
        ["cust_id", "customer_key", "0.91"],
        ["email_addr", "email", "0.88"],
        ["created_at", "order_ts", "0.86"],
      ];
      return (
        <div className="lp-help-preview-body">
          {rows.map((r, i) => (
            <div key={r[0]} className={`lp-help-map-row ${i === tick % 4 ? "is-pulse" : ""}`}>
              <code>{r[0]}</code>
              <i>→</i>
              <strong>{r[1]}</strong>
              <em>{r[2]}</em>
            </div>
          ))}
        </div>
      );
    }

    if (kind === "theater") {
      return (
        <div className="lp-help-preview-body">
          <div className="lp-help-progress">
            <label>Preflight</label>
            <div className="lp-help-bar"><i style={{ width: `${(gates / 8) * 100}%` }} /></div>
            <em>{gates} / 8</em>
          </div>
          <ul className="lp-help-proof">
            <li className={gates >= 2 ? "is-ok" : ""}><span>Schema contract</span><em>pass</em></li>
            <li className={gates >= 4 ? "is-ok" : ""}><span>Type coercion</span><em>pass</em></li>
            <li className={gates >= 6 ? "is-ok" : ""}><span>Destination probe</span><em>pass</em></li>
            <li className={gates >= 8 ? "is-ok" : ""}><span>Checksum</span><em>match</em></li>
          </ul>
          <div className="lp-help-ticker">
            <span className="lp-help-ticker-dot" />
            Writing batch {1200 + tick * 180} · quarantine 0
          </div>
        </div>
      );
    }

    if (kind === "pilot") {
      return (
        <div className="lp-help-preview-body lp-help-preview-body--chat">
          <div className="lp-help-bubble is-user">Why did gate 3 fail on payment_amount?</div>
          <div className={`lp-help-bubble is-bot ${phase > 0 ? "is-in" : ""}`}>
            Type coercion blocked <code>NUMERIC(10,2)</code> → <code>INTEGER</code>. Quarantine kept 14 rows — open Job Theater to review.
          </div>
          <div className={`lp-help-chat-actions ${phase > 1 ? "is-in" : ""}`}>
            <span>Open Job Theater</span>
            <span>Adjust map</span>
          </div>
        </div>
      );
    }

    return (
      <div className="lp-help-preview-body lp-help-preview-body--studio">
        <aside>
          <b>Studio</b>
          {["Connect", "Map", "Preflight", "Prove"].map((label, i) => (
            <p key={label} className={phase >= i ? "is-on" : ""}>{label}</p>
          ))}
        </aside>
        <main>
          <header>
            <strong>LIVE</strong>
            <span>
              {phase === 0 && "Connecting PostgreSQL → Snowflake"}
              {phase === 1 && "Mapping order_amt → payment_amount"}
              {phase === 2 && `Preflight ${gates} / 8 gates`}
              {phase === 3 && "Reconcile · checksum match"}
            </span>
          </header>
          <div className="lp-help-progress">
            <label>Progress</label>
            <div className="lp-help-bar"><i style={{ width: `${((phase + 1) / 4) * 100}%` }} /></div>
            <em>{phase + 1} / 4</em>
          </div>
          <div className="lp-help-studio-lanes">
            <div className={phase >= 1 ? "is-on" : ""}>Semantic map</div>
            <div className={phase >= 2 ? "is-on" : ""}>8 gates</div>
            <div className={phase >= 3 ? "is-on" : ""}>Proof</div>
          </div>
        </main>
      </div>
    );
  })();

  if (bare) {
    return (
      <div className="lp-help-preview lp-help-preview--bare" aria-hidden>
        {body}
      </div>
    );
  }

  const labels: Record<PreviewKind, string> = {
    studio: "Transfer Studio · Orders migration",
    theater: "Job Theater · live",
    mapping: "Transfer Studio · Mapping",
    connectors: "Connectors · catalog",
    pilot: "Data Pilot · triage",
  };

  return (
    <div className="lp-help-preview" aria-hidden>
      <div className="lp-help-preview-chrome">
        <span /><span /><span />
        <em>{labels[kind]}</em>
      </div>
      {body}
    </div>
  );
}
