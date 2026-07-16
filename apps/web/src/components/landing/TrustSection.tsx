import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";
import { DtIcon } from "../DtIcon";

const BADGES = [
  { icon: "shield", title: "Fail-closed by default", body: "Preflight gates block bad loads before rows move, not after." },
  { icon: "lock", title: "Masked secrets", body: "Connector credentials are encrypted and never echoed to the UI client." },
  { icon: "users", title: "Workspace RBAC", body: "Owner, editor, and viewer roles keep connectors and jobs scoped to teams." },
  { icon: "globe", title: "Self-hosted ready", body: "Run on-prem or in your VPC; no data needs to leave your network." },
  { icon: "server", title: "Audit-ready jobs", body: "Every run, quarantine row, and schema decision is logged for compliance." },
  { icon: "check", title: "Checksum proof", body: "Post-load reconciliation verifies row counts and content hashes." },
];

export function TrustSection() {
  const reveal = useRevealOnScroll();
  return (
    <section className={`lp-section lp-section-alt lp-reveal ${reveal.className}`} id="trust" ref={reveal.ref}>
      <div className="lp-section-head">
        <p className="lp-section-kicker">Enterprise trust</p>
        <h2>Security and governance built in</h2>
        <p>DataFlow is designed for regulated environments from day one.</p>
      </div>
      <div className="lp-trust-grid">
        {BADGES.map((b, i) => (
          <article key={b.title} className="lp-trust-card" style={{ "--reveal-i": i } as React.CSSProperties}>
            <span className="lp-trust-card-icon" aria-hidden>
              <DtIcon name={b.icon} size={22} />
            </span>
            <h3>{b.title}</h3>
            <p>{b.body}</p>
          </article>
        ))}
      </div>
    </section>
  );
}
