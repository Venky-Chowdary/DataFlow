import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";
import { DtIcon } from "../DtIcon";

const BADGES = [
  { icon: "shield", title: "Tenant isolation", body: "Each enterprise customer gets a dedicated tenant with custom domain, workspace scoping, and per-tenant security posture." },
  { icon: "lock", title: "BYOK encryption", body: "Customer-managed keys wrap per-purpose data keys. Your credentials are encrypted at rest with your own KMS." },
  { icon: "server", title: "Data residency", body: "Pin jobs and artifacts to a region. Audit trails stay where you choose — us-east, eu-west, ap-south, and more." },
  { icon: "globe", title: "Custom-domain SaaS", body: "Deploy as dataflow.yourcompany.com with SSO, IP allowlisting, and dedicated security contacts." },
  { icon: "server", title: "Audit-ready jobs", body: "Every run, quarantine row, and schema decision is logged for SOC 2, GDPR, and HIPAA review." },
  { icon: "check", title: "Checksum proof", body: "Post-load reconciliation verifies row counts and content hashes before a job is marked complete." },
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
