import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";

const LAYERS = [
  {
    phase: "01",
    title: "Isolate",
    tag: "Tenancy",
    body: "Dedicated tenants, workspace scoping, and per-tenant security posture — no shared control-plane bleed between customers.",
    proof: "Tenant isolated",
  },
  {
    phase: "02",
    title: "Encrypt",
    tag: "Keys",
    body: "Customer-managed keys wrap connector secrets. Purpose keys stay scoped to the job that needs them — never a global vault shortcut.",
    proof: "BYOK wrapped",
  },
  {
    phase: "03",
    title: "Reside",
    tag: "Regions",
    body: "Pin jobs and artifacts to the regions your policy requires. Audit trails stay where compliance and residency demand.",
    proof: "Region pinned",
  },
  {
    phase: "04",
    title: "Prove",
    tag: "Reconcile",
    body: "Post-load reconciliation verifies counts and content hashes. Quarantine never silently drops rows from the load.",
    proof: "Checksum MATCH",
  },
];

export function TrustSection() {
  const reveal = useRevealOnScroll();
  return (
    <section
      className={`lp-home-trust ${reveal.className}`}
      id="trust"
      ref={reveal.ref}
      aria-label="Enterprise trust"
    >
      <div className="lp-home-trust-inner">
        <header className="lp-home-section-head">
          <p className="lp-section-kicker">Enterprise trust</p>
          <h2>Security that moves with the data</h2>
          <p>
            Four continuous layers on every transfer — identity stays with the run, secrets stay
            scoped, residency stays pinned, and proof stays exportable.
          </p>
        </header>

        <div className="lp-home-trust-layout">
          <ol className="lp-home-trust-grid">
            {LAYERS.map((layer) => (
              <li key={layer.phase} className="lp-home-trust-card">
                <div className="lp-home-trust-card-top">
                  <span className="lp-home-trust-num">{layer.phase}</span>
                  <span className="lp-home-trust-tag">{layer.tag}</span>
                </div>
                <h3>{layer.title}</h3>
                <p>{layer.body}</p>
                <span className="lp-home-trust-pill">{layer.proof}</span>
              </li>
            ))}
          </ol>

          <aside className="lp-home-trust-panel" aria-label="Live security posture">
            <header>
              <strong>Live posture</strong>
              <span>Enforced</span>
            </header>
            <div className="lp-home-trust-panel-row is-ok">
              <span>Identity</span>
              <em>SSO · Okta</em>
            </div>
            <div className="lp-home-trust-panel-row is-ok">
              <span>Secrets</span>
              <em>BYOK · KMS</em>
            </div>
            <div className="lp-home-trust-panel-row is-ok">
              <span>Preflight</span>
              <em>G1–G8</em>
            </div>
            <div className="lp-home-trust-panel-row is-ok">
              <span>Quarantine</span>
              <em>surfaced</em>
            </div>
            <div className="lp-home-trust-panel-row is-ok">
              <span>Reconcile</span>
              <em>after write</em>
            </div>
            <div className="lp-home-trust-panel-row">
              <span>Audit</span>
              <em>jobs · maps · agents</em>
            </div>
          </aside>
        </div>
      </div>
    </section>
  );
}
