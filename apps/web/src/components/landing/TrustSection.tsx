import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";

const LAYERS = [
  {
    phase: "01",
    title: "Isolate",
    tag: "Tenancy",
    body: "Every store read is scoped to the caller's workspace — jobs, connectors, schedules, audit, and agent tools alike. Custom domains and per-tenant policy on top.",
    proof: "Workspace isolated",
  },
  {
    phase: "02",
    title: "Encrypt",
    tag: "Keys",
    body: "Connector secrets are encrypted at rest and can be wrapped with your own key — local, wrapped, or AWS KMS. PII hashing uses a per-workspace key.",
    proof: "BYOK wrapped",
  },
  {
    phase: "03",
    title: "Authenticate",
    tag: "Identity",
    body: "SSO with OIDC or SAML and email-domain routing, SCIM 2.0 group-to-role provisioning, and an idle session timeout you control.",
    proof: "SSO · SCIM",
  },
  {
    phase: "04",
    title: "Prove",
    tag: "Verify",
    body: "Every load is read back and verified value by value, and every action lands in a hash-chained audit log signed with its own key.",
    proof: "Gate-8 MATCH",
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
            Four continuous layers on every transfer — the workspace stays isolated, secrets stay
            encrypted, identity stays with the run, and proof stays exportable.
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
              <em>SSO · OIDC / SAML</em>
            </div>
            <div className="lp-home-trust-panel-row is-ok">
              <span>Secrets</span>
              <em>BYOK · AWS KMS</em>
            </div>
            <div className="lp-home-trust-panel-row is-ok">
              <span>Preflight</span>
              <em>G1–G9</em>
            </div>
            <div className="lp-home-trust-panel-row is-ok">
              <span>Quarantine</span>
              <em>surfaced</em>
            </div>
            <div className="lp-home-trust-panel-row is-ok">
              <span>Reconcile</span>
              <em>value &amp; key census</em>
            </div>
            <div className="lp-home-trust-panel-row">
              <span>Audit</span>
              <em>hash-chained</em>
            </div>
          </aside>
        </div>
      </div>
    </section>
  );
}
