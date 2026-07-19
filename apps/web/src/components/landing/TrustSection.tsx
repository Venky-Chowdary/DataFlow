import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";

const LAYERS = [
  {
    phase: "01",
    title: "Isolate",
    body: "Dedicated tenants, workspace scoping, and per-tenant security posture — no shared control-plane bleed.",
  },
  {
    phase: "02",
    title: "Encrypt",
    body: "Customer-managed keys wrap connector secrets. Purpose keys stay scoped to the job that needs them.",
  },
  {
    phase: "03",
    title: "Reside",
    body: "Pin jobs and artifacts to the regions your policy requires. Audit trails stay where you choose.",
  },
  {
    phase: "04",
    title: "Prove",
    body: "Post-load reconciliation verifies counts and content hashes. Quarantine never silently drops rows.",
  },
];

export function TrustSection() {
  const reveal = useRevealOnScroll();
  return (
    <section className={`lp-section lp-section-alt lp-reveal ${reveal.className}`} id="trust" ref={reveal.ref}>
      <div className="lp-section-head">
        <p className="lp-section-kicker">Enterprise trust</p>
        <h2>Security that moves with the data</h2>
        <p>Four continuous layers on every transfer — not a wall of compliance cards.</p>
      </div>

      <ol className="lp-trust-timeline">
        {LAYERS.map((layer) => (
          <li key={layer.phase} className="lp-trust-step">
            <span className="lp-trust-phase">{layer.phase}</span>
            <div>
              <h3>{layer.title}</h3>
              <p>{layer.body}</p>
            </div>
          </li>
        ))}
      </ol>

      <div className="lp-trust-proof" aria-hidden>
        <div className="lp-trust-proof-row is-ok"><span>Preflight</span><em>8 / 8</em></div>
        <div className="lp-trust-proof-row is-ok"><span>Write</span><em>quarantine 0</em></div>
        <div className="lp-trust-proof-row is-ok"><span>Reconcile</span><em>checksum match</em></div>
        <div className="lp-trust-proof-row"><span>Audit</span><em>logged</em></div>
      </div>
    </section>
  );
}
