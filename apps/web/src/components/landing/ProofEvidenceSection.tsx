import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";
import { MarketingSectionFooter } from "../marketing/MarketingSectionFooter";
import type { PublicRoute } from "../../lib/publicNavigation";
import { MARKETING_PROOF_HIGHLIGHTS } from "../../lib/provenEvidence";

/** Landing proof section — product outcomes, never invented testimonials. */
export function ProofEvidenceSection({ onNavigate }: { onNavigate?: (route: PublicRoute) => void }) {
  const reveal = useRevealOnScroll();
  return (
    <section className={`lp-section lp-section-alt lp-reveal ${reveal.className}`} id="evidence" ref={reveal.ref}>
      <div className="lp-section-head">
        <p className="lp-section-kicker">Fidelity</p>
        <h2>Proof you can show finance</h2>
        <p>
          Every load maps, validates, quarantines bad rows, and ends with a verification report
          that reads the destination back wherever its engine allows.
        </p>
      </div>
      <div className="lp-testimonial-stack">
        {MARKETING_PROOF_HIGHLIGHTS.map((row) => (
          <article key={row.title} className="lp-testimonial-row">
            <span className="lp-cust-industry">{row.stat}</span>
            <p><strong>{row.title}.</strong> {row.body}</p>
            <footer>
              <strong>Verification report</strong>
              <span>On every run</span>
            </footer>
          </article>
        ))}
      </div>
      {onNavigate ? (
        <MarketingSectionFooter>
          <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("customers")}>
            See how we prove a load
          </button>
          <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("contact")}>
            Book a pilot
          </button>
        </MarketingSectionFooter>
      ) : null}
    </section>
  );
}
