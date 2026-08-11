import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";
import { MarketingSectionFooter } from "../marketing/MarketingSectionFooter";
import type { PublicRoute } from "../../lib/publicNavigation";
import { BACKEND_SUITE, EVIDENCE_AS_OF, PROVEN_EVIDENCE } from "../../lib/provenEvidence";

/** Landing proof section — measured matrix rows, never quotes we cannot source. */
export function ProofEvidenceSection({ onNavigate }: { onNavigate?: (route: PublicRoute) => void }) {
  const reveal = useRevealOnScroll();
  const featured = PROVEN_EVIDENCE.slice(0, 6);
  return (
    <section className={`lp-section lp-section-alt lp-reveal ${reveal.className}`} id="evidence" ref={reveal.ref}>
      <div className="lp-section-head">
        <p className="lp-section-kicker">Evidence</p>
        <h2>What we have actually proven</h2>
        <p>
          Each card is a live matrix run through the product path against a real engine, with the
          destination re-read afterwards. Backend suite as of {EVIDENCE_AS_OF}:{" "}
          {BACKEND_SUITE.passed.toLocaleString()} passed, {BACKEND_SUITE.failed} failed.
        </p>
      </div>
      <div className="lp-testimonial-stack">
        {featured.map((row) => (
          <article key={row.artifact} className="lp-testimonial-row">
            <span className="lp-cust-industry">{row.cases} live cases</span>
            <p>{row.claim}</p>
            <footer>
              <strong>{row.result}</strong>
              <span>{row.engines}</span>
            </footer>
          </article>
        ))}
      </div>
      {onNavigate ? (
        <MarketingSectionFooter>
          <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("customers")}>
            See all evidence
          </button>
          <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("contact")}>
            Become a design partner
          </button>
        </MarketingSectionFooter>
      ) : null}
    </section>
  );
}
