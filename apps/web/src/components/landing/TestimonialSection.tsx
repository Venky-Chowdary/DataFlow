import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";
import { MarketingSectionFooter } from "../marketing/MarketingSectionFooter";
import type { PublicRoute } from "../../lib/publicNavigation";

const QUOTES = [
  {
    quote: "We replaced a tangle of brittle scripts with Datawrap in a weekend. The preflight gates caught a schema drift that would have cost us hours of rework.",
    name: "Alex R.",
    title: "Staff Data Engineer, Fortune 500 retailer",
    industry: "Retail",
  },
  {
    quote: "The semantic mapping is genuinely better than string matching. AMT and payment_amount line up automatically, even when column names change.",
    name: "Priya K.",
    title: "Data Architect, health systems",
    industry: "Healthcare",
  },
  {
    quote: "MCP support let our AI agent trigger governed transfers from Cursor. That is the future of data ops.",
    name: "Jordan M.",
    title: "Head of Platform, SaaS scale-up",
    industry: "SaaS",
  },
];

interface TestimonialSectionProps {
  onNavigate?: (route: PublicRoute) => void;
}

export function TestimonialSection({ onNavigate }: TestimonialSectionProps) {
  const reveal = useRevealOnScroll();
  return (
    <section className={`lp-section lp-section-alt lp-reveal ${reveal.className}`} id="testimonials" ref={reveal.ref}>
      <div className="lp-section-head">
        <p className="lp-section-kicker">Proof</p>
        <h2>What data teams say</h2>
        <p>Engineers choose Datawrap when accuracy matters more than speed alone.</p>
      </div>
      <div className="lp-testimonial-stack">
        {QUOTES.map((q) => (
          <blockquote key={q.name} className="lp-testimonial-row">
            <span className="lp-cust-industry">{q.industry}</span>
            <p>&ldquo;{q.quote}&rdquo;</p>
            <footer>
              <strong>{q.name}</strong>
              <span>{q.title}</span>
            </footer>
          </blockquote>
        ))}
      </div>
      {onNavigate ? (
        <MarketingSectionFooter>
          <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("customers")}>
            Read customer stories
          </button>
          <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("contact")}>
            Become a design partner
          </button>
        </MarketingSectionFooter>
      ) : null}
    </section>
  );
}
