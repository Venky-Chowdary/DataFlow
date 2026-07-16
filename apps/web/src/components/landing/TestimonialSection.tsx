import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";

const QUOTES = [
  {
    quote: "We replaced a tangle of brittle scripts with DataFlow in a weekend. The preflight gates caught a schema drift that would have cost us hours of rework.",
    name: "Alex R.",
    title: "Staff Data Engineer, Fortune 500 retailer",
  },
  {
    quote: "The semantic mapping is genuinely better than string matching. AMT and payment_amount line up automatically, even when column names change.",
    name: "Priya K.",
    title: "Data Architect, health systems",
  },
  {
    quote: "MCP support let our AI agent trigger governed transfers from Cursor. That is the future of data ops.",
    name: "Jordan M.",
    title: "Head of Platform, SaaS scale-up",
  },
];

export function TestimonialSection() {
  const reveal = useRevealOnScroll();
  return (
    <section className={`lp-section lp-reveal ${reveal.className}`} id="testimonials" ref={reveal.ref}>
      <div className="lp-section-head">
        <p className="lp-section-kicker">Proof</p>
        <h2>What data teams say</h2>
        <p>Engineers choose DataFlow when accuracy matters more than speed alone.</p>
      </div>
      <div className="lp-testimonial-grid">
        {QUOTES.map((q, i) => (
          <blockquote key={i} className="lp-testimonial-card" style={{ "--reveal-i": i } as React.CSSProperties}>
            <p className="lp-testimonial-quote">&ldquo;{q.quote}&rdquo;</p>
            <footer>
              <strong>{q.name}</strong>
              <span>{q.title}</span>
            </footer>
          </blockquote>
        ))}
      </div>
    </section>
  );
}
