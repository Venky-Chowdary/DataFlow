import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";
import { ProductJourneyCinema } from "./ProductJourneyCinema";

export function ObservabilityInAction() {
  const reveal = useRevealOnScroll();

  return (
    <section
      className="lp-obs"
      id="observability-in-action"
      aria-label="See observability in action"
    >
      <div ref={reveal.ref} className={`lp-obs-inner ${reveal.className}`}>
        <header className="lp-home-section-head">
          <p className="lp-section-kicker">See observability in action</p>
          <h2>Click the product. Watch the proof.</h2>
          <p>
            Tap Map, Validate, Write, or Prove. Each stage replays what the engine behind
            Studio, Pipelines, Pilot, and MCP actually does — semantic scores, G1–G9,
            quarantine with reasons, and a destination-engine checksum.
          </p>
        </header>
        <ProductJourneyCinema />
      </div>
    </section>
  );
}
