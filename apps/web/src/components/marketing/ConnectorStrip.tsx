import { ConnectorIcon } from "../../app/vendor-marks";
import { ALL_CONNECTORS } from "../../lib/provenEvidence";

/**
 * The systems Datawrap moves data between, as one slow marquee: the page's
 * single endless motion. It pauses on hover and focus, and stands still
 * under reduced motion. These are supported systems, never customers.
 */
export function ConnectorStrip() {
  const items = ALL_CONNECTORS;
  const row = (hidden: boolean) => (
    <ul className="mk-strip-row" aria-hidden={hidden || undefined}>
      {items.map((c) => (
        <li key={`${hidden ? "b" : "a"}-${c.driver}`} className="mk-strip-item">
          <ConnectorIcon id={c.driver} size={20} />
          <span>{c.name}</span>
        </li>
      ))}
    </ul>
  );
  return (
    <section className="mk-strip" aria-label={`Systems Datawrap moves data between, ${items.length} in all`}>
      <div className="mk-wrap mk-strip-inner">
        <p className="mk-strip-label">
          Moves data between {items.length} systems
        </p>
        <div className="mk-strip-viewport" tabIndex={0}>
          <div className="mk-strip-track">
            {row(false)}
            {row(true)}
          </div>
        </div>
      </div>
    </section>
  );
}
