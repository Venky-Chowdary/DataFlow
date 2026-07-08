import { DtIcon } from "./DtIcon";

interface UniversalHeroProps {
  onStartTransfer: () => void;
  onOpenPilot?: () => void;
}

const SOURCES = ["CSV", "JSON", "PostgreSQL", "Snowflake", "API", "S3"];
const DESTS = ["MongoDB", "BigQuery", "Excel", "Redis", "Kafka", "Any DB"];

export function UniversalHero({ onStartTransfer, onOpenPilot }: UniversalHeroProps) {
  return (
    <section className="dt-universal-hero">
      <div className="dt-universal-hero-content">
        <div className="dt-universal-hero-badge">
          <DtIcon name="zap" size={12} /> Universal Data Platform
        </div>
        <h1 className="dt-universal-hero-title">
          Any data · any format · anywhere to anywhere
        </h1>
        <p className="dt-universal-hero-sub">
          Upload a file, connect a database, or point at an API — AI understands your schema,
          maps columns by meaning, and validates with 8 preflight gates before a single row moves.
        </p>
        <div className="dt-universal-hero-actions">
          <button type="button" className="dt-btn dt-btn-primary dt-btn-lg dt-universal-cta" onClick={onStartTransfer}>
            <DtIcon name="transfer" size={18} />
            Start Transfer
          </button>
          {onOpenPilot && (
            <button type="button" className="dt-btn dt-btn-lg dt-universal-cta-secondary" onClick={onOpenPilot}>
              <DtIcon name="sparkle" size={18} />
              Ask Data Pilot
            </button>
          )}
        </div>
      </div>

      <div className="dt-universal-flow" aria-hidden>
        <div className="dt-universal-flow-col">
          <span className="dt-universal-flow-label">Sources</span>
          {SOURCES.map((s) => (
            <span key={s} className="dt-universal-flow-chip source">{s}</span>
          ))}
        </div>
        <div className="dt-universal-flow-bridge">
          <div className="dt-universal-flow-line" />
          <div className="dt-universal-flow-hub">
            <DtIcon name="sparkle" size={22} />
            <span>AI</span>
          </div>
          <div className="dt-universal-flow-line" />
        </div>
        <div className="dt-universal-flow-col">
          <span className="dt-universal-flow-label">Destinations</span>
          {DESTS.map((d) => (
            <span key={d} className="dt-universal-flow-chip dest">{d}</span>
          ))}
        </div>
      </div>
    </section>
  );
}
