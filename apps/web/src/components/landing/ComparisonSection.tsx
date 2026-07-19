import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";
import { DtIcon } from "../DtIcon";

// Category comparison only — DataFlow vs the managed-ELT and open-source
// pipeline categories. We never name competitor brands in public copy.
const FEATURES = [
  { label: "Semantic schema intelligence", dataflow: true, managed: "partial", oss: "partial" },
  { label: "LLM data copilot", dataflow: true, managed: false, oss: false },
  { label: "Preflight validation gates", dataflow: true, managed: "partial", oss: false },
  { label: "Post-load checksum reconciliation", dataflow: true, managed: "partial", oss: false },
  { label: "MCP / agent-native tooling", dataflow: true, managed: false, oss: false },
  { label: "Self-hosted / air-gapped option", dataflow: true, managed: false, oss: true },
  { label: "Open connector SDK", dataflow: "partial", managed: false, oss: true },
  { label: "Automatic schema-drift blocking", dataflow: true, managed: "partial", oss: false },
];

function Cell({ value }: { value: boolean | string }) {
  if (value === true) return <span className="lp-compare-yes"><DtIcon name="check" size={14} /> Yes</span>;
  if (value === false) return <span className="lp-compare-no">—</span>;
  return <span className="lp-compare-partial">Partial</span>;
}

export function ComparisonSection() {
  const reveal = useRevealOnScroll();
  return (
    <section className={`lp-section lp-section-alt lp-reveal ${reveal.className}`} id="compare" ref={reveal.ref}>
      <div className="lp-section-head">
        <p className="lp-section-kicker">Comparison</p>
        <h2>Built where legacy ETL stops</h2>
        <p>DataFlow combines the breadth of managed ELT, the openness of self-hosted pipelines, and an AI-first control plane you can audit.</p>
      </div>

      <div className="lp-compare-wrap">
        <div className="lp-compare-table" role="table" aria-label="Category comparison">
          <div className="lp-compare-row lp-compare-header" role="row">
            <span role="columnheader">Capability</span>
            <span role="columnheader" className="lp-compare-brand lp-compare-brand--dataflow">DataFlow</span>
            <span role="columnheader" className="lp-compare-brand">Managed ELT</span>
            <span role="columnheader" className="lp-compare-brand">Open-source pipelines</span>
          </div>
          {FEATURES.map((f) => (
            <div className="lp-compare-row" role="row" key={f.label}>
              <span role="cell" className="lp-compare-feature">{f.label}</span>
              <span role="cell" className="lp-compare-cell lp-compare-cell--dataflow"><Cell value={f.dataflow} /></span>
              <span role="cell" className="lp-compare-cell"><Cell value={f.managed} /></span>
              <span role="cell" className="lp-compare-cell"><Cell value={f.oss} /></span>
            </div>
          ))}
        </div>
        <p className="lp-compare-disclaimer">
          Comparison is by product category, not a specific vendor. &quot;Partial&quot; means the capability typically exists but requires manual work or add-ons.
        </p>
      </div>
    </section>
  );
}
