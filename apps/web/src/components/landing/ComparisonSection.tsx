import { useRevealOnScroll } from "../../hooks/useRevealOnScroll";
import { DtIcon } from "../DtIcon";

const FEATURES = [
  { label: "Semantic schema intelligence", dataflow: true, fivetran: "partial", airbyte: "partial" },
  { label: "LLM data copilot", dataflow: true, fivetran: false, airbyte: false },
  { label: "Preflight validation gates", dataflow: true, fivetran: "partial", airbyte: false },
  { label: "Post-load checksum reconciliation", dataflow: true, fivetran: "partial", airbyte: false },
  { label: "MCP / agent-native tooling", dataflow: true, fivetran: false, airbyte: false },
  { label: "Self-hosted / air-gapped option", dataflow: true, fivetran: false, airbyte: true },
  { label: "Open-source connector SDK", dataflow: "partial", fivetran: false, airbyte: true },
  { label: "Automatic schema-drift blocking", dataflow: true, fivetran: "partial", airbyte: false },
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
        <p>DataFlow combines the breadth of Fivetran, the openness of Airbyte, and an AI-first control plane you can audit.</p>
      </div>

      <div className="lp-compare-wrap">
        <div className="lp-compare-table" role="table" aria-label="Product comparison">
          <div className="lp-compare-row lp-compare-header" role="row">
            <span role="columnheader">Capability</span>
            <span role="columnheader" className="lp-compare-brand lp-compare-brand--dataflow">DataFlow</span>
            <span role="columnheader" className="lp-compare-brand">Fivetran</span>
            <span role="columnheader" className="lp-compare-brand">Airbyte</span>
          </div>
          {FEATURES.map((f) => (
            <div className="lp-compare-row" role="row" key={f.label}>
              <span role="cell" className="lp-compare-feature">{f.label}</span>
              <span role="cell" className="lp-compare-cell lp-compare-cell--dataflow"><Cell value={f.dataflow} /></span>
              <span role="cell" className="lp-compare-cell"><Cell value={f.fivetran} /></span>
              <span role="cell" className="lp-compare-cell"><Cell value={f.airbyte} /></span>
            </div>
          ))}
        </div>
        <p className="lp-compare-disclaimer">
          Comparison is based on publicly documented features; &quot;Partial&quot; means the feature exists but requires manual work or add-ons.
        </p>
      </div>
    </section>
  );
}
