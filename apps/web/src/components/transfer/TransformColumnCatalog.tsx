import {
  columnFamily,
  columnFindings,
  qualityScore,
  type ColumnKitchenFamily,
} from "../../lib/transformProfile";
import type { ShapeColumnProfile } from "../../lib/shape";

interface TransformColumnCatalogProps {
  columns: ShapeColumnProfile[];
  selected: string;
  onSelect: (name: string) => void;
}

const FAMILY_LABEL: Record<ColumnKitchenFamily, string> = {
  numeric: "Numeric",
  text: "Text",
  datetime: "Datetime",
  boolean: "Boolean",
  empty: "Empty",
};

/**
 * DataKitchen catalog: one row per column, the findings count, a quality score.
 * Selecting a row is the only way the detail pane changes — no stacked cards.
 */
export function TransformColumnCatalog({
  columns,
  selected,
  onSelect,
}: TransformColumnCatalogProps) {
  return (
    <nav className="df2-xform-catalog" aria-label="Column catalog">
      <header className="df2-xform-catalog-head">
        <h3>Columns</h3>
        <span>{columns.length.toLocaleString()}</span>
      </header>
      <ul className="df2-xform-catalog-list" role="listbox" aria-label="Profiled columns">
        {columns.map((column) => {
          const findings = columnFindings(column);
          const family = columnFamily(column);
          const score = qualityScore(column);
          const active = column.name === selected;
          return (
            <li key={column.name}>
              <button
                type="button"
                role="option"
                aria-selected={active}
                className={`df2-xform-catalog-row${active ? " is-selected" : ""}${findings.length ? " has-findings" : ""}`}
                onClick={() => onSelect(column.name)}
              >
                <span className="df2-xform-catalog-name" title={column.name}>{column.name}</span>
                <span className={`df2-xform-catalog-family is-${family}`}>{FAMILY_LABEL[family]}</span>
                <span className="df2-xform-catalog-meta">
                  <span className="df2-xform-catalog-score" title="Share of sampled rows without a finding">
                    {score}
                  </span>
                  {findings.length > 0 && (
                    <span className="df2-xform-catalog-issues">
                      {findings.length} issue{findings.length === 1 ? "" : "s"}
                    </span>
                  )}
                </span>
              </button>
            </li>
          );
        })}
      </ul>
    </nav>
  );
}
