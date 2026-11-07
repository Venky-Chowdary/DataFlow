import type { RefObject } from "react";
import { DtIcon } from "../DtIcon";
import type { RuleCompileReport } from "../../lib/businessRules";
import type { ShapeSuggestion } from "../../lib/shape";

function severityClass(severity: string): string {
  if (severity === "blocking") return "df2-badge df2-badge-error";
  if (severity === "decision") return "df2-badge df2-badge-warn";
  return "df2-badge";
}

/** The classic Business rules card. The canvas drawer renders this same card. */
export function RulesImportCard({
  ruleReport,
  sourceTables,
  ruleError,
  fileInputRef,
  onPickRules,
  ruleBusy,
  allowed,
}: {
  ruleReport?: RuleCompileReport | null;
  sourceTables: string[];
  ruleError: string;
  fileInputRef: RefObject<HTMLInputElement | null>;
  onPickRules: (file: File | undefined) => void;
  ruleBusy: boolean;
  allowed: boolean;
}) {
  return (
    <div className="df2-rule-import">
      <div className="df2-rule-import-copy">
        <strong>Business rules</strong>
        <p>
          Upload the mapping workbook (Excel, CSV, TSV or JSON). Rules that
          can run exactly are compiled into steps here; everything else waits
          for your review.
        </p>
        <details className="df2-rule-import-detail">
          <summary>What this upload does</summary>
          <p>
            Headers are inferred from the file and the schemas you already
            selected — not a fixed column list. Closed-form rows compile onto
            this recipe and Map. Destination names land on Map — this catalog
            stays the pre-load image (source names plus derived columns).
            Anything we cannot execute stays in review — unused destination
            columns are not written. Applied rows land on this recipe and Map
            immediately. Closed-form Validate checks compile as destination
            contracts — they never write a column. Review is for sentences
            that are not a closed form or did not bind. A leftover bound
            rename can be accepted as Direct on Map, or remapped there.
            {sourceTables.length > 1
              ? ` ${sourceTables.length} source tables are selected (${sourceTables.join(", ")}). Name Source + Column (or Table.column). Joins need you to choose how rows match; they stay in review.`
              : ""}
          </p>
        </details>
        {ruleReport ? <p className="df2-rule-import-summary">{ruleReport.filename}</p> : null}
        {ruleError ? <p className="df2-rule-import-error" role="alert">{ruleError}</p> : null}
      </div>
      <div className="df2-rule-import-actions">
        <input
          ref={fileInputRef}
          type="file"
          accept=".xlsx,.xlsm,.xls,.csv,.tsv,.txt,.json,.ndjson,application/json,text/csv,text/tab-separated-values,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel"
          hidden
          onChange={(event) => void onPickRules(event.target.files?.[0])}
        />
        <button
          type="button"
          className="df2-btn df2-btn-secondary df2-btn-sm"
          disabled={ruleBusy || !allowed}
          onClick={() => fileInputRef.current?.click()}
        >
          {ruleBusy ? "Reading…" : ruleReport ? "Replace rules file" : "Upload rules"}
        </button>
      </div>
    </div>
  );
}

/** The classic Suggested repairs card. The canvas drawer renders this same card. */
export function SuggestionsCard({
  suggestions,
  profileReady,
  allowed,
  disabledReason,
  onAdd,
}: {
  suggestions: ShapeSuggestion[];
  profileReady: boolean;
  allowed: boolean;
  disabledReason: string;
  onAdd: (suggestion: ShapeSuggestion) => void;
}) {
  return (
    <section className="df2-xform-card" aria-labelledby="xform-decisions-title">
      <header className="df2-xform-card-head">
        <h3 id="xform-decisions-title">Suggested repairs</h3>
      </header>
      {suggestions.length > 0 ? (
        <ul className="df2-xform-suggestions">
          {suggestions.map((suggestion) => (
            <li key={suggestion.id}>
              <div className="df2-xform-suggestion-head">
                <span className={severityClass(suggestion.severity)}>{suggestion.severity}</span>
                <strong>{suggestion.title}</strong>
              </div>
              <p>{suggestion.reason}</p>
              <div className="df2-xform-suggestion-foot">
                <span>{suggestion.rows_affected.toLocaleString()} sampled row(s) affected</span>
                <button
                  type="button"
                  className="df2-btn df2-btn-sm"
                  disabled={!allowed}
                  title={disabledReason || "Add this step to the recipe"}
                  onClick={() => onAdd(suggestion)}
                >
                  <DtIcon name="plus" size={14} /> Add step
                </button>
              </div>
            </li>
          ))}
        </ul>
      ) : profileReady ? (
        <p className="df2-xform-empty">
          <DtIcon name="check" size={16} />
          Nothing in the sample needs a transform. Validate still re-checks the whole population.
        </p>
      ) : null}
    </section>
  );
}
