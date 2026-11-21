/**
 * "How this was checked" — the one evidence section under the run result header.
 *
 * The header (RunResultHeader) carries the verdict and read = landed + held +
 * skipped. This section says only what that line does not: which comparison
 * ran after the write, where the destination count came from, and whether the
 * checksums matched — in sentence case, no stat boxes, no second equation.
 * The full row ledger and the Gate-8 detail stay one click away, behind a
 * disclosure, for the reader who wants every number.
 */
import type { ReactNode } from "react";
import { plural } from "../../lib/plural";
import type { Gate8PopulationView } from "../../lib/gate8Population";
import type { Gate8ReconciliationPayload } from "../../lib/types";

export interface RunCheckRow {
  label: string;
  value: string;
  /** Mono for ids and hashes only. */
  code?: string;
}

const COMPARISON: Record<string, string> = {
  full_checksum: "Full checksum of every row, after the write",
  sample: "A sample of rows, compared after the write",
  writer_ack: "Writer acknowledgement only: nothing was read back",
  none: "No comparison ran",
};

function shortHash(hash: string): string {
  return hash.length > 16 ? `${hash.slice(0, 16)}…` : hash;
}

/** The rows of the section, from the run's own Gate-8 record. Exported for tests. */
export function runCheckRows(input: {
  reconciliation?: Gate8ReconciliationPayload | null;
  population: Gate8PopulationView;
  /** Rows the writer reported, shown only when the destination was not counted. */
  writerRows?: number | null;
}): RunCheckRow[] {
  const rec = input.reconciliation ?? null;
  const pop = input.population;
  const rows: RunCheckRow[] = [];

  const coverage = String(pop.coverage || rec?.coverage || rec?.checksum_scope || "").toLowerCase();
  rows.push({
    label: "Comparison",
    value: !rec
      ? COMPARISON.none
      : COMPARISON[coverage] ?? (rec.passed === false ? "Comparison after the write did not pass" : "Row counts and checksums, after the write"),
  });

  rows.push({
    label: "Destination count",
    value: pop.destCount != null
      ? `${plural(pop.destCount, "row")}, counted in the destination`
      : input.writerRows != null
        ? `Not counted. The writer reported ${plural(input.writerRows, "row")}.`
        : "Not counted",
  });

  const sourceSum = String(rec?.source_checksum || "").trim();
  const destSum = pop.destChecksum;
  rows.push(
    destSum && sourceSum
      ? {
        label: "Checksum",
        value: rec?.checksum_match === false || sourceSum !== destSum ? "Source and destination differ" : "Source and destination match",
        code: shortHash(destSum),
      }
      : destSum
        ? { label: "Checksum", value: "Destination checksum recorded", code: shortHash(destSum) }
        : { label: "Checksum", value: "Not computed" },
  );

  if (pop.validateRunId) rows.push({ label: "Validate run", value: "", code: pop.validateRunId });
  return rows;
}

export function RunCheckSummary({
  rows,
  details,
}: {
  rows: RunCheckRow[];
  /** The full ledger and Gate-8 cards, behind "Full ledger and Gate-8 detail". */
  details?: ReactNode;
}) {
  return (
    <section className="df2-run-checks" aria-labelledby="df2-run-checks-title">
      <h3 id="df2-run-checks-title" className="df2-run-checks-title">How this was checked</h3>
      <dl className="df2-run-checks-list">
        {rows.map((row) => (
          <div key={row.label} className="df2-run-checks-row">
            <dt>{row.label}</dt>
            <dd>
              {row.value}
              {row.code ? <code className={row.value ? "has-text" : undefined}>{row.code}</code> : null}
            </dd>
          </div>
        ))}
      </dl>
      {details ? (
        <details className="df2-run-checks-details">
          <summary>Full ledger and Gate-8 detail</summary>
          <div className="df2-run-checks-details-body">{details}</div>
        </details>
      ) : null}
    </section>
  );
}
