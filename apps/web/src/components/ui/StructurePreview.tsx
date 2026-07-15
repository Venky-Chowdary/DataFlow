import { useState } from "react";
import { DtIcon } from "../DtIcon";
import { typeBadgeClass } from "../../lib/typeDisplay";

interface StructurePreviewProps {
  columns: string[];
  schema?: Record<string, string>;
  rows?: Record<string, unknown>[];
  rowCount?: number;
  title?: string;
  subtitle?: string;
  maxRows?: number;
  maxCols?: number;
  showFieldStrip?: boolean;
  showBadge?: boolean;
  className?: string;
  /** When true, show more sample rows so the fill-height card stays useful on large screens */
  fill?: boolean;
}

export function StructurePreview({
  columns,
  schema = {},
  rows = [],
  rowCount,
  title = "Structure preview",
  subtitle,
  maxRows,
  maxCols,
  showFieldStrip = true,
  showBadge = false,
  className = "",
  fill = false,
}: StructurePreviewProps) {
  const rowsPerPage = maxRows ?? (fill ? 40 : 10);
  const [page, setPage] = useState(0);
  const [showAllFields, setShowAllFields] = useState(false);
  const previewCols = columns.slice(0, maxCols ?? columns.length);
  const STRIP_CAP = 24;
  const stripCols = showAllFields ? previewCols : previewCols.slice(0, STRIP_CAP);
  const hiddenFieldCount = previewCols.length - stripCols.length;
  const pageCount = Math.max(1, Math.ceil(rows.length / rowsPerPage));
  const safePage = Math.min(page, pageCount - 1);
  const previewRows = rows.slice(safePage * rowsPerPage, (safePage + 1) * rowsPerPage);

  if (!columns.length) {
    return (
      <div className={`df2-structure-preview is-empty ${className}`.trim()}>
        <DtIcon name="database" size={20} />
        <p>Connect a source to preview columns and sample values.</p>
      </div>
    );
  }

  return (
    <div className={`df2-structure-preview ${fill ? "df2-structure-preview--fill" : ""} ${className}`.trim()}>
      <div className="df2-structure-preview-head">
        <div>
          <h4>{title}</h4>
          <p>
            {subtitle
              ?? `${columns.length} fields${rowCount != null ? ` · ${rowCount.toLocaleString()} rows` : ""} · sample below`}
          </p>
        </div>
        {showBadge && (
          <span className="df2-badge df2-badge-live">
            <DtIcon name="check" size={12} /> Detected
          </span>
        )}
      </div>

      {showFieldStrip && (
        <div className="df2-structure-field-block">
          <div className="df2-structure-field-strip" aria-label="Detected fields">
            {stripCols.map((col) => (
              <span key={col} className={`df2-structure-field-chip ${typeBadgeClass(schema[col])}`} title={`${col} · ${schema[col] || "string"}`}>
                <strong>{col}</strong>
                <small className="df2-type-badge">{schema[col] || "string"}</small>
              </span>
            ))}
            {hiddenFieldCount > 0 && (
              <button
                type="button"
                className="df2-structure-field-more"
                onClick={() => setShowAllFields(true)}
              >
                +{hiddenFieldCount} more
              </button>
            )}
            {showAllFields && previewCols.length > STRIP_CAP && (
              <button
                type="button"
                className="df2-structure-field-more"
                onClick={() => setShowAllFields(false)}
              >
                Show less
              </button>
            )}
          </div>
        </div>
      )}

      {previewRows.length > 0 ? (
        <div className="df2-structure-table-wrap">
          <table className="df2-structure-table" style={{ "--cols": previewCols.length } as React.CSSProperties}>
            <thead>
              <tr>
                {previewCols.map((col) => (
                  <th key={col}>
                    <span>{col}</span>
                    <small>{schema[col] || "string"}</small>
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {previewRows.map((row, i) => (
                <tr key={i}>
                  {previewCols.map((col) => {
                    const raw = row[col];
                    const text = raw == null ? "—" : String(raw);
                    return (
                      <td key={col} title={text}>
                        {text.length > 48 ? `${text.slice(0, 48)}…` : text}
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="df2-structure-empty-rows">
          Schema detected. Sample rows appear after profiling completes.
        </p>
      )}

      {rows.length > rowsPerPage && (
        <div className="df2-structure-pagination">
          <button
            type="button"
            className="df2-btn df2-btn-sm df2-btn-ghost"
            disabled={safePage === 0}
            onClick={() => setPage(safePage - 1)}
            aria-label="Previous sample rows"
          >
            <DtIcon name="chevron-left" size={14} /> Previous
          </button>
          <span className="df2-structure-page-info">
            Page {safePage + 1} of {pageCount} · rows {safePage * rowsPerPage + 1}–{Math.min((safePage + 1) * rowsPerPage, rows.length)} of {rows.length.toLocaleString()}
          </span>
          <button
            type="button"
            className="df2-btn df2-btn-sm df2-btn-ghost"
            disabled={safePage >= pageCount - 1}
            onClick={() => setPage(safePage + 1)}
            aria-label="Next sample rows"
          >
            Next <DtIcon name="chevron-right" size={14} />
          </button>
        </div>
      )}
    </div>
  );
}
