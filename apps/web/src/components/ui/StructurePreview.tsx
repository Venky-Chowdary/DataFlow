import { useState } from "react";
import { DtIcon } from "../DtIcon";
import { typeBadgeClass } from "../../lib/typeDisplay";
import { Dialog } from "./Dialog";

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
  /** Offer a Table / JSON toggle — ideal for document sources (e.g. MongoDB) */
  allowJson?: boolean;
  /** Shown when columns exist but sample rows failed or were not returned */
  sampleWarning?: string | null;
  onRetrySample?: () => void;
  /** Open a wider dialog for full table when the inline preview is cramped */
  expandable?: boolean;
}

function PreviewBody({
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
  allowJson = false,
  sampleWarning = null,
  onRetrySample,
  expandable,
  onExpand,
  hideExpand,
}: StructurePreviewProps & { onExpand?: () => void; hideExpand?: boolean }) {
  const rowsPerPage = maxRows ?? (fill ? 40 : 10);
  const [page, setPage] = useState(0);
  const [showAllFields, setShowAllFields] = useState(false);
  const [view, setView] = useState<"table" | "json">("table");
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
        <div className="df2-structure-preview-head-actions">
          {allowJson && rows.length > 0 && (
            <div className="df2-structure-view-toggle" role="tablist" aria-label="Preview format">
              <button
                type="button"
                role="tab"
                aria-selected={view === "table"}
                className={view === "table" ? "active" : ""}
                onClick={() => setView("table")}
              >
                <DtIcon name="layers" size={13} /> Table
              </button>
              <button
                type="button"
                role="tab"
                aria-selected={view === "json"}
                className={view === "json" ? "active" : ""}
                onClick={() => setView("json")}
              >
                <DtIcon name="code" size={13} /> JSON
              </button>
            </div>
          )}
          {expandable && !hideExpand && onExpand && (
            <button
              type="button"
              className="df2-btn df2-btn-sm df2-btn-ghost"
              onClick={onExpand}
              title="Open full preview"
            >
              <DtIcon name="expand" size={13} /> Expand
            </button>
          )}
          {showBadge && (
            <span className="df2-badge df2-badge-live">
              <DtIcon name="check" size={12} /> Detected
            </span>
          )}
        </div>
      </div>

      {showFieldStrip && (
        <div className="df2-structure-field-block">
          <div className="df2-structure-field-strip" aria-label="Detected fields">
            {stripCols.map((col) => (
              <span
                key={col}
                className={`df2-structure-field-chip ${typeBadgeClass(schema[col])}`}
                title={`${col} · ${schema[col] || "string"}`}
              >
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

      {previewRows.length > 0 && allowJson && view === "json" ? (
        <div className="df2-structure-json" aria-label="Sample documents">
          {previewRows.map((row, i) => (
            <pre key={i} className="df2-structure-json-doc">
              <code>{JSON.stringify(row, null, 2)}</code>
            </pre>
          ))}
        </div>
      ) : previewRows.length > 0 ? (
        <div className="df2-structure-table-wrap">
          <table className="df2-structure-table" style={{ "--cols": previewCols.length } as React.CSSProperties}>
            <thead>
              <tr>
                {previewCols.map((col) => (
                  <th key={col} className={typeBadgeClass(schema[col])}>
                    <span>{col}</span>
                    <small className="df2-type-badge">{schema[col] || "string"}</small>
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
                      <td key={col} title={text} className={typeBadgeClass(schema[col])}>
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
        <div className="df2-structure-empty-rows" role="status">
          <p>
            {sampleWarning
              || "Columns detected, but no sample rows were loaded. Preview data and Validate dry-run need a row sample — re-read the source table."}
          </p>
          {onRetrySample && (
            <button type="button" className="df2-btn df2-btn-sm" onClick={onRetrySample}>
              <DtIcon name="scan" size={14} /> Reload sample preview
            </button>
          )}
        </div>
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

export function StructurePreview(props: StructurePreviewProps) {
  const [expanded, setExpanded] = useState(false);
  const expandable = props.expandable !== false && (props.columns?.length ?? 0) > 0;

  return (
    <>
      <PreviewBody
        {...props}
        expandable={expandable}
        onExpand={() => setExpanded(true)}
      />
      <Dialog
        open={expanded}
        onClose={() => setExpanded(false)}
        size="xl"
        title={props.title || "Structure preview"}
        subtitle={
          props.subtitle
            ?? `${props.columns.length} fields${props.rowCount != null ? ` · ${props.rowCount.toLocaleString()} rows` : ""}`
        }
        ariaLabel="Expanded structure preview"
        className="df2-structure-preview-dialog"
      >
        <PreviewBody
          {...props}
          fill
          maxRows={props.maxRows ?? 40}
          maxCols={props.columns.length}
          showFieldStrip={props.showFieldStrip !== false}
          expandable={false}
          hideExpand
          className={`${props.className || ""} df2-structure-preview--dialog`.trim()}
        />
      </Dialog>
    </>
  );
}
