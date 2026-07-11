import { DtIcon } from "../DtIcon";
import { typeBadgeClass } from "../../lib/typeDisplay";

interface StructurePreviewProps {
  columns: string[];
  schema?: Record<string, string>;
  rows?: Record<string, unknown>[];
  rowCount?: number;
  title?: string;
  subtitle?: string;
}

export function StructurePreview({
  columns,
  schema = {},
  rows = [],
  rowCount,
  title = "Structure preview",
  subtitle,
}: StructurePreviewProps) {
  const previewCols = columns.slice(0, 8);
  const previewRows = rows.slice(0, 5);

  if (!columns.length) {
    return (
      <div className="df2-structure-preview is-empty">
        <DtIcon name="database" size={20} />
        <p>Connect a source to preview columns and sample values.</p>
      </div>
    );
  }

  return (
    <div className="df2-structure-preview">
      <div className="df2-structure-preview-head">
        <div>
          <h4>{title}</h4>
          <p>
            {subtitle
              ?? `${columns.length} fields${rowCount != null ? ` · ${rowCount.toLocaleString()} rows` : ""} · sample below`}
          </p>
        </div>
        <span className="df2-badge df2-badge-live">
          <DtIcon name="check" size={12} /> Detected
        </span>
      </div>

      <div className="df2-structure-field-strip" aria-label="Detected fields">
        {columns.slice(0, 12).map((col) => (
          <span key={col} className={`df2-structure-field-chip ${typeBadgeClass(schema[col])}`}>
            <strong>{col}</strong>
            <small className="df2-type-badge">{schema[col] || "string"}</small>
          </span>
        ))}
        {columns.length > 12 && (
          <span className="df2-structure-field-chip muted">+{columns.length - 12} more</span>
        )}
      </div>

      {previewRows.length > 0 ? (
        <div className="df2-structure-table-wrap">
          <table className="df2-structure-table">
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
    </div>
  );
}
