import { useMemo, useRef } from "react";
import { highlightSql } from "../../lib/queryHighlight";
import { diagnoseSql, type SqlDiagnosis } from "../../lib/sqlEditorModel";

interface SqlEditorProps {
  id: string;
  label: string;
  value: string;
  onChange: (next: string) => void;
  mode: "query" | "procedure";
  dialect?: string;
  bound?: Record<string, string>;
  placeholder?: string;
  hint?: string;
  rows?: number;
  required?: boolean;
}

/**
 * Compact Studio / schedule extract editor.
 *
 * Highlight HTML comes from `highlightSql` (Query Playground SSOT). This
 * wrapper keeps gutter + extract diagnosis — it does not own a second
 * tokenizer and does not pull playground run/explain chrome into Studio.
 */
export function SqlEditor({
  id,
  label,
  value,
  onChange,
  mode,
  dialect,
  bound,
  placeholder,
  hint,
  rows = 8,
  required,
}: SqlEditorProps) {
  const highlighted = useMemo(() => highlightSql(value), [value]);
  const diagnosis: SqlDiagnosis = useMemo(
    () => diagnoseSql(value, { mode, dialect, bound }),
    [value, mode, dialect, bound],
  );
  const lineCount = Math.max(1, String(value || "").split("\n").length);
  const minHeight = Math.max(rows * 20, 140);
  const highlightRef = useRef<HTMLPreElement>(null);
  const gutterRef = useRef<HTMLDivElement>(null);
  const syncScroll = (top: number, left: number) => {
    if (highlightRef.current) {
      highlightRef.current.scrollTop = top;
      highlightRef.current.scrollLeft = left;
    }
    if (gutterRef.current) gutterRef.current.scrollTop = top;
  };

  return (
    <div className="df2-sql-editor">
      <div className="df2-sql-editor-head">
        <label className="df2-label" htmlFor={id}>{label}</label>
        <span className={`df2-sql-editor-pill${diagnosis.ok ? " is-ok" : value.trim() ? " is-err" : ""}`}>
          {value.trim() ? (diagnosis.ok ? diagnosis.statement : "Needs fix") : mode === "query" ? "SELECT" : "CALL"}
        </span>
      </div>
      <div className="df2-sql-editor-frame" style={{ minHeight }}>
        <div className="df2-sql-gutter" aria-hidden ref={gutterRef}>
          {Array.from({ length: lineCount }, (_, i) => (
            <span key={i}>{i + 1}</span>
          ))}
        </div>
        <div className="df2-sql-surface">
          <pre
            className="df2-sql-highlight"
            aria-hidden
            ref={highlightRef}
            dangerouslySetInnerHTML={{
              __html: `${highlighted}${value.endsWith("\n") || !value ? "\n" : ""}`,
            }}
          />
          <textarea
            id={id}
            className="df2-sql-input"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            placeholder={placeholder}
            autoComplete="off"
            autoCorrect="off"
            autoCapitalize="off"
            spellCheck={false}
            required={required}
            wrap="off"
            rows={rows}
            onScroll={(e) => syncScroll(e.currentTarget.scrollTop, e.currentTarget.scrollLeft)}
          />
        </div>
      </div>
      <div className="df2-sql-editor-status" role="status">
        <span>{dialect ? dialect.replace(/_/g, " ") : "SQL"}</span>
        <span>{diagnosis.binds.length} bind{diagnosis.binds.length === 1 ? "" : "s"}</span>
        {diagnosis.error ? (
          <span className="df2-sql-editor-error">{diagnosis.error}</span>
        ) : (
          <span className="df2-sql-editor-ok">
            {mode === "query"
              ? "Read-only extract — result columns map next. Not CDC."
              : "One CALL — result or dest params never invent binds."}
          </span>
        )}
      </div>
      {hint ? <p className="df2-label-hint">{hint}</p> : null}
    </div>
  );
}
