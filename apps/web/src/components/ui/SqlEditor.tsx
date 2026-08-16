import { useMemo, useRef, useState } from "react";
import { highlightSql } from "../../lib/queryHighlight";
import { diagnoseSql, type SqlDiagnosis, type SqlEditorMode } from "../../lib/sqlEditorModel";
import { Dialog } from "./Dialog";
import { DtIcon } from "../DtIcon";

interface SqlEditorProps {
  id: string;
  label: string;
  value: string;
  onChange: (next: string) => void;
  mode: SqlEditorMode;
  dialect?: string;
  bound?: Record<string, string>;
  placeholder?: string;
  hint?: string;
  rows?: number;
  required?: boolean;
  /** Compact pane can open a full-size editor. Default on. */
  expandable?: boolean;
  hideExpand?: boolean;
  fill?: boolean;
}

/**
 * Compact Studio / schedule extract editor.
 *
 * Highlight HTML comes from `highlightSql` (Query Playground SSOT). This
 * wrapper keeps gutter + extract diagnosis — it does not own a second
 * tokenizer and does not pull playground run/explain chrome into Studio.
 */
function SqlEditorBody({
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
  expandable,
  hideExpand,
  fill,
  onExpand,
}: SqlEditorProps & { onExpand?: () => void }) {
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
  const showExpand = expandable !== false && !hideExpand && onExpand;

  return (
    <div className={`df2-sql-editor${fill ? " is-fill" : ""}`}>
      <div className="df2-sql-editor-head">
        <label className="df2-label" htmlFor={id}>{label}</label>
        <div className="df2-sql-editor-head-actions">
          <span className={`df2-sql-editor-pill${diagnosis.ok ? " is-ok" : value.trim() ? " is-err" : ""}`}>
            {value.trim() ? (diagnosis.ok ? diagnosis.statement : "Needs fix") : mode === "query" ? "SELECT" : mode === "dest_dml" ? "INSERT" : "CALL"}
          </span>
          {showExpand && (
            <button
              type="button"
              className="df2-btn df2-btn-ghost df2-btn-sm df2-sql-editor-expand"
              onClick={onExpand}
              aria-label={`Expand ${label}`}
              title="Open a larger editor"
            >
              <DtIcon name="expand" size={13} /> Expand
            </button>
          )}
        </div>
      </div>
      <div
        className={`df2-sql-editor-frame${fill ? " is-fill" : ""}`}
        style={fill ? undefined : { height: minHeight, minHeight }}
      >
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
              : mode === "dest_dml"
                ? "One dest INSERT/MERGE — failed rows quarantine. Not CDC."
                : "One CALL — result or dest params never invent binds."}
          </span>
        )}
      </div>
      {hint ? <p className="df2-label-hint">{hint}</p> : null}
    </div>
  );
}

export function SqlEditor(props: SqlEditorProps) {
  const [expanded, setExpanded] = useState(false);
  const expandable = props.expandable !== false && !props.hideExpand;

  return (
    <>
      <SqlEditorBody
        {...props}
        expandable={expandable}
        onExpand={() => setExpanded(true)}
      />
      <Dialog
        open={expanded}
        onClose={() => setExpanded(false)}
        size="full"
        title={props.label}
        subtitle={
          props.mode === "query"
            ? "Read-only SELECT / WITH — one statement. Result columns map on the next step."
            : props.mode === "dest_dml"
              ? "One INSERT / MERGE / UPDATE with :binds. Failed rows quarantine."
              : "One CALL / EXEC. Missing binds quarantine that row — never invent values."
        }
        ariaLabel={`Expanded ${props.label}`}
        className="df2-sql-editor-dialog"
        footer={
          <button
            type="button"
            className="df2-btn df2-btn-primary"
            onClick={() => setExpanded(false)}
          >
            Done
          </button>
        }
      >
        <SqlEditorBody
          {...props}
          id={`${props.id}-expanded`}
          rows={Math.max(props.rows ?? 8, 20)}
          fill
          hideExpand
          expandable={false}
        />
      </Dialog>
    </>
  );
}
