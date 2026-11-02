import {
  forwardRef,
  useCallback,
  useEffect,
  useImperativeHandle,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import {
  dialectForLanguage,
  highlightCode,
} from "../../lib/queryHighlight";
import {
  analyzeContext,
  buildCompletions,
  checkReadOnly,
  dialectForConnector,
  extractBindParams,
  formatSql,
  resolveRunTarget,
  splitStatements,
  statementAtCursor,
  type Completion,
  type SchemaObject,
} from "../../lib/sqlIntel";
import { DtIcon } from "../DtIcon";
import { CompletionList } from "./CompletionList";

/**
 * Unified query editor — SQL dialects plus Mongo filter/pipeline modes in one
 * surface, with schema-aware completion, statement-scoped execution and an
 * advisory read-only gate.
 *
 * Deliberately zero external editor dependencies (no CodeMirror/Monaco): an
 * overlay highlighter over a real textarea keeps native IME, undo, spellcheck
 * and accessibility behaviour, and keeps the console out of the supply-chain
 * blast radius of an editor bundle. All language logic lives in the pure,
 * unit-tested `sqlIntel` / `queryHighlight` modules.
 */

export type QueryLanguage =
  | "sql"
  | "postgresql"
  | "mysql"
  | "sqlite"
  | "snowflake"
  | "bigquery"
  | "redshift"
  | "mariadb"
  | "tsql"
  | "plsql"
  | "json"
  | "javascript";

const LANGUAGE_OPTIONS: { value: QueryLanguage; label: string; hint: string }[] = [
  { value: "sql", label: "SQL (generic)", hint: "SELECT, WITH, EXPLAIN, SHOW" },
  { value: "postgresql", label: "PostgreSQL", hint: "SELECT, WITH, EXPLAIN, SHOW" },
  { value: "mysql", label: "MySQL / MariaDB", hint: "SELECT, WITH, EXPLAIN, SHOW" },
  { value: "sqlite", label: "SQLite", hint: "SELECT, WITH, EXPLAIN, PRAGMA" },
  { value: "snowflake", label: "Snowflake", hint: "SELECT, WITH, SHOW" },
  { value: "bigquery", label: "BigQuery", hint: "SELECT, WITH, EXPLAIN" },
  { value: "redshift", label: "Redshift", hint: "SELECT, WITH, EXPLAIN" },
  { value: "mariadb", label: "MariaDB", hint: "SELECT, WITH, EXPLAIN, SHOW" },
  { value: "tsql", label: "SQL Server (T-SQL)", hint: "SELECT, WITH, EXPLAIN" },
  { value: "plsql", label: "Oracle (PL/SQL)", hint: "SELECT, WITH, EXPLAIN" },
  { value: "json", label: "MongoDB / JSON", hint: "Filter object or aggregate pipeline" },
  { value: "javascript", label: "MongoDB shell (JS)", hint: "db.collection.find(...)" },
];

const CONNECTOR_LANGUAGE: Record<string, QueryLanguage> = {
  postgresql: "postgresql",
  mysql: "mysql",
  sqlite: "sqlite",
  snowflake: "snowflake",
  bigquery: "bigquery",
  redshift: "redshift",
  mariadb: "mysql",
  sqlserver: "tsql",
  mssql: "tsql",
  tsql: "tsql",
  oracle: "plsql",
  mongodb: "json",
  cosmos: "json",
  json: "json",
  duckdb: "sql",
  clickhouse: "sql",
  csv: "sql",
  excel: "sql",
};

export interface QueryEditorProps {
  value: string;
  onChange: (value: string) => void;
  connectorType?: string;
  placeholder?: string;
  disabled?: boolean;
  height?: string;
  /** Introspected objects that drive completion and the type badges. */
  schemaObjects?: SchemaObject[];
  /** Ctrl/Cmd+Enter. Receives the selection, or the statement under the caret. */
  onRun?: (text: string, scope: "selection" | "statement" | "all") => void;
  /** Ctrl/Cmd+Shift+Enter — run the whole buffer regardless of caret. */
  onRunAll?: () => void;
  /** Bind parameters found in the buffer, in first-appearance order. */
  onParamsChange?: (params: string[]) => void;
  /** Ctrl/Cmd+E. */
  onExplain?: () => void;
  busy?: boolean;
}

export interface QueryEditorHandle {
  /** Insert text at the caret, replacing any selection, and refocus. */
  insertAtCaret: (text: string) => void;
  focus: () => void;
}

function guessLanguage(connectorType?: string): QueryLanguage {
  if (!connectorType) return "sql";
  const t = connectorType.toLowerCase().replace(/[^a-z0-9]/g, "");
  return CONNECTOR_LANGUAGE[t] || (t.includes("mongo") ? "json" : "sql");
}

function validateQuery(language: QueryLanguage, code: string): string | null {
  const trimmed = code.trim();
  if (!trimmed) return null;

  if (language === "json") {
    try {
      const parsed = JSON.parse(trimmed);
      if (parsed === null || typeof parsed !== "object") {
        return "MongoDB/JSON mode expects an object filter or an aggregate pipeline array.";
      }
      return null;
    } catch (e) {
      return `Invalid JSON: ${(e as Error).message}`;
    }
  }

  if (language === "javascript") {
    try {
      // eslint-disable-next-line no-new-func
      new Function(trimmed);
      return null;
    } catch (e) {
      return `Invalid JavaScript syntax: ${(e as Error).message}`;
    }
  }

  // SQL: the shared advisory gate, which mirrors the server's rules and knows
  // not to trip on keywords inside literals or comments.
  const gate = checkReadOnly(trimmed);
  if (!gate.ok) {
    return gate.statement && gate.statement > 1
      ? `Statement ${gate.statement}: ${gate.reason}`
      : (gate.reason ?? "Query rejected.");
  }
  return null;
}

function leadingIndent(line: string): string {
  const m = line.match(/^(\s*)/);
  return m ? m[1] : "";
}

function insertAtCursor(
  value: string,
  start: number,
  end: number,
  insert: string,
): { next: string; cursor: number } {
  const next = value.slice(0, start) + insert + value.slice(end);
  return { next, cursor: start + insert.length };
}

const SQL_SNIPPETS = [
  { label: "SELECT *", text: "SELECT * FROM table_name" },
  { label: "WHERE", text: "WHERE column = :value" },
  { label: "JOIN", text: "JOIN other_table ON a.id = b.id" },
  { label: "GROUP BY", text: "GROUP BY column" },
  { label: "ORDER BY", text: "ORDER BY column DESC" },
  { label: "LIMIT", text: "LIMIT 100" },
  { label: "WITH CTE", text: "WITH cte AS (\n  SELECT * FROM table_name\n)\nSELECT * FROM cte" },
];

const MONGO_SNIPPETS = [
  { label: "Find filter", text: '{"status": "active"}' },
  { label: "Aggregate", text: '[\n  {"$match": {"status": "active"}},\n  {"$limit": 100}\n]' },
  { label: "Group", text: '[\n  {"$group": {"_id": "$field", "count": {"$sum": 1}}}\n]' },
  { label: "Range", text: '{"created_at": {"$gte": "2024-01-01", "$lte": "2024-12-31"}}' },
];

/** Characters that keep an open completion popup filtering rather than closing it. */
const IDENT_CHAR = /[A-Za-z0-9_$.]/;

const MOD_LABEL = typeof navigator !== "undefined" && /Mac|iP(hone|ad)/.test(navigator.platform)
  ? "⌘"
  : "Ctrl";

export const QueryEditor = forwardRef<QueryEditorHandle, QueryEditorProps>(function QueryEditor({
  value,
  onChange,
  connectorType,
  placeholder,
  disabled,
  height = "20rem",
  schemaObjects = [],
  onRun,
  onRunAll,
  onParamsChange,
  onExplain,
  busy,
}: QueryEditorProps, ref) {
  const [lang, setLang] = useState<QueryLanguage>(() => guessLanguage(connectorType));
  const [langPinned, setLangPinned] = useState(false);
  const [cursor, setCursor] = useState({ start: 0, end: 0 });
  const [completions, setCompletions] = useState<Completion[]>([]);
  const [activeIdx, setActiveIdx] = useState(0);
  const [caretBox, setCaretBox] = useState({ left: 0, top: 0 });
  const [charBox, setCharBox] = useState({ w: 7.8, h: 21 });

  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const preRef = useRef<HTMLPreElement>(null);
  const gutterRef = useRef<HTMLDivElement>(null);
  const measureRef = useRef<HTMLSpanElement>(null);

  // Connector changes retarget the dialect, unless the operator chose one —
  // silently overriding an explicit choice is how consoles lose trust.
  useEffect(() => {
    if (langPinned) return;
    setLang(guessLanguage(connectorType));
  }, [connectorType, langPinned]);

  const isMongoLike = lang === "json" || lang === "javascript";
  const dialect = useMemo(
    () => (isMongoLike ? "sql" : dialectForConnector(connectorType || lang)),
    [connectorType, lang, isMongoLike],
  );

  const label = useMemo(() => LANGUAGE_OPTIONS.find((o) => o.value === lang)?.label ?? "SQL", [lang]);
  const hint = useMemo(() => LANGUAGE_OPTIONS.find((o) => o.value === lang)?.hint ?? "", [lang]);
  const error = useMemo(() => validateQuery(lang, value), [lang, value]);
  const isInvalid = Boolean(error);
  const highlightDialect = dialectForLanguage(lang);
  const highlighted = useMemo(() => highlightCode(value, highlightDialect), [value, highlightDialect]);
  const lineCount = Math.max(1, (value.match(/\n/g)?.length ?? 0) + 1);
  const snippets = isMongoLike ? MONGO_SNIPPETS : SQL_SNIPPETS;
  const hasContent = value.trim().length > 0;

  const statements = useMemo(
    () => (isMongoLike ? [] : splitStatements(value)),
    [value, isMongoLike],
  );
  const currentStatement = useMemo(() => {
    if (isMongoLike || statements.length === 0) return 0;
    const st = statementAtCursor(value, cursor.start);
    if (!st) return 0;
    return statements.findIndex((s) => s.start === st.start) + 1;
  }, [isMongoLike, statements, value, cursor.start]);

  const params = useMemo(
    () => (isMongoLike ? [] : extractBindParams(value)),
    [value, isMongoLike],
  );
  // Report by identity-stable key: a new array every render would loop.
  const paramKey = params.join(",");
  useEffect(() => {
    onParamsChange?.(paramKey ? paramKey.split(",") : []);
  }, [paramKey, onParamsChange]);

  // Measure the real glyph box so the popup lands on the caret at any zoom or
  // font-size setting instead of assuming fixed metrics.
  useLayoutEffect(() => {
    const el = measureRef.current;
    if (!el) return;
    const rect = el.getBoundingClientRect();
    if (rect.width > 0) {
      setCharBox({ w: rect.width / 10, h: rect.height });
    }
  }, [height]);

  const closeCompletions = useCallback(() => {
    setCompletions([]);
    setActiveIdx(0);
  }, []);

  const schemaKey = useMemo(
    () => schemaObjects.map((o) => `${o.name}:${o.columns?.length ?? 0}`).join("|"),
    [schemaObjects],
  );

  const openCompletions = useCallback(
    (text: string, pos: number, explicit: boolean) => {
      if (isMongoLike || disabled) return;
      const ctx = analyzeContext(text, pos);
      // Never complete inside a string or comment — that is how editors
      // corrupt working queries.
      if (ctx.inLiteral) {
        closeCompletions();
        return;
      }
      // Implicit popups need something typed; explicit (Ctrl+Space) always opens.
      if (!explicit && !ctx.prefix && !ctx.qualifier) {
        closeCompletions();
        return;
      }
      const items = buildCompletions(ctx, schemaObjects, dialect, { limit: 40 });
      setCompletions(items);
      setActiveIdx(0);

      // Caret box from the replace offset, so the popup aligns with the token
      // being completed rather than the end of the line.
      const upto = text.slice(0, ctx.replaceFrom);
      const line = (upto.match(/\n/g)?.length ?? 0) + 1;
      const col = upto.length - (upto.lastIndexOf("\n") + 1);
      const ta = textareaRef.current;
      setCaretBox({
        left: Math.max(0, col * charBox.w - (ta?.scrollLeft ?? 0)),
        top: line * charBox.h - (ta?.scrollTop ?? 0),
      });
    },
    // schemaKey (not the array identity) keeps this stable across re-renders.
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [isMongoLike, disabled, schemaKey, dialect, charBox.w, charBox.h, closeCompletions],
  );

  const setCaret = (pos: number) => {
    window.setTimeout(() => {
      const ta = textareaRef.current;
      if (!ta) return;
      ta.focus();
      ta.setSelectionRange(pos, pos);
      setCursor({ start: pos, end: pos });
    }, 0);
  };

  const applyCompletion = useCallback(
    (item: Completion) => {
      const ta = textareaRef.current;
      const pos = ta?.selectionStart ?? cursor.start;
      const ctx = analyzeContext(value, pos);
      const insert = item.insert ?? item.label;
      const next = value.slice(0, ctx.replaceFrom) + insert + value.slice(pos);
      onChange(next);
      closeCompletions();
      setCaret(ctx.replaceFrom + insert.length);
    },
    [value, cursor.start, onChange, closeCompletions],
  );

  const syncScroll = () => {
    const ta = textareaRef.current;
    if (!ta) return;
    if (preRef.current) {
      preRef.current.scrollTop = ta.scrollTop;
      preRef.current.scrollLeft = ta.scrollLeft;
    }
    if (gutterRef.current) {
      gutterRef.current.scrollTop = ta.scrollTop;
    }
    // The popup is positioned in editor space; scrolling invalidates it.
    if (completions.length) closeCompletions();
  };

  const handleClear = () => {
    onChange("");
    closeCompletions();
    window.setTimeout(() => textareaRef.current?.focus(), 0);
  };

  const handleFormat = () => {
    if (!value.trim()) return;
    if (lang === "json") {
      try {
        onChange(JSON.stringify(JSON.parse(value), null, 2));
      } catch {
        /* validation banner already explains the parse failure */
      }
      return;
    }
    if (isMongoLike) return;
    onChange(formatSql(value));
  };

  const doRun = () => {
    const ta = textareaRef.current;
    if (!onRun) return;
    if (isMongoLike) {
      onRun(value.trim(), "all");
      return;
    }
    const start = ta?.selectionStart ?? cursor.start;
    const end = ta?.selectionEnd ?? cursor.end;
    const target = resolveRunTarget(value, start, end);
    if (target.text.trim()) onRun(target.text, target.scope);
  };

  const insertSnippet = useCallback(
    (text: string) => {
      const ta = textareaRef.current;
      // Live selection wins over the last tracked one: the schema browser can
      // insert while the textarea has not fired onSelect since the last edit.
      const start = ta?.selectionStart ?? cursor.start;
      const end = ta?.selectionEnd ?? cursor.end;
      const before = value.slice(0, start);
      const after = value.slice(end);
      const prefix = before.length > 0 && !/[(\s\n]$/.test(before) ? " " : "";
      onChange(before + prefix + text + after);
      setCaret(start + prefix.length + text.length);
    },
    [value, cursor.start, cursor.end, onChange],
  );

  useImperativeHandle(
    ref,
    () => ({
      insertAtCaret: insertSnippet,
      focus: () => textareaRef.current?.focus(),
    }),
    [insertSnippet],
  );

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    const ta = e.currentTarget;
    const { selectionStart, selectionEnd } = ta;
    if (selectionStart == null || selectionEnd == null) return;
    const mod = e.metaKey || e.ctrlKey;
    const popupOpen = completions.length > 0;

    // --- completion navigation takes precedence while the popup is open ---
    if (popupOpen) {
      if (e.key === "ArrowDown") {
        e.preventDefault();
        setActiveIdx((i) => (i + 1) % completions.length);
        return;
      }
      if (e.key === "ArrowUp") {
        e.preventDefault();
        setActiveIdx((i) => (i - 1 + completions.length) % completions.length);
        return;
      }
      if (e.key === "Enter" || e.key === "Tab") {
        e.preventDefault();
        applyCompletion(completions[activeIdx]);
        return;
      }
      if (e.key === "Escape") {
        e.preventDefault();
        closeCompletions();
        return;
      }
    }

    if (mod && e.key === "Enter") {
      e.preventDefault();
      closeCompletions();
      if (e.shiftKey) onRunAll?.();
      else doRun();
      return;
    }

    if (mod && e.code === "Space") {
      e.preventDefault();
      openCompletions(value, selectionStart, true);
      return;
    }

    if (mod && e.shiftKey && (e.key === "F" || e.key === "f")) {
      e.preventDefault();
      handleFormat();
      return;
    }

    if (mod && (e.key === "E" || e.key === "e") && onExplain) {
      e.preventDefault();
      onExplain();
      return;
    }

    if (e.key === "Tab") {
      e.preventDefault();
      const { next, cursor: pos } = insertAtCursor(value, selectionStart, selectionEnd, "  ");
      onChange(next);
      setCaret(pos);
      return;
    }

    if (e.key === "Enter" && !isMongoLike) {
      const lineStart = value.lastIndexOf("\n", selectionStart - 1) + 1;
      const currentLine = value.slice(lineStart, selectionStart);
      const indent = leadingIndent(currentLine);
      const extra = /(\(|\[\s*$|,\s*$)/.test(currentLine.trimEnd()) ? "  " : "";
      e.preventDefault();
      const { next, cursor: pos } = insertAtCursor(
        value,
        selectionStart,
        selectionEnd,
        `\n${indent}${extra}`,
      );
      onChange(next);
      setCaret(pos);
    }
  };

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    const next = e.target.value;
    onChange(next);
    const pos = e.target.selectionStart ?? next.length;
    const typed = next[pos - 1] ?? "";
    // Reopen while the operator is still inside an identifier; otherwise a
    // popup would hang around over unrelated text.
    if (IDENT_CHAR.test(typed)) openCompletions(next, pos, false);
    else closeCompletions();
  };

  const canFormat = hasContent && (lang === "json" || !isMongoLike);
  const runHint = isMongoLike ? "Run" : "Run selection or statement at caret";

  return (
    <div
      className="df2-query-editor-shell"
      style={{ minHeight: height }}
      data-invalid={isInvalid}
      data-dialect={highlightDialect}
    >
      <div className="df2-query-editor-langbar">
        <span className="df2-query-editor-lang-label">Syntax</span>
        <select
          className="df2-select df2-select-sm"
          value={lang}
          onChange={(e) => {
            setLang(e.target.value as QueryLanguage);
            setLangPinned(true);
          }}
          disabled={disabled}
          aria-label="Query language"
        >
          {LANGUAGE_OPTIONS.map((o) => (
            <option key={o.value} value={o.value}>
              {o.label}
            </option>
          ))}
        </select>
        <span className="df2-query-editor-hint">
          {label} · {hint}
        </span>
        <span className="df2-query-editor-dialect-pill" title="Active highlighter">
          {lang === "json" ? "JSON" : lang === "javascript" ? "JS" : "SQL"}
        </span>
        {!isMongoLike && statements.length > 1 && (
          <span
            className="df2-qe-stat"
            title="Ctrl/Cmd+Enter runs only the statement under the caret"
          >
            stmt {currentStatement || 1}/{statements.length}
          </span>
        )}
        {params.length > 0 && (
          <span className="df2-qe-stat df2-qe-stat--param" title="Bound server-side, never inlined">
            {params.length} param{params.length === 1 ? "" : "s"}
          </span>
        )}
        {schemaObjects.length > 0 && (
          <span className="df2-qe-stat" title="Objects available to autocomplete">
            <DtIcon name="database" size={11} /> {schemaObjects.length}
          </span>
        )}
        <div className="df2-query-editor-toolbar-actions">
          {canFormat && (
            <button
              type="button"
              className="df2-query-editor-action"
              onClick={handleFormat}
              disabled={disabled}
              title={`Format (${MOD_LABEL}+Shift+F)`}
            >
              <DtIcon name="code" size={14} />
              Format
            </button>
          )}
          {onExplain && !isMongoLike && (
            <button
              type="button"
              className="df2-query-editor-action"
              onClick={onExplain}
              disabled={disabled || !hasContent}
              title={`Explain plan (${MOD_LABEL}+E)`}
            >
              <DtIcon name="scan" size={14} />
              Explain
            </button>
          )}
          {onRun && (
            <button
              type="button"
              className="df2-query-editor-action df2-query-editor-action--run"
              onClick={doRun}
              disabled={disabled || !hasContent || busy}
              title={`${runHint} (${MOD_LABEL}+Enter)`}
            >
              <DtIcon name={busy ? "spinner" : "play"} size={14} />
              {busy ? "Running…" : "Run"}
            </button>
          )}
          <button
            type="button"
            className="df2-query-editor-action df2-query-editor-action--clear"
            onClick={handleClear}
            disabled={disabled || !hasContent}
            title="Clear editor"
          >
            <DtIcon name="x" size={14} />
            Clear
          </button>
        </div>
      </div>

      <div className="df2-query-editor-wrap df2-query-editor-wrap--powered" data-disabled={disabled}>
        <div className="df2-query-editor-gutter" ref={gutterRef} aria-hidden>
          {Array.from({ length: lineCount }, (_, i) => (
            <span key={i}>{i + 1}</span>
          ))}
        </div>
        <div className="df2-query-editor-code">
          {/* Glyph ruler for caret math — ten chars wide, never visible. */}
          <span ref={measureRef} className="df2-qe-measure" aria-hidden>
            0123456789
          </span>
          <pre
            ref={preRef}
            className={`df2-query-editor-pre qe-pre qe-pre--${highlightDialect}`}
            aria-hidden
            dangerouslySetInnerHTML={{
              __html: `${highlighted}${value.endsWith("\n") || !value ? "\n" : ""}`,
            }}
          />
          <textarea
            ref={textareaRef}
            className={`df2-query-editor-textarea ${isInvalid ? "df2-query-editor-textarea--error" : ""}`}
            value={value}
            onChange={handleChange}
            onScroll={syncScroll}
            onKeyDown={handleKeyDown}
            onBlur={closeCompletions}
            onSelect={(e) => {
              const t = e.currentTarget;
              setCursor({ start: t.selectionStart ?? 0, end: t.selectionEnd ?? 0 });
            }}
            placeholder={placeholder}
            disabled={disabled}
            spellCheck={false}
            autoComplete="off"
            autoCorrect="off"
            autoCapitalize="off"
            aria-label="Query editor"
            style={{ minHeight: height }}
          />
          <CompletionList
            items={completions}
            activeIndex={activeIdx}
            left={caretBox.left}
            top={caretBox.top}
            onPick={applyCompletion}
            onHover={setActiveIdx}
          />
        </div>
      </div>

      <div className="df2-query-editor-snippets">
        <span className="df2-query-editor-snippets-label">Insert:</span>
        {snippets.map((s) => (
          <button
            key={s.label}
            type="button"
            className="df2-query-editor-snippet"
            onClick={() => insertSnippet(s.text)}
            disabled={disabled}
            title={s.text}
          >
            {s.label}
          </button>
        ))}
        <span className="df2-qe-keys">
          {MOD_LABEL}+Enter run · {MOD_LABEL}+Shift+Enter all · {MOD_LABEL}+Space complete
        </span>
      </div>

      {isInvalid && (
        <div className="df2-query-editor-error" role="alert">
          {error}
        </div>
      )}
    </div>
  );
});
