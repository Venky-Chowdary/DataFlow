import { useEffect, useMemo, useRef, useState } from "react";
import {
  dialectForLanguage,
  highlightCode,
} from "../../lib/queryHighlight";
import { DtIcon } from "../DtIcon";

/**
 * Dialect-aware query editor — zero external editor deps (works without node_modules extras).
 * Overlay highlighter + line gutter, VS Code–style tab/enter behavior.
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

const SQL_SAFE_START = new Set([
  "SELECT", "WITH", "EXPLAIN", "SHOW", "DESCRIBE", "ANALYZE", "PRAGMA", "VALUES",
]);

const SQL_DESTRUCTIVE = new Set([
  "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
  "GRANT", "REVOKE", "EXEC", "EXECUTE", "MERGE", "COPY", "LOAD",
]);

export interface QueryEditorProps {
  value: string;
  onChange: (value: string) => void;
  connectorType?: string;
  placeholder?: string;
  disabled?: boolean;
  height?: string;
}

function guessLanguage(connectorType?: string): QueryLanguage {
  if (!connectorType) return "sql";
  const t = connectorType.toLowerCase().replace(/[^a-z0-9]/g, "");
  return CONNECTOR_LANGUAGE[t] || (t.includes("mongo") ? "json" : "sql");
}

function stripSqlComments(query: string): string {
  return query
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/--[^\n]*/g, " ");
}

function firstSqlWord(query: string): string | null {
  const cleaned = stripSqlComments(query);
  const match = cleaned.match(/\b([A-Z][A-Z0-9_]*)\b/i);
  return match ? match[1].toUpperCase() : null;
}

function hasDestructiveSql(query: string): boolean {
  const words = stripSqlComments(query).match(/\b[A-Z][A-Z0-9_]*\b/gi) || [];
  return words.some((w) => SQL_DESTRUCTIVE.has(w.toUpperCase()));
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

  const first = firstSqlWord(trimmed);
  if (!first || !SQL_SAFE_START.has(first)) {
    return "SQL mode only supports read/metadata queries (SELECT, WITH, EXPLAIN, SHOW, DESCRIBE, ANALYZE, PRAGMA, VALUES).";
  }
  if (hasDestructiveSql(trimmed)) {
    return "Destructive keywords (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, etc.) are not allowed in the playground.";
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
  { label: "WHERE", text: "WHERE column = 'value'" },
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

export function QueryEditor({ value, onChange, connectorType, placeholder, disabled, height = "20rem" }: QueryEditorProps) {
  const [lang, setLang] = useState<QueryLanguage>(() => guessLanguage(connectorType));
  const [cursor, setCursor] = useState({ start: 0, end: 0 });
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const preRef = useRef<HTMLPreElement>(null);
  const gutterRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setLang(guessLanguage(connectorType));
  }, [connectorType]);

  const label = useMemo(() => LANGUAGE_OPTIONS.find((o) => o.value === lang)?.label ?? "SQL", [lang]);
  const hint = useMemo(() => LANGUAGE_OPTIONS.find((o) => o.value === lang)?.hint ?? "", [lang]);
  const error = useMemo(() => validateQuery(lang, value), [lang, value]);
  const isInvalid = Boolean(error);
  const dialect = dialectForLanguage(lang);
  const highlighted = useMemo(() => highlightCode(value, dialect), [value, dialect]);
  const lineCount = Math.max(1, (value.match(/\n/g)?.length ?? 0) + 1);
  const isMongoLike = lang === "json" || lang === "javascript";
  const snippets = isMongoLike ? MONGO_SNIPPETS : SQL_SNIPPETS;
  const hasContent = value.trim().length > 0;

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
  };

  const handleClear = () => {
    onChange("");
    window.setTimeout(() => textareaRef.current?.focus(), 0);
  };

  const handleFormat = () => {
    if (lang !== "json" || !value.trim()) return;
    try {
      const parsed = JSON.parse(value);
      onChange(JSON.stringify(parsed, null, 2));
    } catch {
      /* validation banner handles invalid JSON */
    }
  };

  const insertSnippet = (text: string) => {
    const { start, end } = cursor;
    const before = value.slice(0, start);
    const after = value.slice(end);
    const prefix = before.length > 0 && !/[(\s\n]$/.test(before) ? " " : "";
    const next = before + prefix + text + after;
    onChange(next);
    const newCursor = start + prefix.length + text.length;
    window.setTimeout(() => {
      const ta = textareaRef.current;
      if (ta) {
        ta.focus();
        ta.setSelectionRange(newCursor, newCursor);
        setCursor({ start: newCursor, end: newCursor });
      }
    }, 0);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    const ta = e.currentTarget;
    const { selectionStart, selectionEnd } = ta;
    if (selectionStart == null || selectionEnd == null) return;

    if (e.key === "Tab") {
      e.preventDefault();
      const { next, cursor: pos } = insertAtCursor(value, selectionStart, selectionEnd, "  ");
      onChange(next);
      window.setTimeout(() => {
        ta.setSelectionRange(pos, pos);
        setCursor({ start: pos, end: pos });
      }, 0);
      return;
    }

    if (e.key === "Enter" && !isMongoLike) {
      const lineStart = value.lastIndexOf("\n", selectionStart - 1) + 1;
      const currentLine = value.slice(lineStart, selectionStart);
      const indent = leadingIndent(currentLine);
      const extra = /(\(|\[\s*$|,\s*$)/.test(currentLine.trimEnd()) ? "  " : "";
      const insert = `\n${indent}${extra}`;
      e.preventDefault();
      const { next, cursor: pos } = insertAtCursor(value, selectionStart, selectionEnd, insert);
      onChange(next);
      window.setTimeout(() => {
        ta.setSelectionRange(pos, pos);
        setCursor({ start: pos, end: pos });
      }, 0);
    }
  };

  return (
    <div className="df2-query-editor-shell" style={{ minHeight: height }} data-invalid={isInvalid} data-dialect={dialect}>
      <div className="df2-query-editor-langbar">
        <span className="df2-query-editor-lang-label">Syntax</span>
        <select
          className="df2-select df2-select-sm"
          value={lang}
          onChange={(e) => setLang(e.target.value as QueryLanguage)}
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
        <div className="df2-query-editor-toolbar-actions">
          {lang === "json" && (
            <button
              type="button"
              className="df2-query-editor-action"
              onClick={handleFormat}
              disabled={disabled || !hasContent}
              title="Format JSON (pretty print)"
            >
              <DtIcon name="code" size={14} />
              Format
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
          <pre
            ref={preRef}
            className={`df2-query-editor-pre qe-pre qe-pre--${dialect}`}
            aria-hidden
            dangerouslySetInnerHTML={{
              __html: `${highlighted}${value.endsWith("\n") || !value ? "\n" : ""}`,
            }}
          />
          <textarea
            ref={textareaRef}
            className={`df2-query-editor-textarea ${isInvalid ? "df2-query-editor-textarea--error" : ""}`}
            value={value}
            onChange={(e) => onChange(e.target.value)}
            onScroll={syncScroll}
            onKeyDown={handleKeyDown}
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
      </div>

      {isInvalid && (
        <div className="df2-query-editor-error" role="alert">
          {error}
        </div>
      )}
    </div>
  );
}
