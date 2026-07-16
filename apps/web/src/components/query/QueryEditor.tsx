import { useEffect, useMemo, useState } from "react";
import Editor from "react-simple-code-editor";
import Prism from "prismjs";
// Core SQL grammar covers most relational dialects.
import "prismjs/components/prism-sql";
// PL/SQL for Oracle-specific syntax.
import "prismjs/components/prism-plsql";
// JSON for MongoDB filters and aggregation arrays.
import "prismjs/components/prism-json";
// JavaScript for MongoDB shell-style queries (db.collection.find(...)).
import "prismjs/components/prism-javascript";

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
  { value: "json", label: "MongoDB / JSON", hint: "Filter object or aggregate array" },
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
  csv: "sql",
  excel: "sql",
};

const PRISM_GRAMMAR: Record<QueryLanguage, string> = {
  sql: "sql",
  postgresql: "sql",
  mysql: "sql",
  sqlite: "sql",
  snowflake: "sql",
  bigquery: "sql",
  redshift: "sql",
  mariadb: "sql",
  tsql: "sql",
  plsql: "plsql",
  json: "json",
  javascript: "javascript",
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

  // JSON mode: must parse as a JSON object or array.
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

  // JavaScript mode: must be syntactically valid JS (not executed).
  if (language === "javascript") {
    try {
      // eslint-disable-next-line no-new-func
      new Function(trimmed);
      return null;
    } catch (e) {
      return `Invalid JavaScript syntax: ${(e as Error).message}`;
    }
  }

  // SQL dialects: allow only read/metadata queries.
  const first = firstSqlWord(trimmed);
  if (!first || !SQL_SAFE_START.has(first)) {
    return "SQL mode only supports read/metadata queries (SELECT, WITH, EXPLAIN, SHOW, DESCRIBE, ANALYZE, PRAGMA, VALUES).";
  }
  if (hasDestructiveSql(trimmed)) {
    return "Destructive keywords (INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, etc.) are not allowed in the playground.";
  }
  return null;
}

const SQL_SNIPPETS = [
  { label: "SELECT *", text: "SELECT * FROM table_name" },
  { label: "SELECT columns", text: "SELECT column1, column2 FROM table_name" },
  { label: "WHERE", text: "WHERE column = 'value'" },
  { label: "AND", text: "AND column = 'value'" },
  { label: "JOIN", text: "JOIN other_table ON table_name.id = other_table.id" },
  { label: "LEFT JOIN", text: "LEFT JOIN other_table ON table_name.id = other_table.id" },
  { label: "GROUP BY", text: "GROUP BY column" },
  { label: "ORDER BY", text: "ORDER BY column DESC" },
  { label: "LIMIT", text: "LIMIT 100" },
  { label: "WITH CTE", text: "WITH cte AS (\n  SELECT * FROM table_name\n)\nSELECT * FROM cte" },
];

const MONGO_SNIPPETS = [
  { label: "Find filter", text: '{"status": "active"}' },
  { label: "Aggregate pipeline", text: '[\n  {"$match": {"status": "active"}},\n  {"$limit": 100}\n]' },
  { label: "Group aggregate", text: '[\n  {"$group": {"_id": "$field", "count": {"$sum": 1}}}\n]' },
  { label: "Range filter", text: '{"created_at": {"$gte": "2024-01-01", "$lte": "2024-12-31"}}' },
  { label: "Projection", text: '[\n  {"$project": {"_id": 0, "name": 1, "status": 1}}\n]' },
];

export function QueryEditor({ value, onChange, connectorType, placeholder, disabled, height = "18rem" }: QueryEditorProps) {
  const [lang, setLang] = useState<QueryLanguage>(() => guessLanguage(connectorType));
  const [cursor, setCursor] = useState({ start: 0, end: 0 });

  useEffect(() => {
    setLang(guessLanguage(connectorType));
  }, [connectorType]);

  const grammarName = PRISM_GRAMMAR[lang];
  const grammar = useMemo(() => {
    const g = (Prism.languages as Record<string, Prism.Grammar | undefined>)[grammarName];
    return g;
  }, [grammarName]);

  const label = useMemo(() => LANGUAGE_OPTIONS.find((o) => o.value === lang)?.label ?? "SQL", [lang]);
  const hint = useMemo(() => LANGUAGE_OPTIONS.find((o) => o.value === lang)?.hint ?? "", [lang]);
  const error = useMemo(() => validateQuery(lang, value), [lang, value]);
  const isInvalid = Boolean(error);

  const highlight = (code: string) => {
    if (!grammar) {
      return code.replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c] as string));
    }
    return Prism.highlight(code, grammar, grammarName);
  };

  const isMongoLike = lang === "json" || lang === "javascript";
  const snippets = isMongoLike ? MONGO_SNIPPETS : SQL_SNIPPETS;

  useEffect(() => {
    const handleSelection = () => {
      const textarea = document.querySelector(".df2-query-editor-textarea") as HTMLTextAreaElement | null;
      if (textarea && document.activeElement === textarea) {
        setCursor({ start: textarea.selectionStart ?? 0, end: textarea.selectionEnd ?? 0 });
      }
    };
    document.addEventListener("selectionchange", handleSelection);
    return () => document.removeEventListener("selectionchange", handleSelection);
  }, []);

  const insertSnippet = (text: string) => {
    const { start, end } = cursor;
    const before = value.slice(0, start);
    const after = value.slice(end);
    const prefix = before.length > 0 && !before.endsWith(" ") && !before.endsWith("\n") && !before.endsWith("(") ? " " : "";
    const next = before + prefix + text + after;
    onChange(next);
    const newCursor = start + prefix.length + text.length;
    window.setTimeout(() => {
      const textarea = document.querySelector(".df2-query-editor-textarea") as HTMLTextAreaElement | null;
      if (textarea) {
        textarea.focus();
        textarea.setSelectionRange(newCursor, newCursor);
      }
    }, 0);
  };

  return (
    <div className="df2-query-editor-shell" style={{ minHeight: height }} data-invalid={isInvalid}>
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
      </div>
      <div className="df2-query-editor-wrap" data-disabled={disabled}>
        <Editor
          value={value}
          onValueChange={onChange}
          highlight={highlight}
          padding={16}
          className="df2-query-editor"
          textareaClassName={`df2-query-editor-textarea ${isInvalid ? "df2-query-editor-textarea--error" : ""}`}
          preClassName={`df2-query-editor-pre language-${grammarName}`}
          placeholder={placeholder}
          disabled={disabled}
          tabSize={2}
          insertSpaces
        />
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
        <div className="df2-query-editor-error">
          <span aria-hidden>⚠</span> {error}
        </div>
      )}
    </div>
  );
}
