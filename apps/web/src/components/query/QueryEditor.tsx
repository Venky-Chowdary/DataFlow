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

export function QueryEditor({ value, onChange, connectorType, placeholder, disabled, height = "18rem" }: QueryEditorProps) {
  const [lang, setLang] = useState<QueryLanguage>(() => guessLanguage(connectorType));

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
      {isInvalid && (
        <div className="df2-query-editor-error">
          <span aria-hidden>⚠</span> {error}
        </div>
      )}
    </div>
  );
}
