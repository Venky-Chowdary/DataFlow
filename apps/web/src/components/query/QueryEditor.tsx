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

export interface QueryEditorProps {
  value: string;
  onChange: (value: string) => void;
  connectorType?: string;
  placeholder?: string;
  disabled?: boolean;
  height?: string;
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

  const highlight = (code: string) => {
    if (!grammar) {
      return code.replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c] as string));
    }
    return Prism.highlight(code, grammar, grammarName);
  };

  return (
    <div className="df2-query-editor-shell" style={{ minHeight: height }}>
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
          textareaClassName="df2-query-editor-textarea"
          preClassName="df2-query-editor-pre"
          placeholder={placeholder}
          disabled={disabled}
          tabSize={2}
          insertSpaces
        />
      </div>
    </div>
  );
}

function guessLanguage(connectorType?: string): QueryLanguage {
  if (!connectorType) return "sql";
  const t = connectorType.toLowerCase().replace(/[^a-z0-9]/g, "");
  return CONNECTOR_LANGUAGE[t] || (t.includes("mongo") ? "json" : "sql");
}
