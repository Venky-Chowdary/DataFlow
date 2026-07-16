import { useEffect, useMemo, useState } from "react";
import Editor from "react-simple-code-editor";
import Prism from "prismjs";
import "prismjs/components/prism-sql";
import "prismjs/components/prism-json";

const TYPE_TO_LANGUAGE: Record<string, string> = {
  postgresql: "sql",
  mysql: "sql",
  sqlite: "sql",
  snowflake: "sql",
  bigquery: "sql",
  redshift: "sql",
  mariadb: "sql",
  oracle: "sql",
  sqlserver: "sql",
  mongodb: "json",
  json: "json",
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
}

export function QueryEditor({ value, onChange, connectorType, placeholder, disabled, height = "16rem" }: QueryEditorProps) {
  const [lang, setLang] = useState<string>(() => guessLanguage(connectorType));

  useEffect(() => {
    setLang(guessLanguage(connectorType));
  }, [connectorType]);

  const grammar = useMemo(() => {
    if (lang === "json") return Prism.languages.json;
    return Prism.languages.sql;
  }, [lang]);

  const highlight = (code: string) => {
    if (!grammar) return code.replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c] as string));
    return Prism.highlight(code, grammar, lang);
  };

  return (
    <div className="df2-query-editor-shell" style={{ minHeight: height }}>
      <div className="df2-query-editor-langbar">
        <span className="df2-query-editor-lang-label">Syntax</span>
        <select
          className="df2-select df2-select-sm"
          value={lang}
          onChange={(e) => setLang(e.target.value)}
          disabled={disabled}
          aria-label="Query language"
        >
          <option value="sql">SQL</option>
          <option value="json">JSON / Mongo</option>
        </select>
        <span className="df2-query-editor-hint">{lang === "json" ? "Object filter or aggregate array" : "Run SELECT, WITH, EXPLAIN, or SHOW"}</span>
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

function guessLanguage(connectorType?: string): string {
  if (!connectorType) return "sql";
  const t = connectorType.toLowerCase();
  return TYPE_TO_LANGUAGE[t] || (t.includes("mongo") ? "json" : "sql");
}
