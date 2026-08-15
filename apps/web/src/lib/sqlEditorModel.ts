/**
 * Dialect SQL editor model — same deny grammar as procedure_source.py.
 * Highlight + diagnose share one tokenizer so the paste box cannot lie.
 */

export type SqlTokenKind =
  | "keyword"
  | "type"
  | "func"
  | "string"
  | "comment"
  | "number"
  | "bind"
  | "ident"
  | "op"
  | "punct"
  | "error"
  | "ws";

export interface SqlToken {
  kind: SqlTokenKind;
  text: string;
}

export interface SqlDiagnosis {
  ok: boolean;
  mode: "query" | "procedure";
  dialect: string;
  statement: string;
  binds: string[];
  error: string;
}

const KEYWORDS = new Set([
  "select", "from", "where", "and", "or", "not", "in", "as", "on", "join",
  "left", "right", "inner", "outer", "full", "cross", "group", "by", "order",
  "having", "limit", "offset", "union", "all", "distinct", "with", "recursive",
  "insert", "update", "delete", "merge", "into", "values", "set", "call",
  "exec", "execute", "return", "returns", "begin", "end", "declare", "if",
  "then", "else", "case", "when", "null", "true", "false", "is", "like",
  "between", "exists", "over", "partition", "rows", "unbounded", "preceding",
  "create", "alter", "drop", "truncate", "grant", "revoke", "table", "view",
  "procedure", "function", "schema", "database",
]);

const TYPES = new Set([
  "int", "integer", "bigint", "smallint", "tinyint", "decimal", "numeric",
  "float", "real", "double", "money", "bit", "bool", "boolean", "char",
  "varchar", "nvarchar", "text", "ntext", "date", "time", "datetime",
  "timestamp", "timestamptz", "json", "jsonb", "uuid", "bytea", "blob",
  "clob", "xml", "geography", "geometry",
]);

const FUNCS = new Set([
  "count", "sum", "avg", "min", "max", "coalesce", "nullif", "cast",
  "convert", "isnull", "nvl", "greatest", "least", "now", "current_date",
  "current_timestamp", "dateadd", "datediff", "extract", "date_trunc",
  "to_char", "to_date", "json_value", "json_query",
]);

const DENIED = /\b(insert|update|delete|drop|create|alter|truncate|grant|revoke|merge|copy|load|replace|openrowset|opendatasource|openquery|bulk|shutdown|dbcc|xp_cmdshell|into\s+outfile|into\s+dumpfile|execute\s+immediate|sp_executesql)\b/i;

export function tokenizeSql(text: string): SqlToken[] {
  const src = String(text || "");
  const out: SqlToken[] = [];
  let i = 0;
  while (i < src.length) {
    const ch = src[i];
    if (ch === " " || ch === "\t" || ch === "\n" || ch === "\r") {
      let j = i + 1;
      while (j < src.length && /\s/.test(src[j])) j += 1;
      out.push({ kind: "ws", text: src.slice(i, j) });
      i = j;
      continue;
    }
    if (ch === "-" && src[i + 1] === "-") {
      let j = i + 2;
      while (j < src.length && src[j] !== "\n") j += 1;
      out.push({ kind: "comment", text: src.slice(i, j) });
      i = j;
      continue;
    }
    if (ch === "/" && src[i + 1] === "*") {
      const end = src.indexOf("*/", i + 2);
      const j = end === -1 ? src.length : end + 2;
      out.push({ kind: end === -1 ? "error" : "comment", text: src.slice(i, j) });
      i = j;
      continue;
    }
    if (ch === "'" || ch === "\"") {
      let j = i + 1;
      while (j < src.length) {
        if (src[j] === ch && src[j + 1] === ch) {
          j += 2;
          continue;
        }
        if (src[j] === ch) {
          j += 1;
          break;
        }
        j += 1;
      }
      const closed = src[j - 1] === ch;
      out.push({ kind: closed ? "string" : "error", text: src.slice(i, j) });
      i = j;
      continue;
    }
    if (ch === ":" && /[A-Za-z_]/.test(src[i + 1] || "")) {
      let j = i + 2;
      while (j < src.length && /[A-Za-z0-9_]/.test(src[j])) j += 1;
      out.push({ kind: "bind", text: src.slice(i, j) });
      i = j;
      continue;
    }
    if (ch === "@" && /[A-Za-z_]/.test(src[i + 1] || "")) {
      let j = i + 2;
      while (j < src.length && /[A-Za-z0-9_]/.test(src[j])) j += 1;
      out.push({ kind: "bind", text: src.slice(i, j) });
      i = j;
      continue;
    }
    if (/[0-9]/.test(ch) || (ch === "." && /[0-9]/.test(src[i + 1] || ""))) {
      let j = i + 1;
      while (j < src.length && /[0-9.eE+-]/.test(src[j])) j += 1;
      out.push({ kind: "number", text: src.slice(i, j) });
      i = j;
      continue;
    }
    if (/[A-Za-z_]/.test(ch)) {
      let j = i + 1;
      while (j < src.length && /[A-Za-z0-9_]/.test(src[j])) j += 1;
      const word = src.slice(i, j);
      const low = word.toLowerCase();
      const kind: SqlTokenKind = KEYWORDS.has(low)
        ? "keyword"
        : TYPES.has(low)
          ? "type"
          : FUNCS.has(low)
            ? "func"
            : "ident";
      out.push({ kind, text: word });
      i = j;
      continue;
    }
    if ("(),.;=*<>!+-/%|&".includes(ch)) {
      out.push({ kind: ch === ";" ? "punct" : "op", text: ch });
      i += 1;
      continue;
    }
    out.push({ kind: "op", text: ch });
    i += 1;
  }
  return out;
}

export function bindNamesFromTokens(tokens: SqlToken[]): string[] {
  const names: string[] = [];
  const seen = new Set<string>();
  for (const tok of tokens) {
    if (tok.kind !== "bind") continue;
    const name = tok.text.replace(/^[:@]/, "");
    if (name && !seen.has(name)) {
      seen.add(name);
      names.push(name);
    }
  }
  return names;
}

export function diagnoseSql(
  text: string,
  opts: { mode: "query" | "procedure"; dialect?: string; bound?: Record<string, string> },
): SqlDiagnosis {
  const dialect = String(opts.dialect || "").toLowerCase();
  const raw = String(text || "");
  const stripped = raw
    .replace(/\/\*[\s\S]*?\*\//g, " ")
    .replace(/--[^\n]*/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  const tokens = tokenizeSql(raw);
  const binds = bindNamesFromTokens(tokens);
  const bound = opts.bound || {};
  if (!stripped) {
    return {
      ok: false,
      mode: opts.mode,
      dialect,
      statement: "",
      binds,
      error: opts.mode === "query"
        ? "Paste one read-only SELECT / WITH."
        : "Paste one CALL / EXEC, or a set-returning function.",
    };
  }
  if (stripped.includes(";") && stripped.replace(/;+\s*$/, "").includes(";")) {
    return {
      ok: false,
      mode: opts.mode,
      dialect,
      statement: "",
      binds,
      error: "Only one statement is allowed — remove extra semicolons.",
    };
  }
  if (DENIED.test(stripped) && (opts.mode === "query" || !/^\s*(call|exec(?:ute)?)\b/i.test(stripped))) {
    return {
      ok: false,
      mode: opts.mode,
      dialect,
      statement: "",
      binds,
      error: "This statement is not an extract — DDL, DML, and admin calls are blocked.",
    };
  }
  if (opts.mode === "query" && !/^\s*(select|with|explain|show|describe|desc|values)\b/i.test(stripped)) {
    return {
      ok: false,
      mode: opts.mode,
      dialect,
      statement: "",
      binds,
      error: "Query source allows one read-only SELECT/WITH — CALL belongs in Stored procedure.",
    };
  }
  const bareIdent = /^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2}$/.test(
    stripped.replace(/;+$/, ""),
  );
  if (
    opts.mode === "procedure"
    && !bareIdent
    && /^(select|with)\b/i.test(stripped)
    && !/select\s+\*\s+from\s+[A-Za-z_][A-Za-z0-9_.]*\s*\(/i.test(stripped)
  ) {
    if (!["postgresql", "postgres", "pgvector", "redshift"].includes(dialect) || !/select\s+\*\s+from\s+/i.test(stripped)) {
      return {
        ok: false,
        mode: opts.mode,
        dialect,
        statement: "",
        binds,
        error: "Stored procedure mode wants CALL / EXEC (PostgreSQL may use SELECT * FROM fn()).",
      };
    }
  }
  const missing = binds.filter((n) => bound[n] == null || String(bound[n]).trim() === "");
  if (missing.length) {
    return {
      ok: false,
      mode: opts.mode,
      dialect,
      statement: firstVerb(stripped),
      binds,
      error: `Bind :${missing.join(", :")} is not set.`,
    };
  }
  return {
    ok: true,
    mode: opts.mode,
    dialect,
    statement: firstVerb(stripped),
    binds,
    error: "",
  };
}

function firstVerb(text: string): string {
  const m = text.match(/^(call|exec(?:ute)?|select|with)/i);
  return (m?.[1] || "SQL").toUpperCase();
}
