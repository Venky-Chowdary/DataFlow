/**
 * Lightweight dialect-aware syntax highlighters for Query Playground.
 * No Prism / external deps — works even when node_modules is incomplete.
 */

export type HighlightDialect = "sql" | "json" | "javascript";

const SQL_KEYWORDS = new Set(
  [
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL", "AS", "ON",
    "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS", "GROUP", "BY",
    "ORDER", "HAVING", "LIMIT", "OFFSET", "WITH", "UNION", "ALL", "DISTINCT",
    "CASE", "WHEN", "THEN", "ELSE", "END", "INSERT", "UPDATE", "DELETE", "INTO",
    "VALUES", "SET", "CREATE", "ALTER", "DROP", "TABLE", "VIEW", "INDEX",
    "EXPLAIN", "ANALYZE", "DESCRIBE", "SHOW", "PRAGMA", "TRUE", "FALSE",
    "BETWEEN", "LIKE", "ILIKE", "EXISTS", "CAST", "COALESCE", "COUNT", "SUM",
    "AVG", "MIN", "MAX", "OVER", "PARTITION", "ROW_NUMBER", "RANK", "DENSE_RANK",
    "ASC", "DESC", "NULLS", "FIRST", "LAST", "USING", "NATURAL", "INTERSECT",
    "EXCEPT", "RETURNING", "WINDOW", "FILTER", "LATERAL", "RECURSIVE",
  ].map((k) => k.toLowerCase()),
);

function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function wrap(cls: string, text: string): string {
  return `<span class="qe-tok qe-tok--${cls}">${escapeHtml(text)}</span>`;
}

/** Highlight SQL / warehouse dialects with keyword, string, number, comment tokens. */
export function highlightSql(code: string): string {
  if (!code) return "";
  let out = "";
  let i = 0;
  const n = code.length;

  while (i < n) {
    // Line comment --
    if (code[i] === "-" && code[i + 1] === "-") {
      let j = i + 2;
      while (j < n && code[j] !== "\n") j += 1;
      out += wrap("comment", code.slice(i, j));
      i = j;
      continue;
    }
    // Block comment /* */
    if (code[i] === "/" && code[i + 1] === "*") {
      let j = i + 2;
      while (j < n - 1 && !(code[j] === "*" && code[j + 1] === "/")) j += 1;
      j = Math.min(n, j + 2);
      out += wrap("comment", code.slice(i, j));
      i = j;
      continue;
    }
    // String '...' or "..."
    if (code[i] === "'" || code[i] === '"') {
      const q = code[i];
      let j = i + 1;
      while (j < n) {
        if (code[j] === q && code[j + 1] === q) {
          j += 2;
          continue;
        }
        if (code[j] === q) {
          j += 1;
          break;
        }
        j += 1;
      }
      out += wrap("string", code.slice(i, j));
      i = j;
      continue;
    }
    // Number
    if (/[0-9]/.test(code[i]) && (i === 0 || /[\s(,=<>+\-*/%]/.test(code[i - 1]))) {
      let j = i;
      while (j < n && /[0-9._]/.test(code[j])) j += 1;
      out += wrap("number", code.slice(i, j));
      i = j;
      continue;
    }
    // Identifier / keyword
    if (/[A-Za-z_]/.test(code[i])) {
      let j = i + 1;
      while (j < n && /[A-Za-z0-9_$]/.test(code[j])) j += 1;
      const word = code.slice(i, j);
      if (SQL_KEYWORDS.has(word.toLowerCase())) {
        out += wrap("keyword", word);
      } else if (j < n && code[j] === "(") {
        out += wrap("function", word);
      } else {
        out += wrap("ident", word);
      }
      i = j;
      continue;
    }
    // Operators / punctuation
    if (/[=<>!+\-*/%|,.;:()[\]{}]/.test(code[i])) {
      out += wrap("punct", code[i]);
      i += 1;
      continue;
    }
    out += escapeHtml(code[i]);
    i += 1;
  }
  return out;
}

/** Highlight JSON with keys, strings, numbers, literals. */
export function highlightJson(code: string): string {
  if (!code) return "";
  let out = "";
  let i = 0;
  const n = code.length;

  while (i < n) {
    if (code[i] === '"' ) {
      let j = i + 1;
      let escaped = false;
      while (j < n) {
        if (escaped) {
          escaped = false;
          j += 1;
          continue;
        }
        if (code[j] === "\\") {
          escaped = true;
          j += 1;
          continue;
        }
        if (code[j] === '"') {
          j += 1;
          break;
        }
        j += 1;
      }
      const chunk = code.slice(i, j);
      // Key if followed by :
      let k = j;
      while (k < n && /\s/.test(code[k])) k += 1;
      const isKey = code[k] === ":";
      out += wrap(isKey ? "key" : "string", chunk);
      i = j;
      continue;
    }
    if (/[0-9-]/.test(code[i]) && (i === 0 || /[\s:\[\],]/.test(code[i - 1]))) {
      let j = i;
      if (code[j] === "-") j += 1;
      while (j < n && /[0-9.eE+-]/.test(code[j])) j += 1;
      out += wrap("number", code.slice(i, j));
      i = j;
      continue;
    }
    if (/[a-z]/.test(code[i])) {
      let j = i;
      while (j < n && /[a-z]/.test(code[j])) j += 1;
      const lit = code.slice(i, j);
      if (lit === "true" || lit === "false" || lit === "null") {
        out += wrap("keyword", lit);
      } else {
        out += escapeHtml(lit);
      }
      i = j;
      continue;
    }
    if (/[{}[\]:,]/.test(code[i])) {
      out += wrap("punct", code[i]);
      i += 1;
      continue;
    }
    out += escapeHtml(code[i]);
    i += 1;
  }
  return out;
}

/** Minimal JS highlighter for Mongo shell snippets. */
export function highlightJavascript(code: string): string {
  if (!code) return "";
  // Reuse SQL-ish tokenization with JS keywords
  const JS_KW = new Set([
    "const", "let", "var", "function", "return", "if", "else", "for", "while",
    "true", "false", "null", "undefined", "new", "this", "await", "async",
    "db", "find", "aggregate", "match", "project", "group", "sort", "limit",
  ]);
  let out = "";
  let i = 0;
  const n = code.length;
  while (i < n) {
    if (code[i] === "/" && code[i + 1] === "/") {
      let j = i + 2;
      while (j < n && code[j] !== "\n") j += 1;
      out += wrap("comment", code.slice(i, j));
      i = j;
      continue;
    }
    if (code[i] === '"' || code[i] === "'" || code[i] === "`") {
      const q = code[i];
      let j = i + 1;
      while (j < n && code[j] !== q) {
        if (code[j] === "\\") j += 2;
        else j += 1;
      }
      j = Math.min(n, j + 1);
      out += wrap("string", code.slice(i, j));
      i = j;
      continue;
    }
    if (/[0-9]/.test(code[i])) {
      let j = i;
      while (j < n && /[0-9.xXa-fA-F]/.test(code[j])) j += 1;
      out += wrap("number", code.slice(i, j));
      i = j;
      continue;
    }
    if (/[A-Za-z_$]/.test(code[i])) {
      let j = i + 1;
      while (j < n && /[A-Za-z0-9_$]/.test(code[j])) j += 1;
      const word = code.slice(i, j);
      if (JS_KW.has(word)) out += wrap("keyword", word);
      else if (j < n && code[j] === "(") out += wrap("function", word);
      else out += wrap("ident", word);
      i = j;
      continue;
    }
    if (/[=<>!+\-*/%|,.;:()[\]{}]/.test(code[i])) {
      out += wrap("punct", code[i]);
      i += 1;
      continue;
    }
    out += escapeHtml(code[i]);
    i += 1;
  }
  return out;
}

export function highlightCode(code: string, dialect: HighlightDialect): string {
  if (dialect === "json") return highlightJson(code);
  if (dialect === "javascript") return highlightJavascript(code);
  return highlightSql(code);
}

export function dialectForLanguage(lang: string): HighlightDialect {
  if (lang === "json") return "json";
  if (lang === "javascript") return "javascript";
  return "sql";
}
