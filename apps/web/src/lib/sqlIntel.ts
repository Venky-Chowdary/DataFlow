/**
 * SQL intelligence for the unified query workspace — statement splitting,
 * bind-parameter extraction, clause/alias-aware completion, and formatting.
 *
 * Pure functions, no DOM and no external deps, so the whole layer is unit
 * testable and the editor stays dependency-free (see queryHighlight.ts).
 *
 * Dialect coverage matches the connector families the engine actually
 * transfers; anything unknown degrades to generic SQL rather than guessing.
 */

export type SqlDialect =
  | "sql"
  | "postgresql"
  | "mysql"
  | "sqlite"
  | "snowflake"
  | "bigquery"
  | "redshift"
  | "tsql"
  | "plsql"
  | "clickhouse"
  | "duckdb";

export interface SqlStatement {
  /** Statement text without the trailing delimiter. */
  text: string;
  /** Offset of the first character in the original buffer. */
  start: number;
  /** Offset one past the last character (excluding the delimiter). */
  end: number;
  /** 1-based line number the statement starts on. */
  line: number;
}

export type CompletionKind =
  | "table"
  | "column"
  | "alias"
  | "keyword"
  | "function"
  | "schema"
  | "snippet";

export interface Completion {
  label: string;
  kind: CompletionKind;
  /** Text actually inserted; defaults to label. */
  insert?: string;
  /** Right-aligned annotation — data type, owning table, signature. */
  detail?: string;
  /** Higher sorts first, before the match-quality score. */
  boost?: number;
}

export interface SchemaColumn {
  name: string;
  /** Logical or native type as reported by introspection. */
  type?: string;
  nullable?: boolean;
  primaryKey?: boolean;
}

export interface SchemaObject {
  name: string;
  /** table | view | collection | ... */
  type?: string;
  schema?: string;
  columns?: SchemaColumn[];
  rowEstimate?: number;
}

/** Clause the cursor sits in — drives what we suggest. */
export type SqlClause =
  | "select"
  | "from"
  | "join"
  | "on"
  | "where"
  | "group"
  | "order"
  | "having"
  | "set"
  | "into"
  | "using"
  | "none";

export interface CompletionContext {
  /** Word fragment immediately before the cursor (after any dot). */
  prefix: string;
  /** Qualifier before the dot, e.g. `a` in `a.na`. Empty when unqualified. */
  qualifier: string;
  clause: SqlClause;
  /** Offset where the replaced token begins. */
  replaceFrom: number;
  /** alias -> table name, from FROM/JOIN of the statement under the cursor. */
  aliases: Record<string, string>;
  /** Tables referenced by the statement under the cursor, in order. */
  tables: string[];
  /** True when the cursor is inside a string literal or comment. */
  inLiteral: boolean;
}

// ---------------------------------------------------------------------------
// Keyword and function catalogs
// ---------------------------------------------------------------------------

const CORE_KEYWORDS = [
  "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING", "LIMIT",
  "OFFSET", "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "FULL OUTER JOIN",
  "CROSS JOIN", "ON", "USING", "AS", "AND", "OR", "NOT", "IN", "IS NULL",
  "IS NOT NULL", "BETWEEN", "LIKE", "EXISTS", "CASE", "WHEN", "THEN", "ELSE",
  "END", "DISTINCT", "WITH", "UNION", "UNION ALL", "INTERSECT", "EXCEPT",
  "ASC", "DESC", "NULLS FIRST", "NULLS LAST", "OVER", "PARTITION BY",
  "LATERAL", "RECURSIVE", "FILTER", "WINDOW", "EXPLAIN", "VALUES", "CAST",
];

const CORE_FUNCTIONS: { name: string; detail: string }[] = [
  { name: "COUNT", detail: "COUNT(*) -> integer" },
  { name: "SUM", detail: "SUM(numeric) -> numeric" },
  { name: "AVG", detail: "AVG(numeric) -> numeric" },
  { name: "MIN", detail: "MIN(any) -> any" },
  { name: "MAX", detail: "MAX(any) -> any" },
  { name: "COALESCE", detail: "COALESCE(a, b, ...) -> any" },
  { name: "NULLIF", detail: "NULLIF(a, b) -> any" },
  { name: "ABS", detail: "ABS(numeric) -> numeric" },
  { name: "ROUND", detail: "ROUND(numeric, digits) -> numeric" },
  { name: "FLOOR", detail: "FLOOR(numeric) -> numeric" },
  { name: "CEIL", detail: "CEIL(numeric) -> numeric" },
  { name: "LOWER", detail: "LOWER(text) -> text" },
  { name: "UPPER", detail: "UPPER(text) -> text" },
  { name: "TRIM", detail: "TRIM(text) -> text" },
  { name: "LENGTH", detail: "LENGTH(text) -> integer" },
  { name: "SUBSTRING", detail: "SUBSTRING(text FROM n FOR len) -> text" },
  { name: "REPLACE", detail: "REPLACE(text, from, to) -> text" },
  { name: "CONCAT", detail: "CONCAT(a, b, ...) -> text" },
  { name: "ROW_NUMBER", detail: "ROW_NUMBER() OVER (...) -> integer" },
  { name: "RANK", detail: "RANK() OVER (...) -> integer" },
  { name: "DENSE_RANK", detail: "DENSE_RANK() OVER (...) -> integer" },
  { name: "LAG", detail: "LAG(expr, offset) OVER (...) -> any" },
  { name: "LEAD", detail: "LEAD(expr, offset) OVER (...) -> any" },
];

/**
 * Dialect-specific additions. Deliberately small and accurate — a wrong
 * suggestion in a migration tool costs more trust than a missing one.
 */
const DIALECT_FUNCTIONS: Partial<Record<SqlDialect, { name: string; detail: string }[]>> = {
  postgresql: [
    { name: "NOW", detail: "NOW() -> timestamptz" },
    { name: "DATE_TRUNC", detail: "DATE_TRUNC(unit, timestamp) -> timestamp" },
    { name: "AGE", detail: "AGE(timestamp, timestamp) -> interval" },
    { name: "JSONB_BUILD_OBJECT", detail: "JSONB_BUILD_OBJECT(k, v, ...) -> jsonb" },
    { name: "JSONB_ARRAY_ELEMENTS", detail: "JSONB_ARRAY_ELEMENTS(jsonb) -> setof jsonb" },
    { name: "STRING_AGG", detail: "STRING_AGG(expr, delimiter) -> text" },
    { name: "ARRAY_AGG", detail: "ARRAY_AGG(expr) -> array" },
    { name: "GENERATE_SERIES", detail: "GENERATE_SERIES(start, stop, step) -> setof" },
    { name: "TO_CHAR", detail: "TO_CHAR(value, format) -> text" },
    { name: "PG_TYPEOF", detail: "PG_TYPEOF(any) -> regtype" },
  ],
  mysql: [
    { name: "NOW", detail: "NOW() -> datetime" },
    { name: "DATE_FORMAT", detail: "DATE_FORMAT(date, format) -> string" },
    { name: "GROUP_CONCAT", detail: "GROUP_CONCAT(expr SEPARATOR s) -> string" },
    { name: "IFNULL", detail: "IFNULL(a, b) -> any" },
    { name: "JSON_EXTRACT", detail: "JSON_EXTRACT(json, path) -> json" },
    { name: "UNIX_TIMESTAMP", detail: "UNIX_TIMESTAMP(date) -> integer" },
  ],
  sqlite: [
    { name: "DATETIME", detail: "DATETIME(timestring, modifier) -> text" },
    { name: "JSON_EXTRACT", detail: "JSON_EXTRACT(json, path) -> any" },
    { name: "GROUP_CONCAT", detail: "GROUP_CONCAT(expr, sep) -> text" },
    { name: "TYPEOF", detail: "TYPEOF(any) -> text" },
  ],
  snowflake: [
    { name: "CURRENT_TIMESTAMP", detail: "CURRENT_TIMESTAMP() -> timestamp_ltz" },
    { name: "DATE_TRUNC", detail: "DATE_TRUNC(unit, date) -> date" },
    { name: "TRY_CAST", detail: "TRY_CAST(expr AS type) -> type | NULL" },
    { name: "PARSE_JSON", detail: "PARSE_JSON(string) -> variant" },
    { name: "OBJECT_CONSTRUCT", detail: "OBJECT_CONSTRUCT(k, v, ...) -> object" },
    { name: "LISTAGG", detail: "LISTAGG(expr, delimiter) -> string" },
    { name: "FLATTEN", detail: "FLATTEN(input => variant) -> table" },
  ],
  bigquery: [
    { name: "CURRENT_TIMESTAMP", detail: "CURRENT_TIMESTAMP() -> timestamp" },
    { name: "TIMESTAMP_TRUNC", detail: "TIMESTAMP_TRUNC(ts, unit) -> timestamp" },
    { name: "SAFE_CAST", detail: "SAFE_CAST(expr AS type) -> type | NULL" },
    { name: "ARRAY_AGG", detail: "ARRAY_AGG(expr) -> array" },
    { name: "UNNEST", detail: "UNNEST(array) -> table" },
    { name: "STRING_AGG", detail: "STRING_AGG(expr, delimiter) -> string" },
    { name: "JSON_VALUE", detail: "JSON_VALUE(json, path) -> string" },
  ],
  redshift: [
    { name: "GETDATE", detail: "GETDATE() -> timestamp" },
    { name: "DATE_TRUNC", detail: "DATE_TRUNC(unit, timestamp) -> timestamp" },
    { name: "LISTAGG", detail: "LISTAGG(expr, delimiter) -> varchar" },
  ],
  tsql: [
    { name: "GETDATE", detail: "GETDATE() -> datetime" },
    { name: "ISNULL", detail: "ISNULL(a, b) -> any" },
    { name: "TRY_CONVERT", detail: "TRY_CONVERT(type, expr) -> type | NULL" },
    { name: "DATEDIFF", detail: "DATEDIFF(unit, start, end) -> int" },
    { name: "STRING_AGG", detail: "STRING_AGG(expr, sep) -> nvarchar" },
  ],
  plsql: [
    { name: "SYSDATE", detail: "SYSDATE -> date" },
    { name: "SYSTIMESTAMP", detail: "SYSTIMESTAMP -> timestamp with tz" },
    { name: "NVL", detail: "NVL(a, b) -> any" },
    { name: "TO_DATE", detail: "TO_DATE(text, format) -> date" },
    { name: "LISTAGG", detail: "LISTAGG(expr, delimiter) -> varchar2" },
    { name: "TRUNC", detail: "TRUNC(date | number) -> date | number" },
  ],
  clickhouse: [
    { name: "now", detail: "now() -> DateTime" },
    { name: "toStartOfDay", detail: "toStartOfDay(DateTime) -> DateTime" },
    { name: "groupArray", detail: "groupArray(expr) -> Array" },
    { name: "uniqExact", detail: "uniqExact(expr) -> UInt64" },
    { name: "arrayJoin", detail: "arrayJoin(Array) -> rows" },
  ],
  duckdb: [
    { name: "list_aggregate", detail: "list_aggregate(list, fn) -> any" },
    { name: "read_parquet", detail: "read_parquet(path) -> table" },
    { name: "read_csv_auto", detail: "read_csv_auto(path) -> table" },
    { name: "date_trunc", detail: "date_trunc(unit, timestamp) -> timestamp" },
  ],
};

/** Row-limit syntax differs enough that suggesting the wrong one is harmful. */
export function limitSyntax(dialect: SqlDialect, n = 100): string {
  if (dialect === "tsql") return `TOP ${n}`;
  if (dialect === "plsql") return `FETCH FIRST ${n} ROWS ONLY`;
  return `LIMIT ${n}`;
}

export function dialectFunctions(dialect: SqlDialect): { name: string; detail: string }[] {
  return [...CORE_FUNCTIONS, ...(DIALECT_FUNCTIONS[dialect] ?? [])];
}

const CONNECTOR_DIALECT: Record<string, SqlDialect> = {
  postgresql: "postgresql",
  postgres: "postgresql",
  redshift: "redshift",
  mysql: "mysql",
  mariadb: "mysql",
  sqlite: "sqlite",
  snowflake: "snowflake",
  bigquery: "bigquery",
  sqlserver: "tsql",
  mssql: "tsql",
  tsql: "tsql",
  oracle: "plsql",
  plsql: "plsql",
  clickhouse: "clickhouse",
  duckdb: "duckdb",
};

/** Map a saved-connector type onto a SQL dialect; unknown falls back to generic. */
export function dialectForConnector(connectorType?: string): SqlDialect {
  if (!connectorType) return "sql";
  const key = connectorType.toLowerCase().replace(/[^a-z0-9]/g, "");
  return CONNECTOR_DIALECT[key] ?? "sql";
}

// ---------------------------------------------------------------------------
// Statement splitting
// ---------------------------------------------------------------------------

interface ScanState {
  inSingle: boolean;
  inDouble: boolean;
  inBacktick: boolean;
  inLineComment: boolean;
  inBlockComment: boolean;
  dollarTag: string | null;
}

function blankState(): ScanState {
  return {
    inSingle: false,
    inDouble: false,
    inBacktick: false,
    inLineComment: false,
    inBlockComment: false,
    dollarTag: null,
  };
}

function inAnyLiteral(s: ScanState): boolean {
  return (
    s.inSingle ||
    s.inDouble ||
    s.inBacktick ||
    s.inLineComment ||
    s.inBlockComment ||
    s.dollarTag !== null
  );
}

/**
 * Advance the lexer state by one character, returning how many characters were
 * consumed. Handles doubled-quote escapes, `--`/`/* *\/` comments and
 * PostgreSQL dollar-quoted bodies so a `;` inside any of them never splits.
 */
function step(sql: string, i: number, s: ScanState): number {
  const c = sql[i];
  const next = sql[i + 1];

  if (s.inLineComment) {
    if (c === "\n") s.inLineComment = false;
    return 1;
  }
  if (s.inBlockComment) {
    if (c === "*" && next === "/") {
      s.inBlockComment = false;
      return 2;
    }
    return 1;
  }
  if (s.dollarTag !== null) {
    if (c === "$" && sql.startsWith(s.dollarTag, i)) {
      const len = s.dollarTag.length;
      s.dollarTag = null;
      return len;
    }
    return 1;
  }
  if (s.inSingle) {
    if (c === "'" && next === "'") return 2;
    if (c === "\\" && next) return 2;
    if (c === "'") {
      s.inSingle = false;
    }
    return 1;
  }
  if (s.inDouble) {
    if (c === '"' && next === '"') return 2;
    if (c === '"') s.inDouble = false;
    return 1;
  }
  if (s.inBacktick) {
    if (c === "`") s.inBacktick = false;
    return 1;
  }

  if (c === "-" && next === "-") {
    s.inLineComment = true;
    return 2;
  }
  if (c === "/" && next === "*") {
    s.inBlockComment = true;
    return 2;
  }
  if (c === "'") {
    s.inSingle = true;
    return 1;
  }
  if (c === '"') {
    s.inDouble = true;
    return 1;
  }
  if (c === "`") {
    s.inBacktick = true;
    return 1;
  }
  if (c === "$") {
    const tag = sql.slice(i).match(/^\$[A-Za-z_][A-Za-z0-9_]*\$|^\$\$/);
    if (tag) {
      s.dollarTag = tag[0];
      return tag[0].length;
    }
  }
  return 1;
}

/**
 * Split a buffer into executable statements. Quote-, comment- and
 * dollar-quote-aware, so `SELECT ';'` and `$$ begin ... end; $$` stay intact.
 * Statements that are only whitespace or comments are dropped.
 */
export function splitStatements(sql: string): SqlStatement[] {
  const out: SqlStatement[] = [];
  const s = blankState();
  let i = 0;
  let segStart = 0;

  const push = (start: number, end: number) => {
    const raw = sql.slice(start, end);
    if (!raw.trim()) return;
    // Leading whitespace belongs to the gap between statements, not the body.
    const lead = raw.length - raw.replace(/^\s+/, "").length;
    const text = raw.trim();
    if (!stripComments(text).trim()) return;
    const from = start + lead;
    out.push({
      text,
      start: from,
      end: from + text.length,
      line: sql.slice(0, from).split("\n").length,
    });
  };

  while (i < sql.length) {
    const before = inAnyLiteral(s);
    if (!before && sql[i] === ";") {
      push(segStart, i);
      i += 1;
      segStart = i;
      continue;
    }
    i += step(sql, i, s);
  }
  push(segStart, sql.length);
  return out;
}

/**
 * Blank out the contents of string literals and quoted identifiers, keeping
 * offsets stable. Keyword scanning must not see `SELECT 'DELETE'` as a write.
 */
export function blankLiterals(sql: string): string {
  const s = blankState();
  let out = "";
  let i = 0;
  while (i < sql.length) {
    const wasIn = s.inSingle || s.inDouble || s.inBacktick || s.dollarTag !== null;
    const start = i;
    const consumed = step(sql, i, s);
    const isIn = s.inSingle || s.inDouble || s.inBacktick || s.dollarTag !== null;
    const chunk = sql.slice(start, start + consumed);
    if (wasIn || isIn) {
      // Inside (or on the boundary of) a literal — keep length, drop content
      // except newlines so line numbers survive.
      out += chunk.replace(/[^\n]/g, " ");
    } else {
      out += chunk;
    }
    i += consumed;
  }
  return out;
}

/** Remove SQL comments while preserving string literals. */
export function stripComments(sql: string): string {
  const s = blankState();
  let out = "";
  let i = 0;
  while (i < sql.length) {
    const wasComment = s.inLineComment || s.inBlockComment;
    const start = i;
    const consumed = step(sql, i, s);
    const isComment = s.inLineComment || s.inBlockComment;
    if (!wasComment && !isComment) {
      out += sql.slice(start, start + consumed);
    } else if (wasComment && !isComment) {
      // Closing delimiter of a comment — replace with a space so tokens
      // on either side do not fuse together.
      out += " ";
    }
    i += consumed;
  }
  return out;
}

/** The statement the cursor sits in, preferring the one it touches directly. */
export function statementAtCursor(sql: string, pos: number): SqlStatement | null {
  const statements = splitStatements(sql);
  if (statements.length === 0) return null;
  for (const st of statements) {
    if (pos >= st.start && pos <= st.end) return st;
  }
  // Cursor is in trailing whitespace or on a delimiter — use the statement
  // that ends closest before it.
  let best: SqlStatement | null = null;
  for (const st of statements) {
    if (st.end <= pos && (!best || st.end > best.end)) best = st;
  }
  return best ?? statements[0];
}

/**
 * Text to execute for a run action: the selection when the user made one,
 * otherwise the single statement under the cursor.
 */
export function resolveRunTarget(
  sql: string,
  selStart: number,
  selEnd: number,
): { text: string; scope: "selection" | "statement" | "all" } {
  if (selEnd > selStart) {
    const selected = sql.slice(selStart, selEnd).trim();
    if (selected) return { text: selected, scope: "selection" };
  }
  const st = statementAtCursor(sql, selStart);
  if (st && st.text.trim()) return { text: st.text, scope: "statement" };
  return { text: sql.trim(), scope: "all" };
}

// ---------------------------------------------------------------------------
// Bind parameters
// ---------------------------------------------------------------------------

/**
 * Collect `:name` bind placeholders, in first-appearance order.
 *
 * Skips `::type` casts, literals and comments so PostgreSQL casts are never
 * mistaken for parameters. The engine binds these server-side, which is why
 * the editor surfaces them instead of letting operators interpolate values.
 */
export function extractBindParams(sql: string): string[] {
  const cleaned = stripComments(sql);
  const s = blankState();
  const names: string[] = [];
  const seen = new Set<string>();
  let i = 0;
  while (i < cleaned.length) {
    if (!inAnyLiteral(s) && (cleaned[i] === ":" || cleaned[i] === "@")) {
      // `::` is a cast; `@@` is a T-SQL global — neither is a bind.
      if (cleaned[i + 1] === cleaned[i]) {
        i += 2;
        continue;
      }
      const m = cleaned.slice(i + 1).match(/^[A-Za-z_][A-Za-z0-9_]*/);
      if (m) {
        const name = m[0];
        if (!seen.has(name)) {
          seen.add(name);
          names.push(name);
        }
        i += 1 + name.length;
        continue;
      }
    }
    i += step(cleaned, i, s);
  }
  return names;
}

// ---------------------------------------------------------------------------
// Context analysis
// ---------------------------------------------------------------------------

/**
 * A partially typed identifier at the very end of the buffer — lets
 * `FROM us|` still resolve to the `from` clause instead of falling back to
 * `select`. Matches nothing when the cursor sits after whitespace.
 */
const PARTIAL_IDENT = String.raw`[A-Za-z0-9_$."\`\[\]]*`;

const CLAUSE_PATTERNS: { clause: SqlClause; re: RegExp }[] = [
  { clause: "join", re: new RegExp(String.raw`\b(?:inner|left|right|full|cross|outer)?\s*join\s+${PARTIAL_IDENT}$|\b(?:inner|left|right|full|cross|outer)?\s*join\s*$`, "i") },
  { clause: "from", re: new RegExp(String.raw`\bfrom\s+${PARTIAL_IDENT}$|\bfrom\s*$`, "i") },
  { clause: "into", re: new RegExp(String.raw`\binto\s+${PARTIAL_IDENT}$|\binto\s*$`, "i") },
  { clause: "using", re: new RegExp(String.raw`\busing\s+${PARTIAL_IDENT}$|\busing\s*$`, "i") },
  { clause: "on", re: /\bon\b[^()]*$/i },
  { clause: "where", re: /\bwhere\b[\s\S]*$/i },
  { clause: "group", re: /\bgroup\s+by\b[\s\S]*$/i },
  { clause: "having", re: /\bhaving\b[\s\S]*$/i },
  { clause: "order", re: /\border\s+by\b[\s\S]*$/i },
  { clause: "set", re: /\bset\b[\s\S]*$/i },
  { clause: "select", re: /\bselect\b[\s\S]*$/i },
];

/**
 * Determine the clause the cursor is in. Later clauses win, so
 * `SELECT x FROM t WHERE |` resolves to `where`, not `select`.
 */
function detectClause(before: string): SqlClause {
  let best: { clause: SqlClause; at: number } | null = null;
  for (const { clause, re } of CLAUSE_PATTERNS) {
    const m = before.match(re);
    if (!m) continue;
    const at = m.index ?? -1;
    if (at < 0) continue;
    if (!best || at >= best.at) best = { clause, at };
  }
  return best?.clause ?? "none";
}

const RESERVED_AFTER_TABLE = new Set([
  "on", "using", "where", "group", "order", "having", "limit", "offset",
  "join", "inner", "left", "right", "full", "cross", "outer", "union",
  "select", "set", "and", "or", "as",
]);

function unquotePart(raw: string): string {
  const t = raw.trim();
  if (
    t.length > 1 &&
    ((t[0] === '"' && t.endsWith('"')) ||
      (t[0] === "`" && t.endsWith("`")) ||
      (t[0] === "[" && t.endsWith("]")))
  ) {
    return t.slice(1, -1);
  }
  return t;
}

/**
 * Strip quoting from every part of a possibly qualified identifier, so
 * `"public"."users"` and `public.users` resolve to the same catalog key.
 */
function unquoteIdent(raw: string): string {
  return raw
    .trim()
    .split(/\.(?=(?:[^"`\]]*(?:"[^"]*"|`[^`]*`|\[[^\]]*\]))*[^"`\]]*$)/)
    .map(unquotePart)
    .join(".");
}

/** One identifier part: quoted, backticked, bracketed, or bare. */
const IDENT_PART = String.raw`(?:"[^"]+"|\`[^\`]+\`|\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_$]*)`;
/** A possibly schema/catalog-qualified identifier, each part quoted or bare. */
const IDENT = String.raw`(?:${IDENT_PART}(?:\.${IDENT_PART})*)`;

/**
 * Extract table references and aliases from FROM / JOIN / UPDATE / INTO.
 * Alias-qualified completion depends on this, so it tolerates schema-qualified
 * names, quoted identifiers and a missing AS keyword.
 */
export function extractTableRefs(statement: string): {
  tables: string[];
  aliases: Record<string, string>;
} {
  const sql = stripComments(statement);
  const tables: string[] = [];
  const aliases: Record<string, string> = {};
  const re = new RegExp(
    String.raw`\b(?:from|join|update|into)\s+(${IDENT})(?:\s+(?:as\s+)?(${IDENT}))?`,
    "gi",
  );
  let m: RegExpExecArray | null;
  while ((m = re.exec(sql)) !== null) {
    const table = unquoteIdent(m[1]);
    if (!table || table === "(") continue;
    if (!tables.includes(table)) tables.push(table);
    const rawAlias = m[2] ? unquoteIdent(m[2]) : "";
    if (rawAlias && !RESERVED_AFTER_TABLE.has(rawAlias.toLowerCase()) && !rawAlias.includes(".")) {
      aliases[rawAlias] = table;
    }
  }
  return { tables, aliases };
}

/** Analyze the buffer at a cursor offset into a completion context. */
export function analyzeContext(sql: string, pos: number): CompletionContext {
  const statement = statementAtCursor(sql, pos);
  const stStart = statement ? statement.start : 0;
  const body = statement ? statement.text : "";
  // Slice the ORIGINAL buffer, not the trimmed statement text: trailing
  // whitespace decides whether the cursor is on a partial token or after one.
  const before = sql.slice(Math.min(stStart, pos), Math.max(stStart, pos));

  // Literal / comment detection up to the cursor.
  const s = blankState();
  let i = 0;
  while (i < before.length) {
    i += step(before, i, s);
  }
  const inLiteral = inAnyLiteral(s);

  const tokenMatch = before.match(new RegExp(String.raw`(${IDENT}|)?(\.)?([A-Za-z_][A-Za-z0-9_$]*)?$`));
  let qualifier = "";
  let prefix = "";
  if (tokenMatch) {
    const dotted = before.match(/([A-Za-z_][A-Za-z0-9_$]*)\.([A-Za-z_][A-Za-z0-9_$]*)?$/);
    if (dotted) {
      qualifier = dotted[1];
      prefix = dotted[2] ?? "";
    } else {
      const plain = before.match(/([A-Za-z_][A-Za-z0-9_$]*)$/);
      prefix = plain ? plain[1] : "";
    }
  }
  const replaceFrom = pos - prefix.length;

  const { tables, aliases } = extractTableRefs(body);
  return {
    prefix,
    qualifier,
    clause: detectClause(stripComments(before)),
    replaceFrom,
    aliases,
    tables,
    inLiteral,
  };
}

// ---------------------------------------------------------------------------
// Match scoring
// ---------------------------------------------------------------------------

/**
 * Score a candidate against a prefix: exact > prefix > word-boundary >
 * substring > subsequence. Returns -1 when there is no match at all.
 * Shorter candidates win ties so `id` outranks `identity_column`.
 */
export function scoreMatch(candidate: string, query: string): number {
  if (!query) return 1;
  const c = candidate.toLowerCase();
  const q = query.toLowerCase();
  if (c === q) return 1000;
  if (c.startsWith(q)) return 800 - Math.min(candidate.length, 100);
  const boundary = c.search(new RegExp(`[_. ]${q.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}`));
  if (boundary >= 0) return 600 - Math.min(candidate.length, 100);
  const idx = c.indexOf(q);
  if (idx >= 0) return 400 - idx - Math.min(candidate.length, 100) / 10;
  // Subsequence (fuzzy): every query char appears in order.
  let ci = 0;
  for (const ch of q) {
    ci = c.indexOf(ch, ci);
    if (ci < 0) return -1;
    ci += 1;
  }
  return 100 - Math.min(candidate.length, 100) / 10;
}

// ---------------------------------------------------------------------------
// Completion building
// ---------------------------------------------------------------------------

function columnDetail(col: SchemaColumn, table?: string): string {
  const bits: string[] = [];
  if (col.type) bits.push(col.type);
  if (col.primaryKey) bits.push("PK");
  if (col.nullable === false) bits.push("NOT NULL");
  const meta = bits.join(" · ");
  if (table && meta) return `${table} — ${meta}`;
  return table || meta;
}

/**
 * Build a ranked completion list for a cursor context.
 *
 * Clause drives the candidate pool (tables after FROM/JOIN, columns in
 * SELECT/WHERE/ON/...); a qualifier narrows columns to the aliased table.
 * Inside a string literal or comment we return nothing — completing there is
 * how editors corrupt queries.
 */
export function buildCompletions(
  ctx: CompletionContext,
  objects: SchemaObject[],
  dialect: SqlDialect = "sql",
  options: { limit?: number } = {},
): Completion[] {
  if (ctx.inLiteral) return [];
  const limit = options.limit ?? 50;
  const pool: Completion[] = [];

  const byName = new Map<string, SchemaObject>();
  for (const o of objects) {
    byName.set(o.name.toLowerCase(), o);
    if (o.schema) byName.set(`${o.schema}.${o.name}`.toLowerCase(), o);
  }

  const columnsFor = (tableName: string): SchemaColumn[] => {
    const direct = byName.get(tableName.toLowerCase());
    if (direct?.columns?.length) return direct.columns;
    // Schema-qualified reference against an unqualified catalog entry.
    const bare = tableName.split(".").pop() ?? tableName;
    return byName.get(bare.toLowerCase())?.columns ?? [];
  };

  if (ctx.qualifier) {
    // `alias.` or `table.` — only that relation's columns are valid here.
    const target = ctx.aliases[ctx.qualifier] ?? ctx.qualifier;
    for (const col of columnsFor(target)) {
      pool.push({
        label: col.name,
        kind: "column",
        detail: columnDetail(col, target),
        boost: col.primaryKey ? 60 : 40,
      });
    }
    return rank(pool, ctx.prefix, limit);
  }

  const wantsTables = ctx.clause === "from" || ctx.clause === "join" || ctx.clause === "into";
  const wantsColumns =
    ctx.clause === "select" ||
    ctx.clause === "where" ||
    ctx.clause === "on" ||
    ctx.clause === "group" ||
    ctx.clause === "order" ||
    ctx.clause === "having" ||
    ctx.clause === "set" ||
    ctx.clause === "using" ||
    ctx.clause === "none";

  if (wantsTables || ctx.clause === "none") {
    for (const o of objects) {
      const detail = [o.type || "table", o.schema, o.rowEstimate ? `~${o.rowEstimate.toLocaleString()} rows` : ""]
        .filter(Boolean)
        .join(" · ");
      pool.push({ label: o.name, kind: "table", detail, boost: wantsTables ? 80 : 20 });
    }
  }

  if (wantsColumns) {
    // In-scope tables first; when the statement names none, offer everything
    // so an empty buffer still completes.
    const scope = ctx.tables.length > 0 ? ctx.tables : objects.map((o) => o.name);
    const emitted = new Set<string>();
    for (const table of scope) {
      for (const col of columnsFor(table)) {
        const key = `${table}.${col.name}`.toLowerCase();
        if (emitted.has(key)) continue;
        emitted.add(key);
        pool.push({
          label: col.name,
          kind: "column",
          detail: columnDetail(col, table),
          boost: (col.primaryKey ? 70 : 50) + (ctx.tables.includes(table) ? 10 : 0),
        });
      }
    }
    for (const [alias, table] of Object.entries(ctx.aliases)) {
      pool.push({ label: alias, kind: "alias", detail: `alias of ${table}`, boost: 45 });
    }
    for (const fn of dialectFunctions(dialect)) {
      pool.push({
        label: fn.name,
        kind: "function",
        insert: `${fn.name}(`,
        detail: fn.detail,
        boost: 30,
      });
    }
  }

  for (const kw of CORE_KEYWORDS) {
    pool.push({ label: kw, kind: "keyword", boost: 10 });
  }

  return rank(pool, ctx.prefix, limit);
}

function rank(pool: Completion[], prefix: string, limit: number): Completion[] {
  const scored: { item: Completion; score: number }[] = [];
  const seen = new Set<string>();
  for (const item of pool) {
    const key = `${item.kind}:${item.label.toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const score = scoreMatch(item.label, prefix);
    if (score < 0) continue;
    scored.push({ item, score: score + (item.boost ?? 0) });
  }
  scored.sort((a, b) => b.score - a.score || a.item.label.localeCompare(b.item.label));
  return scored.slice(0, limit).map((s) => s.item);
}

// ---------------------------------------------------------------------------
// Formatting
// ---------------------------------------------------------------------------

const NEWLINE_BEFORE = [
  "select", "from", "where", "group by", "order by", "having", "limit",
  "offset", "union all", "union", "intersect", "except", "values", "set",
  "left join", "right join", "inner join", "full outer join", "outer join",
  "cross join", "join", "on",
];

/**
 * Format a SQL statement: one major clause per line, indented AND/OR and
 * select-list items, comments and literals preserved verbatim.
 *
 * Deliberately conservative — a formatter that rewrites a query it does not
 * fully understand is worse than no formatter, so anything unrecognised is
 * passed through untouched.
 */
export function formatSql(sql: string): string {
  if (!sql.trim()) return sql;
  const statements = splitStatements(sql);
  if (statements.length === 0) return sql;
  return statements.map((st) => formatOne(st.text)).join(";\n\n") + ";";
}

function formatOne(statement: string): string {
  const tokens = tokenizeForFormat(statement);
  let out = "";
  let depth = 0;
  let atLineStart = true;

  const emitNewline = (extraIndent = 0) => {
    out = out.replace(/[ \t]+$/, "");
    out += "\n" + "  ".repeat(Math.max(0, depth + extraIndent));
    atLineStart = true;
  };

  for (let i = 0; i < tokens.length; i += 1) {
    const tok = tokens[i];
    if (tok.type === "space") continue;

    if (tok.type === "punct" && tok.text === "(") {
      out += "(";
      depth += 1;
      atLineStart = false;
      continue;
    }
    if (tok.type === "punct" && tok.text === ")") {
      depth = Math.max(0, depth - 1);
      out = out.replace(/[ \t]+$/, "");
      out += ")";
      atLineStart = false;
      continue;
    }
    if (tok.type === "punct" && tok.text === ",") {
      out = out.replace(/[ \t]+$/, "");
      out += ",";
      emitNewline(1);
      continue;
    }

    if (tok.type === "word") {
      const lower = tok.text.toLowerCase();
      const two = `${lower} ${tokens[i + 1]?.type === "space" ? tokens[i + 2]?.text?.toLowerCase() ?? "" : tokens[i + 1]?.text?.toLowerCase() ?? ""}`.trim();
      const three = matchMultiWord(tokens, i);

      if (three && NEWLINE_BEFORE.includes(three.phrase) && depth === 0) {
        if (out.trim()) emitNewline();
        out += three.phrase.toUpperCase();
        i = three.lastIndex;
        atLineStart = false;
        continue;
      }
      if (NEWLINE_BEFORE.includes(lower) && depth === 0 && !isPartOfPhrase(two)) {
        if (out.trim()) emitNewline();
        out += tok.text.toUpperCase();
        atLineStart = false;
        continue;
      }
      if ((lower === "and" || lower === "or") && depth === 0) {
        emitNewline(1);
        out += tok.text.toUpperCase();
        atLineStart = false;
        continue;
      }
      if (!atLineStart) out += " ";
      out += RESERVED_UPPER.has(lower) ? tok.text.toUpperCase() : tok.text;
      atLineStart = false;
      continue;
    }

    // Strings, numbers, comments, operators — verbatim.
    if (!atLineStart && !/^[.)]/.test(tok.text)) out += " ";
    out += tok.text;
    atLineStart = false;
  }

  return out
    .split("\n")
    .map((l) => l.replace(/\s+$/, ""))
    .filter((l, idx, arr) => l.trim() !== "" || idx === arr.length - 1)
    .join("\n")
    .trim();
}

const RESERVED_UPPER = new Set([
  "select", "from", "where", "and", "or", "not", "in", "is", "null", "as", "on",
  "join", "left", "right", "inner", "outer", "full", "cross", "group", "by",
  "order", "having", "limit", "offset", "with", "union", "all", "distinct",
  "case", "when", "then", "else", "end", "between", "like", "ilike", "exists",
  "cast", "asc", "desc", "nulls", "first", "last", "using", "intersect",
  "except", "over", "partition", "lateral", "recursive", "filter", "window",
  "explain", "values", "set", "into", "true", "false",
]);

function isPartOfPhrase(two: string): boolean {
  // `group`/`order` alone are not clause starts — `GROUP BY` / `ORDER BY` are,
  // and those are matched by the multi-word pass.
  return two === "by";
}

function matchMultiWord(
  tokens: FormatToken[],
  i: number,
): { phrase: string; lastIndex: number } | null {
  const words: { text: string; index: number }[] = [];
  for (let j = i; j < tokens.length && words.length < 3; j += 1) {
    if (tokens[j].type === "space") continue;
    if (tokens[j].type !== "word") break;
    words.push({ text: tokens[j].text.toLowerCase(), index: j });
  }
  for (let len = Math.min(3, words.length); len >= 2; len -= 1) {
    const phrase = words.slice(0, len).map((w) => w.text).join(" ");
    if (NEWLINE_BEFORE.includes(phrase)) {
      return { phrase, lastIndex: words[len - 1].index };
    }
  }
  return null;
}

interface FormatToken {
  type: "word" | "string" | "number" | "comment" | "punct" | "op" | "space";
  text: string;
}

function tokenizeForFormat(sql: string): FormatToken[] {
  const out: FormatToken[] = [];
  let i = 0;
  while (i < sql.length) {
    const c = sql[i];
    if (/\s/.test(c)) {
      let j = i;
      while (j < sql.length && /\s/.test(sql[j])) j += 1;
      out.push({ type: "space", text: sql.slice(i, j) });
      i = j;
      continue;
    }
    if (c === "-" && sql[i + 1] === "-") {
      let j = i;
      while (j < sql.length && sql[j] !== "\n") j += 1;
      out.push({ type: "comment", text: sql.slice(i, j) });
      i = j;
      continue;
    }
    if (c === "/" && sql[i + 1] === "*") {
      let j = i + 2;
      while (j < sql.length - 1 && !(sql[j] === "*" && sql[j + 1] === "/")) j += 1;
      j = Math.min(sql.length, j + 2);
      out.push({ type: "comment", text: sql.slice(i, j) });
      i = j;
      continue;
    }
    if (c === "'" || c === '"' || c === "`") {
      const s = blankState();
      let j = i;
      do {
        j += step(sql, j, s);
      } while (j < sql.length && inAnyLiteral(s));
      out.push({ type: "string", text: sql.slice(i, j) });
      i = j;
      continue;
    }
    if (/[0-9]/.test(c)) {
      let j = i;
      while (j < sql.length && /[0-9._eE]/.test(sql[j])) j += 1;
      out.push({ type: "number", text: sql.slice(i, j) });
      i = j;
      continue;
    }
    if (/[A-Za-z_$]/.test(c)) {
      let j = i;
      while (j < sql.length && /[A-Za-z0-9_$]/.test(sql[j])) j += 1;
      out.push({ type: "word", text: sql.slice(i, j) });
      i = j;
      continue;
    }
    if (c === "(" || c === ")" || c === ",") {
      out.push({ type: "punct", text: c });
      i += 1;
      continue;
    }
    let j = i;
    while (j < sql.length && /[=<>!+\-*/%|&^~:.;[\]{}@#?]/.test(sql[j])) j += 1;
    out.push({ type: "op", text: sql.slice(i, Math.max(j, i + 1)) });
    i = Math.max(j, i + 1);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Read-only safety (mirrors the API gate so the UI fails fast)
// ---------------------------------------------------------------------------

const SAFE_STARTS = new Set([
  "SELECT", "WITH", "EXPLAIN", "SHOW", "DESCRIBE", "DESC", "ANALYZE", "PRAGMA", "VALUES",
]);

const DESTRUCTIVE = new Set([
  "INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
  "GRANT", "REVOKE", "EXEC", "EXECUTE", "MERGE", "COPY", "LOAD", "REPLACE",
  "CALL", "VACUUM", "REINDEX", "CLUSTER",
]);

/**
 * Client-side mirror of the API's read-only gate. Advisory only — the server
 * remains the authority — but it turns a round-trip rejection into an inline
 * message and names the offending statement in a multi-statement buffer.
 */
export function checkReadOnly(sql: string): { ok: boolean; reason?: string; statement?: number } {
  const statements = splitStatements(sql);
  if (statements.length === 0) return { ok: true };
  for (let idx = 0; idx < statements.length; idx += 1) {
    // Comments removed and literal bodies blanked, so neither a commented-out
    // DROP nor a string containing "DELETE" can trip the gate.
    const body = blankLiterals(stripComments(statements[idx].text)).trim();
    if (!body) continue;
    const first = body.match(/^([A-Za-z][A-Za-z0-9_]*)/)?.[1]?.toUpperCase() ?? "";
    if (!SAFE_STARTS.has(first)) {
      return {
        ok: false,
        reason: `Only read and metadata queries run here (SELECT, WITH, EXPLAIN, SHOW, DESCRIBE, ANALYZE, PRAGMA, VALUES) — found ${first || "an unrecognised statement"}.`,
        statement: idx + 1,
      };
    }
    const words = body.match(/\b[A-Za-z][A-Za-z0-9_]*\b/g) ?? [];
    const bad = words.find((w) => DESTRUCTIVE.has(w.toUpperCase()));
    if (bad) {
      return {
        ok: false,
        reason: `${bad.toUpperCase()} is a write statement and is refused here. Use Transfer Studio (Map → Validate → Execute) for anything that changes data.`,
        statement: idx + 1,
      };
    }
    if (/\bselect\b[\s\S]*\binto\b/i.test(body)) {
      return {
        ok: false,
        reason: "SELECT … INTO creates a table and is refused here.",
        statement: idx + 1,
      };
    }
  }
  return { ok: true };
}

/** Wrap a statement in the dialect's plan syntax. */
export function explainPrefix(dialect: SqlDialect, analyze = false): string {
  if (dialect === "tsql") return "";
  if (dialect === "plsql") return "EXPLAIN PLAN FOR ";
  if (dialect === "bigquery") return "";
  if (dialect === "postgresql" || dialect === "redshift") {
    return analyze ? "EXPLAIN (ANALYZE, VERBOSE, BUFFERS) " : "EXPLAIN (VERBOSE) ";
  }
  return "EXPLAIN ";
}

/** True when the dialect can produce a plan through the read-only gate. */
export function supportsExplain(dialect: SqlDialect): boolean {
  return dialect !== "tsql" && dialect !== "bigquery";
}
