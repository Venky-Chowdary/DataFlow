/**
 * Run: npx --yes tsx --test apps/web/src/lib/sqlIntel.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  analyzeContext,
  buildCompletions,
  checkReadOnly,
  dialectForConnector,
  explainPrefix,
  extractBindParams,
  extractTableRefs,
  formatSql,
  limitSyntax,
  resolveRunTarget,
  scoreMatch,
  splitStatements,
  statementAtCursor,
  stripComments,
  supportsExplain,
  type SchemaObject,
} from "./sqlIntel.js";

const SCHEMA: SchemaObject[] = [
  {
    name: "users",
    type: "table",
    schema: "public",
    rowEstimate: 1200,
    columns: [
      { name: "id", type: "BIGINT", primaryKey: true, nullable: false },
      { name: "email", type: "TEXT", nullable: false },
      { name: "created_at", type: "TIMESTAMPTZ", nullable: true },
    ],
  },
  {
    name: "orders",
    type: "table",
    schema: "public",
    columns: [
      { name: "id", type: "BIGINT", primaryKey: true },
      { name: "user_id", type: "BIGINT" },
      { name: "amount", type: "NUMERIC(12,2)" },
    ],
  },
];

describe("splitStatements", () => {
  it("splits on top-level semicolons and records offsets", () => {
    const sql = "SELECT 1;\nSELECT 2;";
    const parts = splitStatements(sql);
    assert.deepEqual(parts.map((p) => p.text), ["SELECT 1", "SELECT 2"]);
    assert.equal(sql.slice(parts[1].start, parts[1].end), "SELECT 2");
    assert.equal(parts[1].line, 2);
  });

  it("ignores semicolons inside single-quoted literals", () => {
    const parts = splitStatements("SELECT ';' AS x, 'a;b' AS y");
    assert.equal(parts.length, 1);
  });

  it("handles doubled-quote escapes", () => {
    const parts = splitStatements("SELECT 'it''s; fine' AS x");
    assert.equal(parts.length, 1);
    assert.equal(parts[0].text, "SELECT 'it''s; fine' AS x");
  });

  it("ignores semicolons in line and block comments", () => {
    const parts = splitStatements("SELECT 1 -- ;nope\n; /* also ; here */ SELECT 2");
    assert.equal(parts.length, 2);
    assert.match(parts[0].text, /^SELECT 1/);
    // The `;` inside the block comment must not split statement 2.
    assert.match(parts[1].text, /SELECT 2$/);
  });

  it("keeps dollar-quoted bodies intact", () => {
    const sql = "SELECT $$ begin; end; $$ AS body";
    assert.equal(splitStatements(sql).length, 1);
  });

  it("keeps tagged dollar-quoted bodies intact", () => {
    const sql = "SELECT $tag$ a; b; $tag$ AS body";
    assert.equal(splitStatements(sql).length, 1);
  });

  it("drops comment-only and empty segments", () => {
    const parts = splitStatements("-- just a note\n;;\nSELECT 1;");
    assert.deepEqual(parts.map((p) => p.text), ["SELECT 1"]);
  });

  it("respects quoted identifiers containing a semicolon", () => {
    const parts = splitStatements('SELECT "we;ird" FROM t');
    assert.equal(parts.length, 1);
  });

  it("respects backtick identifiers containing a semicolon", () => {
    const parts = splitStatements("SELECT `we;ird` FROM t");
    assert.equal(parts.length, 1);
  });
});

describe("stripComments", () => {
  it("removes comments but preserves literals", () => {
    const out = stripComments("SELECT 'a -- b' /* c */ , x -- tail\n, y");
    assert.match(out, /'a -- b'/);
    assert.doesNotMatch(out, /\/\* c \*\//);
    assert.doesNotMatch(out, /tail/);
    assert.match(out, /, y/);
  });

  it("does not fuse tokens across a removed comment", () => {
    assert.match(stripComments("a/*x*/b"), /a\s+b/);
  });
});

describe("statementAtCursor", () => {
  const sql = "SELECT 1;\nSELECT 2;\nSELECT 3";

  it("returns the statement containing the cursor", () => {
    assert.equal(statementAtCursor(sql, 2)?.text, "SELECT 1");
    assert.equal(statementAtCursor(sql, 12)?.text, "SELECT 2");
    assert.equal(statementAtCursor(sql, sql.length)?.text, "SELECT 3");
  });

  it("falls back to the nearest preceding statement on a delimiter", () => {
    assert.equal(statementAtCursor("SELECT 1;   ", 11)?.text, "SELECT 1");
  });

  it("returns null for an empty buffer", () => {
    assert.equal(statementAtCursor("   ", 1), null);
  });
});

describe("resolveRunTarget", () => {
  const sql = "SELECT 1;\nSELECT 2;";

  it("prefers a non-empty selection", () => {
    const t = resolveRunTarget(sql, 0, 8);
    assert.equal(t.scope, "selection");
    assert.equal(t.text, "SELECT 1");
  });

  it("falls back to the statement under the cursor", () => {
    const t = resolveRunTarget(sql, 12, 12);
    assert.equal(t.scope, "statement");
    assert.equal(t.text, "SELECT 2");
  });

  it("treats a whitespace-only selection as no selection", () => {
    const t = resolveRunTarget(sql, 9, 10);
    assert.equal(t.scope, "statement");
  });
});

describe("extractBindParams", () => {
  it("collects named parameters in first-appearance order", () => {
    assert.deepEqual(
      extractBindParams("SELECT * FROM t WHERE a = :region AND b < :max AND c = :region"),
      ["region", "max"],
    );
  });

  it("ignores PostgreSQL :: casts", () => {
    assert.deepEqual(extractBindParams("SELECT x::text, y::numeric FROM t"), []);
  });

  it("ignores parameters inside literals and comments", () => {
    assert.deepEqual(extractBindParams("SELECT ':notparam' -- :alsonot\nFROM t WHERE a = :real"), ["real"]);
  });

  it("handles a cast next to a real parameter", () => {
    assert.deepEqual(extractBindParams("WHERE created_at > :since::timestamptz"), ["since"]);
  });

  it("returns an empty list when there are none", () => {
    assert.deepEqual(extractBindParams("SELECT 1"), []);
  });

  it("collects T-SQL @binds and ignores @@globals", () => {
    assert.deepEqual(
      extractBindParams("EXEC get_orders @since, @@ROWCOUNT"),
      ["since"],
    );
  });
});

describe("extractTableRefs", () => {
  it("reads tables and aliases from FROM and JOIN", () => {
    const { tables, aliases } = extractTableRefs(
      "SELECT * FROM users u LEFT JOIN orders AS o ON o.user_id = u.id",
    );
    assert.deepEqual(tables, ["users", "orders"]);
    assert.deepEqual(aliases, { u: "users", o: "orders" });
  });

  it("does not treat a following keyword as an alias", () => {
    const { aliases } = extractTableRefs("SELECT * FROM users WHERE id = 1");
    assert.deepEqual(aliases, {});
  });

  it("keeps schema-qualified names and unquotes every part", () => {
    const { tables, aliases } = extractTableRefs('SELECT * FROM "public"."users" AS "u"');
    assert.deepEqual(tables, ["public.users"]);
    assert.deepEqual(aliases, { u: "public.users" });
  });

  it("handles backtick and bracket quoting", () => {
    assert.deepEqual(extractTableRefs("SELECT * FROM `app`.`users` u").tables, ["app.users"]);
    assert.deepEqual(extractTableRefs("SELECT * FROM [dbo].[Users] u").tables, ["dbo.Users"]);
  });

  it("resolves a schema-qualified reference against an unqualified catalog entry", () => {
    const sql = "SELECT p. FROM public.users p";
    const items = buildCompletions(analyzeContext(sql, 9), SCHEMA, "postgresql");
    assert.deepEqual(items.map((i) => i.label).sort(), ["created_at", "email", "id"]);
  });
});

describe("analyzeContext", () => {
  it("detects the FROM clause", () => {
    const sql = "SELECT * FROM ";
    assert.equal(analyzeContext(sql, sql.length).clause, "from");
  });

  it("detects WHERE after a FROM clause", () => {
    const sql = "SELECT * FROM users WHERE ";
    assert.equal(analyzeContext(sql, sql.length).clause, "where");
  });

  it("detects a JOIN clause", () => {
    const sql = "SELECT * FROM users u LEFT JOIN ";
    assert.equal(analyzeContext(sql, sql.length).clause, "join");
  });

  it("splits a qualifier from the prefix", () => {
    const sql = "SELECT u.em FROM users u";
    const ctx = analyzeContext(sql, 11);
    assert.equal(ctx.qualifier, "u");
    assert.equal(ctx.prefix, "em");
    assert.equal(ctx.replaceFrom, 9);
  });

  it("has an empty prefix when the cursor follows whitespace", () => {
    // Regression: slicing the trimmed statement text made `FROM ` report a
    // prefix of "FROM", so every suggestion was filtered against a keyword.
    for (const sql of ["SELECT * FROM ", "SELECT * FROM orders WHERE "]) {
      assert.equal(analyzeContext(sql, sql.length).prefix, "", sql);
    }
  });

  it("stays in the FROM clause while a table name is being typed", () => {
    const sql = "SELECT * FROM us";
    const ctx = analyzeContext(sql, sql.length);
    assert.equal(ctx.clause, "from");
    assert.equal(ctx.prefix, "us");
  });

  it("flags a cursor inside a string literal", () => {
    const sql = "SELECT * FROM users WHERE email = 'abc";
    assert.equal(analyzeContext(sql, sql.length).inLiteral, true);
  });

  it("does not flag a cursor after a closed literal", () => {
    const sql = "SELECT * FROM users WHERE email = 'abc' AND ";
    assert.equal(analyzeContext(sql, sql.length).inLiteral, false);
  });

  it("scopes to the statement under the cursor", () => {
    const sql = "SELECT * FROM users;\nSELECT * FROM orders WHERE ";
    const ctx = analyzeContext(sql, sql.length);
    assert.deepEqual(ctx.tables, ["orders"]);
  });
});

describe("buildCompletions", () => {
  it("suggests tables after FROM", () => {
    const sql = "SELECT * FROM ";
    const items = buildCompletions(analyzeContext(sql, sql.length), SCHEMA, "postgresql");
    assert.equal(items[0].kind, "table");
    assert.ok(items.some((i) => i.label === "users"));
    assert.ok(items.some((i) => i.label === "orders"));
  });

  it("suggests only the aliased table's columns after a qualifier", () => {
    const sql = "SELECT u. FROM users u JOIN orders o ON o.id = u.id";
    const items = buildCompletions(analyzeContext(sql, 9), SCHEMA, "postgresql");
    const labels = items.map((i) => i.label);
    assert.deepEqual(labels.sort(), ["created_at", "email", "id"]);
    assert.ok(!labels.includes("amount"));
  });

  it("restricts WHERE columns to in-scope tables", () => {
    const sql = "SELECT * FROM orders WHERE ";
    const items = buildCompletions(analyzeContext(sql, sql.length), SCHEMA, "postgresql");
    const cols = items.filter((i) => i.kind === "column").map((i) => i.label);
    assert.ok(cols.includes("amount"));
    assert.ok(!cols.includes("email"));
  });

  it("annotates columns with type, PK and nullability", () => {
    const sql = "SELECT  FROM users";
    const items = buildCompletions(analyzeContext(sql, 7), SCHEMA, "postgresql");
    const id = items.find((i) => i.label === "id" && i.kind === "column");
    assert.ok(id);
    assert.match(id.detail ?? "", /BIGINT/);
    assert.match(id.detail ?? "", /PK/);
  });

  it("returns nothing inside a string literal", () => {
    const sql = "SELECT * FROM users WHERE email = 'ab";
    assert.deepEqual(buildCompletions(analyzeContext(sql, sql.length), SCHEMA), []);
  });

  it("filters by the typed prefix", () => {
    const sql = "SELECT * FROM us";
    const items = buildCompletions(analyzeContext(sql, sql.length), SCHEMA, "postgresql");
    assert.equal(items[0].label, "users");
  });

  it("offers dialect-specific functions", () => {
    const sql = "SELECT PARSE FROM users";
    const items = buildCompletions(analyzeContext(sql, 12), SCHEMA, "snowflake");
    assert.ok(items.some((i) => i.label === "PARSE_JSON" && i.kind === "function"));
  });

  it("does not offer another dialect's functions", () => {
    const sql = "SELECT PARSE FROM users";
    const items = buildCompletions(analyzeContext(sql, 12), SCHEMA, "postgresql");
    assert.ok(!items.some((i) => i.label === "PARSE_JSON"));
  });

  it("exposes aliases as candidates", () => {
    const sql = "SELECT  FROM users u";
    const items = buildCompletions(analyzeContext(sql, 7), SCHEMA, "postgresql");
    assert.ok(items.some((i) => i.kind === "alias" && i.label === "u"));
  });
});

describe("scoreMatch", () => {
  it("ranks exact above prefix above substring", () => {
    assert.ok(scoreMatch("id", "id") > scoreMatch("identity", "id"));
    assert.ok(scoreMatch("identity", "id") > scoreMatch("valid_at", "id"));
  });

  it("matches on a word boundary", () => {
    assert.ok(scoreMatch("user_id", "id") > scoreMatch("valid", "id"));
  });

  it("supports subsequence matching", () => {
    assert.ok(scoreMatch("created_at", "cat") > 0);
  });

  it("rejects a non-match", () => {
    assert.equal(scoreMatch("email", "zzz"), -1);
  });

  it("treats an empty query as a match", () => {
    assert.ok(scoreMatch("anything", "") > 0);
  });
});

describe("checkReadOnly", () => {
  it("allows read and metadata statements", () => {
    for (const q of ["SELECT 1", "WITH a AS (SELECT 1) SELECT * FROM a", "EXPLAIN SELECT 1", "SHOW TABLES"]) {
      assert.equal(checkReadOnly(q).ok, true, q);
    }
  });

  it("refuses writes and names the statement", () => {
    const r = checkReadOnly("SELECT 1;\nDELETE FROM users");
    assert.equal(r.ok, false);
    assert.equal(r.statement, 2);
    assert.match(r.reason ?? "", /DELETE/);
  });

  it("refuses SELECT INTO", () => {
    assert.equal(checkReadOnly("SELECT * INTO backup FROM users").ok, false);
  });

  it("is not fooled by a write keyword inside a literal", () => {
    assert.equal(checkReadOnly("SELECT 'DELETE' AS label").ok, true);
  });

  it("ignores a write keyword inside a comment", () => {
    assert.equal(checkReadOnly("SELECT 1 -- DROP TABLE users").ok, true);
  });

  it("allows an empty buffer", () => {
    assert.equal(checkReadOnly("   ").ok, true);
  });
});

describe("formatSql", () => {
  it("puts major clauses on their own line", () => {
    const out = formatSql("select id, email from users where id = 1 order by id desc");
    const lines = out.split("\n");
    assert.match(lines[0], /^SELECT/);
    assert.ok(lines.some((l) => /^FROM/.test(l)));
    assert.ok(lines.some((l) => /^WHERE/.test(l)));
    assert.ok(lines.some((l) => /^ORDER BY/.test(l)));
  });

  it("keeps GROUP BY as one clause", () => {
    const out = formatSql("select a, count(*) from t group by a");
    assert.ok(out.split("\n").some((l) => /^GROUP BY/.test(l)));
  });

  it("indents AND onto its own line", () => {
    const out = formatSql("select 1 from t where a = 1 and b = 2");
    assert.match(out, /\n\s+AND b = 2/);
  });

  it("preserves string literals verbatim", () => {
    const out = formatSql("select 'Keep from Me' from t");
    assert.match(out, /'Keep from Me'/);
  });

  it("preserves comments", () => {
    assert.match(formatSql("select 1 -- keep me\nfrom t"), /-- keep me/);
  });

  it("returns whitespace input unchanged", () => {
    assert.equal(formatSql("   "), "   ");
  });

  it("is idempotent", () => {
    const once = formatSql("select id from users where id = 1");
    assert.equal(formatSql(once), once);
  });
});

describe("dialect helpers", () => {
  it("maps connector types onto dialects", () => {
    assert.equal(dialectForConnector("postgresql"), "postgresql");
    assert.equal(dialectForConnector("SQL Server"), "tsql");
    assert.equal(dialectForConnector("oracle"), "plsql");
    assert.equal(dialectForConnector("mariadb"), "mysql");
    assert.equal(dialectForConnector("something-new"), "sql");
    assert.equal(dialectForConnector(undefined), "sql");
  });

  it("uses the dialect's row-limit syntax", () => {
    assert.equal(limitSyntax("postgresql", 10), "LIMIT 10");
    assert.equal(limitSyntax("tsql", 10), "TOP 10");
    assert.equal(limitSyntax("plsql", 10), "FETCH FIRST 10 ROWS ONLY");
  });

  it("only offers EXPLAIN where it survives the read-only gate", () => {
    assert.equal(supportsExplain("postgresql"), true);
    assert.equal(supportsExplain("tsql"), false);
    assert.equal(supportsExplain("bigquery"), false);
    assert.match(explainPrefix("postgresql"), /^EXPLAIN \(VERBOSE\)/);
    assert.match(explainPrefix("postgresql", true), /ANALYZE/);
  });
});
