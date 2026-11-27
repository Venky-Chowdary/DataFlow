/**
 * Connection test as a step checklist (UI pass 2, item 4).
 * Run: npx tsx --test apps/web/src/lib/connectionTestSteps.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { ConnectionTestChecklist } from "../components/connections/ConnectionTestChecklist.tsx";
import { connectionTestSteps, grantFixes, isSqlGrantEngine, schemaFromMessage } from "./connectionTestSteps.ts";

const states = (r: Parameters<typeof connectionTestSteps>[0]) => connectionTestSteps(r).map((s) => `${s.id}:${s.state}`);

describe("connectionTestSteps: where the test stopped, from the API's error code", () => {
  it("a success passes reach and sign-in and never claims catalog or grants", () => {
    assert.deepEqual(states({ success: true }), ["reach:passed", "sign_in:passed", "catalog:not_checked", "privileges:not_checked"]);
  });
  it("stops at the failing step; later steps did not run", () => {
    assert.deepEqual(states({ success: false, code: "network.connect_timeout" }), ["reach:failed", "sign_in:not_run", "catalog:not_run", "privileges:not_run"]);
    assert.deepEqual(states({ success: false, code: "network.tls_untrusted" })[0], "reach:failed");
    // Review UI2 #10: a settings or driver problem dialled nothing.
    assert.deepEqual(states({ success: false, code: "config.driver_missing" }), ["settings:failed", "reach:not_run", "sign_in:not_run", "catalog:not_run", "privileges:not_run"]);
    assert.match(connectionTestSteps({ success: false, code: "config.driver_missing" })[0].detail, /driver/);
    assert.deepEqual(states({ success: false, code: "auth.password_rejected" }), ["reach:passed", "sign_in:failed", "catalog:not_run", "privileges:not_run"]);
    assert.deepEqual(states({ success: false, code: "access.database_not_granted" }), ["reach:passed", "sign_in:passed", "catalog:failed", "privileges:not_run"]);
    assert.deepEqual(states({ success: false, code: "object.schema_not_found" })[2], "catalog:failed");
    assert.deepEqual(states({ success: false, code: "access.denied" }), ["reach:passed", "sign_in:passed", "catalog:passed", "privileges:failed"]);
  });
  it("does not guess when the API did not say", () => {
    assert.deepEqual(states({ success: false, code: "unclassified" }), ["reach:unknown", "sign_in:not_run", "catalog:not_run", "privileges:not_run"]);
  });
});

describe("grantFixes (review UI2 #5)", () => {
  it("quotes what the person typed for the engine", () => {
    const pg = grantFixes({ type: "postgresql", username: 'etl"user', database: "sales", schema: "raw" });
    assert.equal(pg.length, 2);
    assert.match(pg[0].sql, /GRANT USAGE ON SCHEMA "raw" TO "etl""user";/);
    assert.match(pg[1].sql, /GRANT USAGE, CREATE ON SCHEMA "raw" TO "etl""user";/);
    const my = grantFixes({ type: "mysql", username: "o'brien", database: "shop" });
    assert.match(my[1].sql, /ON `shop`\.\* TO 'o''brien'@'%';/);
  });
  it("Snowflake leaves plain names unquoted (they fold to upper case) and quotes only what needs it", () => {
    const sf = grantFixes({ type: "snowflake", role: "loader", database: "analytics", warehouse: "compute_wh", schema: "raw" });
    assert.match(sf[0].sql, /GRANT USAGE ON WAREHOUSE compute_wh TO ROLE loader;/);
    assert.match(sf[1].sql, /GRANT USAGE, CREATE TABLE ON SCHEMA analytics\.raw TO ROLE loader;/);
    assert.match(grantFixes({ type: "snowflake", role: "my role", database: "d", schema: "s" })[0].sql, /TO ROLE "my role";/);
  });
  it("Redshift has no CONNECT on a database", () => {
    const rs = grantFixes({ type: "redshift", username: "etl", database: "dev", schema: "raw" });
    assert.doesNotMatch(rs.map((f) => f.sql).join("\n"), /CONNECT/);
  });
  it("never defaults the schema: it comes from the form, the failure, or a placeholder", () => {
    assert.match(grantFixes({ type: "postgresql", username: "etl", database: "sales", message: "permission denied for schema raw" })[1].sql, /ON SCHEMA "raw"/);
    const unknown = grantFixes({ type: "postgresql" })[0].sql;
    assert.match(unknown, /ON SCHEMA <schema> TO <user>;/);
    assert.doesNotMatch(unknown, /public/);
    assert.doesNotMatch(grantFixes({ type: "sqlserver", username: "etl", database: "dw" })[1].sql, /dbo/);
  });
  it("the SQL Server destination set can read back what it writes", () => {
    assert.match(grantFixes({ type: "sqlserver", username: "etl", database: "dw", schema: "stage" })[1].sql, /db_datareader ADD MEMBER \[etl\];[\s\S]*GRANT ALTER ON SCHEMA::\[stage\] TO \[etl\];/);
  });
  it("applies to SQL engines only", () => {
    assert.deepEqual(grantFixes({ type: "csv" }), []);
    assert.equal(isSqlGrantEngine("csv"), false);
    assert.equal(isSqlGrantEngine("postgresql"), true);
  });
});

describe("ConnectionTestChecklist", () => {
  it("names every step's state in words and opens the grants on a permission failure", () => {
    const html = renderToStaticMarkup(React.createElement(ConnectionTestChecklist, {
      result: { success: false, code: "access.denied" },
      connection: { type: "postgresql", username: "etl", database: "sales" },
    }));
    assert.match(html, /Reach host<\/span><span class="df2-ctest-word">Passed/);
    assert.match(html, /Privileges<\/span><span class="df2-ctest-word">Failed/);
    assert.match(html, /<details class="df2-ctest-grants" open="">/);
    assert.match(html, /aria-label="Copy grants to write \(destination\)"/);
    const ok = renderToStaticMarkup(React.createElement(ConnectionTestChecklist, { result: { success: true }, connection: { type: "postgresql" } }));
    assert.match(ok, /Not checked by Test/);
    assert.match(ok, /<details class="df2-ctest-grants">/);
  });
});

describe("schemaFromMessage reads only the engine's own phrasing (re-review C)", () => {
  it("finds the schema in known error texts", () => {
    assert.equal(schemaFromMessage("permission denied for schema raw", "postgresql"), "raw");
    assert.equal(schemaFromMessage('ERROR: schema "Sales Q1" does not exist', "postgresql"), "Sales Q1");
    assert.equal(schemaFromMessage('The specified schema name "stage" either does not exist or you do not have permission to use it.', "sqlserver"), "stage");
    assert.equal(schemaFromMessage("Schema 'ANALYTICS.RAW' does not exist or not authorized.", "snowflake"), "RAW");
  });
  it("never guesses a word out of prose", () => {
    assert.equal(schemaFromMessage("no schema has been selected to create in", "postgresql"), "");
    assert.equal(schemaFromMessage('Invalid schema name "dbo" in this context', "postgresql"), "");
    assert.equal(schemaFromMessage('schema name "dbo"', "sqlserver"), "");
    assert.equal(schemaFromMessage("Schema does not exist", "snowflake"), "");
    assert.equal(schemaFromMessage("permission denied for schema raw", "mysql"), "", "only the engine that says it");
    assert.match(grantFixes({ type: "postgresql", username: "etl", database: "d", message: "no schema has been selected to create in" })[0].sql, /ON SCHEMA <schema>/);
  });
});

/** Re-review A: whatever is typed, every snippet is exactly the statements intended. */
describe("grant snippets cannot be injected (property test)", () => {
  // Per-engine tokenizers: a quoted identifier or string literal is one opaque value.
  const TOKENS: Record<string, RegExp> = {
    postgresql: /"(?:[^"]|"")*"/g,
    redshift: /"(?:[^"]|"")*"/g,
    oracle: /"(?:[^"]|"")*"/g,
    snowflake: /"(?:[^"]|"")*"/g,
    mysql: /`(?:[^`]|``)*`|'(?:[^'\\]|''|\\.)*'/g,
    sqlserver: /\[(?:[^\]]|\]\])*\]/g,
  };
  const skeleton = (engine: string, sql: string, bare: string[]) => {
    // Quoted tokens and Datawrap's own placeholders are both one opaque value.
    let out = sql.replace(TOKENS[engine], "\u0001").replace(/<[a-z]+>/g, "\u0001");
    // Snowflake leaves a plain typed identifier bare: one word of [A-Za-z0-9_$], equal to the input.
    if (engine === "snowflake") out = out.replace(/[A-Za-z_][A-Za-z0-9_$]*/g, (w) => (bare.includes(w) ? "\u0001" : w));
    return out;
  };
  const alphabet = ["a", "Z", "_", "%", "public", "\u202e", "\u200b", "\u2066", "$", " ", ";", "'", '"', "`", "[", "]", "\\", "-", "--", "/*", "*/", "\n", "\r", "\t", "\u0000",
    "<", ">", "<u>", "é", "名", "🙂", " ", "%", "@", "DROP", "ALTER ROLE etl SUPERUSER", ".", ","];
  let seed = 20261127;
  const rand = () => {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    return seed / 0x7fffffff;
  };
  const randomText = () => Array.from({ length: 1 + Math.floor(rand() * 8) }, () => alphabet[Math.floor(rand() * alphabet.length)]).join("");
  const engines = ["postgresql", "redshift", "mysql", "sqlserver", "oracle", "snowflake"];
  const plain = { username: "u", database: "d", schema: "s", warehouse: "w", role: "r" };

  it("the named injection stays inside a quoted value on every engine", () => {
    const evil = "<u>; ALTER ROLE etl SUPERUSER; --";
    for (const engine of engines) {
      const sql = grantFixes({ type: engine, username: evil, database: evil, schema: evil, warehouse: evil, role: evil }).map((f) => f.sql).join("\n");
      assert.doesNotMatch(skeleton(engine, sql, []), /ALTER ROLE etl SUPERUSER/, engine);
    }
  });

  it("for 600 random inputs per engine, the snippet has the same statements as for plain names", () => {
    for (const engine of engines) {
      const want = grantFixes({ type: engine, ...plain }).map((f) => skeleton(engine, f.sql, Object.values(plain)));
      for (let i = 0; i < 600; i += 1) {
        const input = { username: randomText(), database: randomText(), schema: randomText(), warehouse: randomText(), role: randomText() };
        grantFixes({ type: engine, ...input }).forEach((fix, j) => {
          assert.equal(fix.sql.split("\n").length, want[j].split("\n").length, `${engine} one statement per line: ${JSON.stringify(input)}`);
          assert.equal(skeleton(engine, fix.sql, Object.values(input).map((v) => v.trim())), want[j], `${engine} ${JSON.stringify(input)}`);
        });
      }
    }
  });
});

describe("review round 3 grant fixes", () => {
  /** The database name a MySQL db-level pattern matches, or null if it has a wildcard. */
  const literalDbPattern = (sql: string): string | null => {
    const m = /ON `((?:[^`]|``)*)`\.\*/.exec(sql);
    assert.ok(m, sql);
    const inner = m![1].replace(/``/g, "`");
    let out = "";
    for (let i = 0; i < inner.length; i += 1) {
      const c = inner[i];
      if (c === "\\") { out += inner[i + 1] ?? ""; i += 1; continue; }
      if (c === "_" || c === "%") return null; // an unescaped wildcard
      out += c;
    }
    return out;
  };

  it("MySQL / MariaDB: _ and % in a database name are escaped, so the grant names exactly that database", () => {
    for (const engine of ["mysql", "mariadb"]) {
      const fixes = grantFixes({ type: engine, username: "etl", database: "app_prod" });
      assert.match(fixes[1].sql, /ON `app\\_prod`\.\* TO 'etl'@'%';/);
      for (const name of ["app_prod", "50%off", "a\\_b", "x`y_z", "plain"]) {
        for (const fix of grantFixes({ type: engine, username: "etl", database: name })) {
          assert.equal(literalDbPattern(fix.sql), name, `${engine} ${name}`);
        }
      }
    }
  });

  it("PostgreSQL: an unquoted schema name is taken only when it is the whole rest of the message", () => {
    assert.equal(schemaFromMessage("permission denied for schema My Schema", "postgresql"), "");
    assert.equal(schemaFromMessage("permission denied for schema raw", "postgresql"), "raw");
    assert.equal(schemaFromMessage('schema "a""b" does not exist', "postgresql"), 'a""b', "PostgreSQL does not double quotes: no decoding");
    assert.equal(schemaFromMessage('The specified schema name "x""y" either does not exist or you do not have permission to use it.', "sqlserver"), 'x""y');
    assert.match(grantFixes({ type: "postgresql", username: "etl", database: "d", message: "permission denied for schema My Schema" })[0].sql, /SCHEMA <schema>/);
  });

  it("Oracle: a value with a double quote cannot be quoted there, so it is the placeholder", () => {
    const sql = grantFixes({ type: "oracle", username: 'a"b', schema: 'o"w' }).map((f) => f.sql).join("\n");
    assert.doesNotMatch(sql, /a""b|o""w/);
    assert.match(sql, /TO <user>;/);
    assert.match(sql, /ON <owner>\.<table>/);
  });

  it("Unicode format characters (bidi overrides, zero-width, isolates) become the placeholder", () => {
    for (const ch of ["‮", "​", "⁦", "⁩", "­", "﻿"]) {
      const sql = grantFixes({ type: "postgresql", username: `etl${ch}admin`, database: "d", schema: "s" })[0].sql;
      assert.match(sql, /TO <user>;/, JSON.stringify(ch));
    }
  });

  it("PostgreSQL / Redshift: a user typed as PUBLIC (any case, quoted or not) is never filled in", () => {
    for (const engine of ["postgresql", "redshift"]) {
      for (const user of ["public", "PUBLIC", '"public"', "Public"]) {
        const fixes = grantFixes({ type: engine, username: user, database: "d", schema: "s" });
        assert.match(fixes[0].sql, /TO <user>;/, `${engine} ${user}`);
        assert.match(fixes[0].note || "", /PUBLIC means every role/);
      }
    }
    assert.equal(grantFixes({ type: "postgresql", username: "publisher", database: "d", schema: "s" })[0].note, undefined);
  });
});
