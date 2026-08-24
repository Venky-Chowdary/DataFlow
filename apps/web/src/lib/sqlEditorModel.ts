/**
 * Studio / schedule extract diagnosis — query vs stored-procedure.
 *
 * Highlight and bind extraction live in queryHighlight / sqlIntel (Query
 * Playground SSOT). This module only adds transfer-extract rules the
 * playground does not own: one statement, CALL/EXEC vs SELECT, missing binds.
 */

import { checkReadOnly, extractBindParams, stripComments } from "./sqlIntel";

export type SqlEditorMode = "query" | "procedure" | "dest_dml";

export interface SqlDiagnosis {
  ok: boolean;
  mode: SqlEditorMode;
  dialect: string;
  statement: string;
  binds: string[];
  error: string;
}

const DENIED = /\b(insert|update|delete|drop|create|alter|truncate|grant|revoke|merge|copy|load|replace|openrowset|opendatasource|openquery|bulk|shutdown|dbcc|xp_cmdshell|into\s+outfile|into\s+dumpfile|execute\s+immediate|sp_executesql)\b/i;

const PG_FN_DIALECTS = new Set(["postgresql", "postgres", "pgvector", "redshift"]);

export function diagnoseSql(
  text: string,
  opts: { mode: SqlEditorMode; dialect?: string; bound?: Record<string, string> },
): SqlDiagnosis {
  const dialect = String(opts.dialect || "").toLowerCase();
  const raw = String(text || "");
  const binds = extractBindParams(raw);
  const stripped = stripComments(raw).replace(/\s+/g, " ").trim();
  const bound = opts.bound || {};

  const fail = (error: string, statement = ""): SqlDiagnosis => ({
    ok: false,
    mode: opts.mode,
    dialect,
    statement,
    binds,
    error,
  });

  if (!stripped) {
    return fail(
      opts.mode === "query"
        ? "Paste one read-only SELECT / WITH."
        : opts.mode === "dest_dml"
          ? "Paste one INSERT / MERGE / UPDATE with :binds."
          : "Paste one CALL / EXEC, or a set-returning function.",
    );
  }
  if (stripped.includes(";") && stripped.replace(/;+\s*$/, "").includes(";")) {
    return fail("Only one statement is allowed — remove extra semicolons.");
  }

  const verb = firstVerb(stripped);
  const isCall = /^(call|exec(?:ute)?)$/i.test(verb);

  if (opts.mode === "dest_dml") {
    if (isCall) {
      return fail("CALL belongs in Stored procedure — dest query is INSERT/MERGE/UPDATE.");
    }
    if (!/^(insert|merge|update|upsert|replace)$/i.test(verb)) {
      return fail("Destination query allows INSERT, MERGE, UPDATE, UPSERT, or REPLACE.");
    }
    if (/\b(drop|create|alter|truncate|grant|revoke|delete)\b/i.test(stripped)) {
      return fail("Destination query refuses DELETE, DDL, and admin tokens.");
    }
  } else if (opts.mode === "query") {
    if (isCall) {
      return fail("Query source allows one read-only SELECT/WITH — CALL belongs in Stored procedure.");
    }
    const ro = checkReadOnly(raw);
    if (!ro.ok) {
      return fail(ro.reason || "This statement is not a read-only extract.");
    }
    if (DENIED.test(stripped)) {
      return fail("This statement is not an extract — DDL, DML, and admin calls are blocked.");
    }
  } else {
    if (DENIED.test(stripped) && !isCall) {
      return fail("This statement is not an extract — DDL, DML, and admin calls are blocked.");
    }
    const bareIdent = /^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2}$/.test(
      stripped.replace(/;+$/, ""),
    );
    if (
      !bareIdent
      && /^(select|with)$/i.test(verb)
      && !/select\s+\*\s+from\s+[A-Za-z_][A-Za-z0-9_.]*\s*\(/i.test(stripped)
    ) {
      if (!PG_FN_DIALECTS.has(dialect) || !/select\s+\*\s+from\s+/i.test(stripped)) {
        return fail("Stored procedure mode wants CALL / EXEC (PostgreSQL may use SELECT * FROM fn()).");
      }
    }
  }

  const missing = binds.filter((n) => bound[n] == null || String(bound[n]).trim() === "");
  if (missing.length) {
    return fail(`Bind :${missing.join(", :")} is not set.`, verb);
  }
  return {
    ok: true,
    mode: opts.mode,
    dialect,
    statement: verb,
    binds,
    error: "",
  };
}

function firstVerb(text: string): string {
  const m = text.match(/^(call|exec(?:ute)?|select|with|insert|merge|update|upsert|replace)/i);
  return (m?.[1] || "SQL").toUpperCase();
}
