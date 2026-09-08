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
  const definition = definitionPasted(stripped, dialect, opts.mode);
  if (definition) {
    return fail(definition);
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

const DDL_DEFINITION = /^\s*create\s+(?:or\s+(?:replace|alter)\s+)?(?:temp(?:orary)?\s+|secure\s+)?(procedure|proc|function|table|view)\b/i;
const TSQL_DIALECTS = new Set([
  "mssql", "sqlserver", "sybase", "azure_sql", "azure_sql_database",
  "microsoft_sql_server", "amazon_rds_sql_server", "synapse",
]);
const TSQL_MARKERS = /(?:^|[;\s])go(?:\s|$)|@[a-z_]\w*\s+(?:n?varchar|int|bigint|bit|decimal|datetime)\b|\bset\s+nocount\s+on\b|\bbegin\s+try\b|\braiserror\s*\(|\bdbo\./gi;

/** A T-SQL script pasted against a non-SQL-Server engine. */
function foreignTsql(stripped: string, dialect: string): boolean {
  if (!dialect || TSQL_DIALECTS.has(dialect)) return false;
  return (stripped.match(TSQL_MARKERS) || []).length >= 2;
}

/** A pasted `CREATE PROCEDURE …` script is the object's definition, not an extract. */
function definitionPasted(stripped: string, dialect: string, mode: SqlEditorMode): string {
  const m = stripped.match(DDL_DEFINITION);
  if (!m) return "";
  const kind = m[1].toUpperCase();
  const engine = dialect || "the source engine";
  if (kind === "PROCEDURE" || kind === "PROC" || kind === "FUNCTION") {
    const call = TSQL_DIALECTS.has(dialect) ? "EXEC" : "CALL";
    const via = mode === "query"
      ? "Paste one read-only SELECT / WITH here, or switch to Stored procedure and "
      : "Then ";
    if (foreignTsql(stripped, dialect)) {
      return `This is a SQL Server T-SQL CREATE ${kind} script (@params, GO, dbo.), not a ${engine} extract — ${engine} cannot run it as written. ${via}paste \`CALL schema.name(:param)\` for a procedure that already exists in ${engine}, with binds set below.`;
    }
    return `This is a CREATE ${kind} definition, not an extract. Create the object in ${engine} with your own client first. ${via}paste \`${call} schema.name(:param)\` for a procedure that already exists, with binds set below.`;
  }
  return `This is a CREATE ${kind} statement — DataFlow only reads here. Use Table, one read-only SELECT / WITH, or a stored procedure CALL.`;
}

function firstVerb(text: string): string {
  const m = text.match(/^(call|exec(?:ute)?|select|with|insert|merge|update|upsert|replace)/i);
  return (m?.[1] || "SQL").toUpperCase();
}
