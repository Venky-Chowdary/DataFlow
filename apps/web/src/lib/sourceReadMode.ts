/** Source extract mode — table, read-only query, or stored procedure CALL/EXEC. */

export type SourceReadMode = "table" | "query" | "procedure";

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$/;

const PROCEDURE_DIALECTS = new Set([
  "postgresql",
  "postgres",
  "pgvector",
  "redshift",
  "mysql",
  "mariadb",
  "sqlserver",
  "mssql",
  "oracle",
  "snowflake",
  "generic_sql",
]);

export function dialectOffersProcedures(driver: string | undefined | null): boolean {
  const d = String(driver || "").toLowerCase();
  if (!d) return false;
  if (d === "mongodb" || d === "sqlite" || d === "duckdb" || d === "dynamodb") return false;
  return PROCEDURE_DIALECTS.has(d) || d.includes("sql");
}

/** Read-only SELECT extract — SQLite/DuckDB have no stored procedures. */
export function dialectOffersQuery(driver: string | undefined | null): boolean {
  const d = String(driver || "").toLowerCase();
  if (!d) return false;
  if (d === "mongodb" || d === "dynamodb") return false;
  return dialectOffersProcedures(d) || d === "sqlite" || d === "duckdb";
}

export function dialectOffersSqlExtract(driver: string | undefined | null): boolean {
  return dialectOffersProcedures(driver) || dialectOffersQuery(driver);
}

/** Dest table SQL write — INSERT/MERGE — including SQLite (no stored procs). */
export function dialectOffersDestQuery(driver: string | undefined | null): boolean {
  return dialectOffersQuery(driver);
}

export function destQueryHint(driver: string | undefined | null): string {
  const d = String(driver || "").toLowerCase();
  if (d === "sqlserver" || d === "mssql") {
    return "INSERT INTO dbo.orders (id, amt) VALUES (@id, @amt) — one statement, bound params only.";
  }
  if (d === "snowflake") {
    return "MERGE INTO dest.orders t USING (SELECT :id id, :amt amt) s ON t.id = s.id …";
  }
  return "INSERT INTO schema.orders (id, amt) VALUES (:id, :amt) — one INSERT/MERGE/UPDATE. CALL belongs in Stored procedure.";
}

export function bindNamesFromSql(text: string): string[] {
  const names: string[] = [];
  const seen = new Set<string>();
  const re = /(?<!:):([A-Za-z_][A-Za-z0-9_]*)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(String(text || ""))) !== null) {
    const name = m[1];
    if (name && !seen.has(name)) {
      seen.add(name);
      names.push(name);
    }
  }
  return names;
}

export function queryHint(driver: string | undefined | null): string {
  const d = String(driver || "").toLowerCase();
  if (d === "sqlite" || d === "duckdb") {
    return "SELECT id, email FROM customers — one read-only statement.";
  }
  return "SELECT … FROM schema.table WHERE … — one read-only SELECT/WITH. CALL belongs in Stored procedure.";
}

export function procedureStreamName(callText: string): string {
  const raw = String(callText || "").trim();
  if (!raw) return "procedure_result";
  const bare = raw.replace(/;+\s*$/, "").trim();
  if (IDENT.test(bare)) return bare.split(".").pop() || "procedure_result";
  const named = bare.match(
    /\b(?:CALL|EXEC(?:UTE)?|FROM)\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2})/i,
  );
  if (named?.[1]) return named[1].split(".").pop() || "procedure_result";
  return "procedure_result";
}

export function procedureHint(driver: string | undefined | null): string {
  const d = String(driver || "").toLowerCase();
  if (d === "sqlserver" || d === "mssql") {
    return "EXEC dbo.GetOrders @since = '2024-01-01' — one statement, bound literals only.";
  }
  if (d === "postgresql" || d === "postgres" || d === "pgvector" || d === "redshift") {
    return "SELECT * FROM public.get_orders('2024-01-01') or CALL public.refresh_orders().";
  }
  if (d === "mysql" || d === "mariadb") {
    return "CALL get_orders('2024-01-01') — one CALL, no stacked statements.";
  }
  return "CALL schema.name(...) or EXEC schema.name — one statement. Result columns map on the next step.";
}

export function isCallableSourceMode(mode: SourceReadMode | string | undefined): boolean {
  return mode === "procedure" || mode === "query";
}

export type SourceExtractReadyInput = {
  sourceKind: string;
  parsed?: boolean;
  sourceConnectorId?: string;
  cloudPath?: string;
  sourceTable?: string;
  sourceCollection?: string;
  sourceReadMode?: SourceReadMode | string;
  procedureCall?: string;
};

/**
 * Whether Source has the extract the operator must name before Destination.
 * Query / procedure ready is the SQL text — not a table/collection name.
 */
export function sourceExtractReady(input: SourceExtractReadyInput): boolean {
  const kind = String(input.sourceKind || "");
  if (kind === "file") return Boolean(input.parsed);
  if (!String(input.sourceConnectorId || "").trim()) return false;
  if (kind === "cloud") return Boolean(String(input.cloudPath || "").trim());
  if (isCallableSourceMode(input.sourceReadMode)) {
    return Boolean(String(input.procedureCall || "").trim());
  }
  return Boolean(
    String(input.sourceTable || "").trim() || String(input.sourceCollection || "").trim(),
  );
}

/** Fields Execute must send in source_extra — Validate already stamps these. */
export function callableSourceExtra(
  mode: SourceReadMode | string | undefined,
  sql: string,
  params?: Record<string, string>,
): Record<string, unknown> | undefined {
  if (!isCallableSourceMode(mode)) return undefined;
  const extra: Record<string, unknown> = { source_read_mode: mode };
  const text = String(sql || "").trim();
  if (mode === "procedure") extra.procedure_call = text;
  if (mode === "query") extra.source_query = text;
  if (params && Object.keys(params).length) extra.procedure_params = { ...params };
  return extra;
}
