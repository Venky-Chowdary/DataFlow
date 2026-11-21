/**
 * Connection test as a step checklist (UI pass 2, item 4).
 *
 * Test (POST /connectors/test) dials the system and signs in; a failure
 * carries the API's error code (services/error_taxonomy: network.*, auth.*,
 * access.*, object.*, config.*). The code says where the test stopped, so the
 * steps before it passed and the steps after it did not run. Test does not
 * read the catalog or check grants: those steps say where they are checked
 * (picking a table, Validate's G2), never "passed".
 *
 * Grant statements are what a DBA runs to give this login the access a
 * transfer needs; identifiers the person typed are quoted for the engine.
 */
import { resolveDriverType } from "./connectorTypes";

export type TestStepState = "passed" | "failed" | "not_run" | "not_checked" | "unknown";

export interface TestStep {
  id: "settings" | "reach" | "sign_in" | "catalog" | "privileges";
  label: string;
  state: TestStepState;
  /** One plain sentence. */
  detail: string;
}

const LABELS: Record<TestStep["id"], string> = {
  settings: "Connection settings",
  reach: "Reach host",
  sign_in: "Sign in",
  catalog: "Read catalog",
  privileges: "Privileges",
};

const NOT_CHECKED: Record<"catalog" | "privileges", string> = {
  catalog: "Test does not list tables. They are read when you pick a table.",
  privileges: "Test does not check grants. Validate checks read and write access before a run.",
};

function step(id: TestStep["id"], state: TestStepState, detail: string): TestStep {
  return { id, label: LABELS[id], state, detail };
}

/** Which step a failure code stops at. */
function failedStep(code: string): TestStep["id"] | null {
  if (/^network\./.test(code)) return "reach";
  if (/^auth\./.test(code)) return "sign_in";
  if (code === "access.database_not_granted" || /^object\.(database|schema)_not_found$/.test(code)) return "catalog";
  if (/^access\./.test(code)) return "privileges";
  return null;
}

export function connectionTestSteps(result: { success: boolean; code?: string | null; message?: string }): TestStep[] {
  if (result.success) {
    return [
      step("reach", "passed", "The host answered."),
      step("sign_in", "passed", "The login was accepted."),
      step("catalog", "not_checked", NOT_CHECKED.catalog),
      step("privileges", "not_checked", NOT_CHECKED.privileges),
    ];
  }
  const code = String(result.code || "");
  if (/^config\./.test(code)) {
    // Nothing was dialled: the settings (or this server's driver) stopped the test first.
    return [
      step("settings", "failed", code === "config.driver_missing"
        ? "This server does not have the driver for this database installed."
        : "The connection settings are incomplete or invalid."),
      step("reach", "not_run", "Not run."),
      step("sign_in", "not_run", "Not run."),
      step("catalog", "not_run", "Not run."),
      step("privileges", "not_run", "Not run."),
    ];
  }
  const stop = failedStep(code);
  if (!stop) {
    // The API did not say where it stopped: say so rather than guess.
    return [
      step("reach", "unknown", "The test failed before it could say which step: see the message below."),
      step("sign_in", "not_run", "Not run."),
      step("catalog", "not_run", "Not run."),
      step("privileges", "not_run", "Not run."),
    ];
  }
  const order: TestStep["id"][] = ["reach", "sign_in", "catalog", "privileges"];
  const at = order.indexOf(stop);
  const passedDetail: Record<TestStep["id"], string> = {
    settings: "",
    reach: "The host answered.",
    sign_in: "The login was accepted.",
    catalog: "The database is visible to this login.",
    privileges: "",
  };
  const failedDetail: Record<TestStep["id"], string> = {
    settings: "",
    reach: "The host could not be reached or refused the connection.",
    sign_in: "The host refused this login.",
    catalog: "This login cannot see the database or schema.",
    privileges: "This login lacks a permission the transfer needs.",
  };
  return order.map((id, i) => {
    if (i < at) {
      // A grant failure means the catalog was read; anything else before the stop passed.
      return step(id, "passed", passedDetail[id]);
    }
    if (i === at) return step(id, "failed", failedDetail[id]);
    return step(id, "not_run", "Not run.");
  });
}

export interface GrantFix {
  engine: string;
  /** "To read (source)" / "To write (destination)". */
  purpose: string;
  sql: string;
}

/** Engines whose access is granted in SQL: the step checklist and grants apply to these only (review UI2 #10). */
export const SQL_GRANT_ENGINES = new Set(["postgresql", "redshift", "mysql", "mariadb", "sqlserver", "oracle", "snowflake"]);

export function isSqlGrantEngine(type: string): boolean {
  return SQL_GRANT_ENGINES.has(resolveDriverType(type));
}

const filled = (value: string | undefined, fallback: string) => (value && value.trim() ? value.trim() : fallback);

function pgIdent(name: string): string {
  return name.startsWith("<") ? name : `"${name.replace(/"/g, '""')}"`;
}
function myIdent(name: string): string {
  return name.startsWith("<") ? name : `\`${name.replace(/`/g, "``")}\``;
}
function myString(value: string): string {
  return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "''")}'`;
}
function msIdent(name: string): string {
  return name.startsWith("<") ? name : `[${name.replace(/]/g, "]]")}]`;
}
/**
 * Snowflake folds an unquoted identifier to upper case and treats a quoted
 * one as case-sensitive (https://docs.snowflake.com/en/sql-reference/identifiers-syntax).
 * A plain name someone typed (compute_wh) is left unquoted so it resolves to
 * the object they mean (COMPUTE_WH); only a name that needs quotes gets them.
 */
function sfIdent(name: string): string {
  if (name.startsWith("<")) return name;
  return /^[A-Za-z_][A-Za-z0-9_$]*$/.test(name) ? name : `"${name.replace(/"/g, '""')}"`;
}

/** The schema the failure named ("permission denied for schema raw"), if any. */
export function schemaFromMessage(message: string | undefined): string {
  const m = /\bschema\s+["`[]?([A-Za-z_][\w$]*)["`\]]?/i.exec(String(message || ""));
  return m ? m[1] : "";
}

/**
 * The grants a transfer needs on this engine, with what the person typed
 * filled in. A value the person did not give is a <placeholder>, never a
 * silent default (review UI2 #5); the schema comes from the form or from the
 * failure message. Statements checked against each vendor's GRANT docs:
 * - PostgreSQL: https://www.postgresql.org/docs/current/sql-grant.html
 * - Amazon Redshift (no CONNECT privilege for database users):
 *   https://docs.aws.amazon.com/redshift/latest/dg/r_GRANT.html
 * - MySQL / MariaDB: https://dev.mysql.com/doc/refman/8.4/en/grant.html,
 *   https://mariadb.com/kb/en/grant/
 * - SQL Server: https://learn.microsoft.com/sql/relational-databases/security/authentication-access/database-level-roles,
 *   https://learn.microsoft.com/sql/t-sql/statements/grant-database-permissions-transact-sql
 * - Oracle: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/GRANT.html
 * - Snowflake: https://docs.snowflake.com/en/sql-reference/sql/grant-privilege
 * The destination sets include SELECT: Datawrap reads the destination back
 * (Gate-8 read-back, existing-table checks).
 */
export function grantFixes(input: {
  type: string; username?: string; database?: string; schema?: string; warehouse?: string; role?: string; message?: string;
}): GrantFix[] {
  const driver = resolveDriverType(input.type);
  const user = filled(input.username, "<user>");
  const db = filled(input.database, "<database>");
  const schema = filled(input.schema, schemaFromMessage(input.message) || "<schema>");
  switch (driver) {
    case "postgresql": {
      const u = pgIdent(user);
      const s = pgIdent(schema);
      return [
        { engine: driver, purpose: "To read (source)", sql: `GRANT CONNECT ON DATABASE ${pgIdent(db)} TO ${u};\nGRANT USAGE ON SCHEMA ${s} TO ${u};\nGRANT SELECT ON ALL TABLES IN SCHEMA ${s} TO ${u};` },
        { engine: driver, purpose: "To write (destination)", sql: `GRANT USAGE, CREATE ON SCHEMA ${s} TO ${u};\nGRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${s} TO ${u};` },
      ];
    }
    case "redshift": {
      const u = pgIdent(user);
      const s = pgIdent(schema);
      return [
        { engine: driver, purpose: "To read (source)", sql: `GRANT USAGE ON SCHEMA ${s} TO ${u};\nGRANT SELECT ON ALL TABLES IN SCHEMA ${s} TO ${u};` },
        { engine: driver, purpose: "To write (destination)", sql: `GRANT USAGE, CREATE ON SCHEMA ${s} TO ${u};\nGRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${s} TO ${u};` },
      ];
    }
    case "mysql":
    case "mariadb": {
      const target = `${myIdent(db)}.*`;
      const who = user.startsWith("<") ? `${user}@'%'` : `${myString(user)}@'%'`;
      return [
        { engine: driver, purpose: "To read (source)", sql: `GRANT SELECT, SHOW VIEW ON ${target} TO ${who};` },
        { engine: driver, purpose: "To write (destination)", sql: `GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX ON ${target} TO ${who};` },
      ];
    }
    case "sqlserver": {
      const u = msIdent(user);
      return [
        { engine: driver, purpose: "To read (source)", sql: `USE ${msIdent(db)};\nALTER ROLE db_datareader ADD MEMBER ${u};\nGRANT VIEW DEFINITION TO ${u};` },
        { engine: driver, purpose: "To write (destination)", sql: `USE ${msIdent(db)};\nALTER ROLE db_datareader ADD MEMBER ${u};\nALTER ROLE db_datawriter ADD MEMBER ${u};\nGRANT CREATE TABLE TO ${u};\nGRANT ALTER ON SCHEMA::${msIdent(schema)} TO ${u};` },
      ];
    }
    case "oracle": {
      const u = user.startsWith("<") ? user : pgIdent(user.toUpperCase());
      const owner = schema.startsWith("<") ? "<owner>" : pgIdent(schema.toUpperCase());
      return [
        { engine: driver, purpose: "To read (source)", sql: `GRANT CREATE SESSION TO ${u};\n-- once per table the transfer reads:\nGRANT SELECT ON ${owner}.<table> TO ${u};` },
        { engine: driver, purpose: "To write (destination)", sql: `GRANT CREATE SESSION, CREATE TABLE TO ${u};\nALTER USER ${u} QUOTA UNLIMITED ON <tablespace>;` },
      ];
    }
    case "snowflake": {
      const r = sfIdent(filled(input.role, "<role>"));
      const wh = sfIdent(filled(input.warehouse, "<warehouse>"));
      const d = sfIdent(db);
      const sch = `${d}.${sfIdent(schema)}`;
      return [
        { engine: driver, purpose: "To read (source)", sql: `GRANT USAGE ON WAREHOUSE ${wh} TO ROLE ${r};\nGRANT USAGE ON DATABASE ${d} TO ROLE ${r};\nGRANT USAGE ON SCHEMA ${sch} TO ROLE ${r};\nGRANT SELECT ON ALL TABLES IN SCHEMA ${sch} TO ROLE ${r};` },
        { engine: driver, purpose: "To write (destination)", sql: `GRANT USAGE ON WAREHOUSE ${wh} TO ROLE ${r};\nGRANT USAGE ON DATABASE ${d} TO ROLE ${r};\nGRANT USAGE, CREATE TABLE ON SCHEMA ${sch} TO ROLE ${r};\nGRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${sch} TO ROLE ${r};` },
      ];
    }
    default:
      return [];
  }
}
