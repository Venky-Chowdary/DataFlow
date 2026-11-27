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
  /** Why a typed value was not filled in (e.g. PUBLIC is every role). */
  note?: string;
}

/** Engines whose access is granted in SQL: the step checklist and grants apply to these only (review UI2 #10). */
export const SQL_GRANT_ENGINES = new Set(["postgresql", "redshift", "mysql", "mariadb", "sqlserver", "oracle", "snowflake"]);

export function isSqlGrantEngine(type: string): boolean {
  return SQL_GRANT_ENGINES.has(resolveDriverType(type));
}

/**
 * A value in a grant: typed by a person (always quoted / escaped for the
 * engine, whatever it holds) or a placeholder Datawrap writes (never from
 * input). The kind is a flag, never read from the text (re-review A: a value
 * starting with "<" was emitted raw).
 */
export type GrantValue = { kind: "typed"; text: string } | { kind: "placeholder"; name: string };

/**
 * A typed value is used only when it can sit on one line as one quoted token:
 * a control character (newline, NUL, line separator …) would split the
 * snippet's lines, so the placeholder is shown and the person fills it in.
 */
export function grantValue(text: string | undefined | null, placeholder: string): GrantValue {
  const t = String(text ?? "").trim();
  // Control characters split lines; Unicode format characters (bidi overrides
  // such as U+202E, zero-width spaces, isolates U+2066-2069 — category Cf)
  // make what an admin reads differ from what runs. Either: the placeholder.
  // eslint-disable-next-line no-control-regex
  if (!t || /[\u0000-\u001f\u007f-\u009f\u2028\u2029]|\p{Cf}/u.test(t) || t.length > 256) return { kind: "placeholder", name: placeholder };
  return { kind: "typed", text: t };
}

const show = (v: GrantValue, quote: (text: string) => string): string => (v.kind === "placeholder" ? `<${v.name}>` : quote(v.text));
const dq = (text: string) => `"${text.replace(/"/g, '""')}"`;
const bq = (text: string) => `\`${text.replace(/`/g, "``")}\``;
const br = (text: string) => `[${text.replace(/]/g, "]]")}]`;
/** A MySQL string literal, safe with or without NO_BACKSLASH_ESCAPES (backslash and quote doubled). */
const sq = (text: string) => `'${text.replace(/\\/g, "\\\\").replace(/'/g, "''")}'`;
/**
 * Snowflake folds an unquoted identifier to upper case and treats a quoted
 * one as case-sensitive (https://docs.snowflake.com/en/sql-reference/identifiers-syntax):
 * a plain name someone typed (compute_wh) stays unquoted so it resolves to
 * COMPUTE_WH; anything else is double-quoted.
 */
const sf = (text: string) => (/^[A-Za-z_][A-Za-z0-9_$]*$/.test(text) ? text : dq(text));

/**
 * The schema a failure named, read only from the engine's own phrasing of
 * that error (re-review C), never a word guessed out of prose:
 * - PostgreSQL / Redshift: `permission denied for schema raw`,
 *   `schema "raw" does not exist`
 * - SQL Server: `The specified schema name "raw" either does not exist or you
 *   do not have permission to use it.`
 * - Snowflake: `Schema 'DB.RAW' does not exist or not authorized.`
 */
export function schemaFromMessage(message: string | undefined, type: string): string {
  const said = String(message || "");
  const driver = resolveDriverType(type);
  // PostgreSQL and SQL Server print a name between double quotes without
  // doubling an inner quote, so the quoted text is taken as is (no "" decoding).
  // An unquoted name is taken only when it is the whole rest of the message:
  // "permission denied for schema My Schema" could be "My" or "My Schema".
  const patterns: RegExp[] = driver === "postgresql" || driver === "redshift"
    ? [/\bpermission denied for schema ([A-Za-z_][\w$]*)\s*\.?\s*$/, /\bschema "(.+)" does not exist\b/]
    : driver === "sqlserver"
      ? [/\bThe specified schema name "(.+)" either does not exist or you do not have permission to use it\b/]
      : driver === "snowflake"
        ? [/\bSchema '(?:[^'.]+\.)?([^'.]+)' does not exist or not authorized\b/]
        : [];
  for (const re of patterns) {
    const m = re.exec(said);
    if (m && m[1]) return m[1];
  }
  return "";
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
  const user = grantValue(input.username, "user");
  const db = grantValue(input.database, "database");
  const schema = grantValue(input.schema || schemaFromMessage(input.message, input.type), "schema");
  const line = (...parts: string[]) => parts.join("\n");
  // PostgreSQL / Redshift: PUBLIC (any case, quoted or not) is every role, never a login.
  const isPublic = user.kind === "typed" && /^"?public"?$/i.test(user.text);
  const pgUser: GrantValue = isPublic ? { kind: "placeholder", name: "user" } : user;
  const publicNote = isPublic ? "PUBLIC means every role, so it was not filled in: enter the login's own name." : undefined;
  switch (driver) {
    case "postgresql": {
      const u = show(pgUser, dq);
      const s = show(schema, dq);
      return [
        { engine: driver, purpose: "To read (source)", note: publicNote, sql: line(`GRANT CONNECT ON DATABASE ${show(db, dq)} TO ${u};`, `GRANT USAGE ON SCHEMA ${s} TO ${u};`, `GRANT SELECT ON ALL TABLES IN SCHEMA ${s} TO ${u};`) },
        { engine: driver, purpose: "To write (destination)", note: publicNote, sql: line(`GRANT USAGE, CREATE ON SCHEMA ${s} TO ${u};`, `GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${s} TO ${u};`) },
      ];
    }
    case "redshift": {
      const u = show(pgUser, dq);
      const s = show(schema, dq);
      return [
        { engine: driver, purpose: "To read (source)", note: publicNote, sql: line(`GRANT USAGE ON SCHEMA ${s} TO ${u};`, `GRANT SELECT ON ALL TABLES IN SCHEMA ${s} TO ${u};`) },
        { engine: driver, purpose: "To write (destination)", note: publicNote, sql: line(`GRANT USAGE, CREATE ON SCHEMA ${s} TO ${u};`, `GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${s} TO ${u};`) },
      ];
    }
    case "mysql":
    case "mariadb": {
      // In a database-level GRANT, _ and % are wildcards even inside backticks:
      // `app_prod`.* also grants on appXprod. Escape them (and the backslash)
      // so the grant names exactly this database (MySQL GRANT docs, "Database Privileges").
      const target = `${show(db, (text) => bq(text.replace(/\\/g, "\\\\").replace(/([_%])/g, "\\$1")))}.*`;
      const who = `${show(user, sq)}@'%'`;
      return [
        { engine: driver, purpose: "To read (source)", sql: `GRANT SELECT, SHOW VIEW ON ${target} TO ${who};` },
        { engine: driver, purpose: "To write (destination)", sql: `GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX ON ${target} TO ${who};` },
      ];
    }
    case "sqlserver": {
      const u = show(user, br);
      const d = show(db, br);
      return [
        { engine: driver, purpose: "To read (source)", sql: line(`USE ${d};`, `ALTER ROLE db_datareader ADD MEMBER ${u};`, `GRANT VIEW DEFINITION TO ${u};`) },
        { engine: driver, purpose: "To write (destination)", sql: line(`USE ${d};`, `ALTER ROLE db_datareader ADD MEMBER ${u};`, `ALTER ROLE db_datawriter ADD MEMBER ${u};`, `GRANT CREATE TABLE TO ${u};`, `GRANT ALTER ON SCHEMA::${show(schema, br)} TO ${u};`) },
      ];
    }
    case "oracle": {
      // Oracle stores unquoted names in upper case; the quoted form names exactly that.
      // Oracle quoted identifiers cannot contain a double quote at all (doubling is
      // not an escape there): such a value becomes the placeholder.
      const up = (v: GrantValue): GrantValue => (v.kind !== "typed" ? v
        : v.text.includes('"') ? { kind: "placeholder", name: v === user ? "user" : "owner" }
          : { kind: "typed", text: v.text.toUpperCase() });
      const u = show(up(user), dq);
      const owner = show(schema.kind === "typed" ? up(schema) : { kind: "placeholder", name: "owner" }, dq);
      return [
        { engine: driver, purpose: "To read (source)", sql: line(`GRANT CREATE SESSION TO ${u};`, `GRANT SELECT ON ${owner}.<table> TO ${u}; -- once per table the transfer reads`) },
        { engine: driver, purpose: "To write (destination)", sql: line(`GRANT CREATE SESSION, CREATE TABLE TO ${u};`, `ALTER USER ${u} QUOTA UNLIMITED ON <tablespace>;`) },
      ];
    }
    case "snowflake": {
      const r = show(grantValue(input.role, "role"), sf);
      const wh = show(grantValue(input.warehouse, "warehouse"), sf);
      const d = show(db, sf);
      const sch = `${d}.${show(schema, sf)}`;
      return [
        { engine: driver, purpose: "To read (source)", sql: line(`GRANT USAGE ON WAREHOUSE ${wh} TO ROLE ${r};`, `GRANT USAGE ON DATABASE ${d} TO ROLE ${r};`, `GRANT USAGE ON SCHEMA ${sch} TO ROLE ${r};`, `GRANT SELECT ON ALL TABLES IN SCHEMA ${sch} TO ROLE ${r};`) },
        { engine: driver, purpose: "To write (destination)", sql: line(`GRANT USAGE ON WAREHOUSE ${wh} TO ROLE ${r};`, `GRANT USAGE ON DATABASE ${d} TO ROLE ${r};`, `GRANT USAGE, CREATE TABLE ON SCHEMA ${sch} TO ROLE ${r};`, `GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${sch} TO ROLE ${r};`) },
      ];
    }
    default:
      return [];
  }
}
