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
  id: "reach" | "sign_in" | "catalog" | "privileges";
  label: string;
  state: TestStepState;
  /** One plain sentence. */
  detail: string;
}

const LABELS: Record<TestStep["id"], string> = {
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
  if (/^(network|config)\./.test(code)) return "reach";
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
    reach: "The host answered.",
    sign_in: "The login was accepted.",
    catalog: "The database is visible to this login.",
    privileges: "",
  };
  const failedDetail: Record<TestStep["id"], string> = {
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

const placeholder = (value: string | undefined, fallback: string) => (value && value.trim() ? value.trim() : fallback);

function pgIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
function myIdent(name: string): string {
  return `\`${name.replace(/`/g, "``")}\``;
}
function myString(value: string): string {
  return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "''")}'`;
}
function msIdent(name: string): string {
  return `[${name.replace(/]/g, "]]")}]`;
}

/**
 * The grants a transfer needs on this engine, with what the person typed
 * filled in (placeholders in angle brackets otherwise). Empty for engines
 * whose access is not granted in SQL (files, SaaS, cloud IAM).
 */
export function grantFixes(input: { type: string; username?: string; database?: string; schema?: string; warehouse?: string; role?: string }): GrantFix[] {
  const driver = resolveDriverType(input.type);
  const user = placeholder(input.username, "<user>");
  const db = placeholder(input.database, "<database>");
  switch (driver) {
    case "postgresql":
    case "redshift": {
      const schema = placeholder(input.schema, "public");
      const u = pgIdent(user);
      const s = pgIdent(schema);
      return [
        { engine: driver, purpose: "To read (source)", sql: `GRANT CONNECT ON DATABASE ${pgIdent(db)} TO ${u};\nGRANT USAGE ON SCHEMA ${s} TO ${u};\nGRANT SELECT ON ALL TABLES IN SCHEMA ${s} TO ${u};` },
        { engine: driver, purpose: "To write (destination)", sql: `GRANT USAGE, CREATE ON SCHEMA ${s} TO ${u};\nGRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${s} TO ${u};` },
      ];
    }
    case "mysql":
    case "mariadb": {
      const target = `${myIdent(db)}.*`;
      const who = `${myString(user)}@'%'`;
      return [
        { engine: driver, purpose: "To read (source)", sql: `GRANT SELECT, SHOW VIEW ON ${target} TO ${who};` },
        { engine: driver, purpose: "To write (destination)", sql: `GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, INDEX ON ${target} TO ${who};` },
      ];
    }
    case "sqlserver": {
      const schema = placeholder(input.schema, "dbo");
      const u = msIdent(user);
      return [
        { engine: driver, purpose: "To read (source)", sql: `USE ${msIdent(db)};\nALTER ROLE db_datareader ADD MEMBER ${u};\nGRANT VIEW DEFINITION TO ${u};` },
        { engine: driver, purpose: "To write (destination)", sql: `USE ${msIdent(db)};\nALTER ROLE db_datawriter ADD MEMBER ${u};\nGRANT CREATE TABLE TO ${u};\nGRANT ALTER ON SCHEMA::${msIdent(schema)} TO ${u};` },
      ];
    }
    case "oracle": {
      const u = pgIdent(user.toUpperCase());
      const owner = placeholder(input.schema, "<owner>");
      return [
        { engine: driver, purpose: "To read (source)", sql: `GRANT CREATE SESSION TO ${u};\n-- once per table the transfer reads:\nGRANT SELECT ON ${pgIdent(owner.toUpperCase())}.<table> TO ${u};` },
        { engine: driver, purpose: "To write (destination)", sql: `GRANT CREATE SESSION, CREATE TABLE TO ${u};\nALTER USER ${u} QUOTA UNLIMITED ON USERS;` },
      ];
    }
    case "snowflake": {
      const role = placeholder(input.role, "<role>");
      const wh = placeholder(input.warehouse, "<warehouse>");
      const schema = `${pgIdent(db)}.${pgIdent(placeholder(input.schema, "PUBLIC"))}`;
      const r = pgIdent(role);
      return [
        { engine: driver, purpose: "To read (source)", sql: `GRANT USAGE ON WAREHOUSE ${pgIdent(wh)} TO ROLE ${r};\nGRANT USAGE ON DATABASE ${pgIdent(db)} TO ROLE ${r};\nGRANT USAGE ON SCHEMA ${schema} TO ROLE ${r};\nGRANT SELECT ON ALL TABLES IN SCHEMA ${schema} TO ROLE ${r};` },
        { engine: driver, purpose: "To write (destination)", sql: `GRANT USAGE, CREATE TABLE ON SCHEMA ${schema} TO ROLE ${r};\nGRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA ${schema} TO ROLE ${r};` },
      ];
    }
    default:
      return [];
  }
}
