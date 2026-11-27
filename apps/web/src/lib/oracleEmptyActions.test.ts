/**
 * Every Oracle '' gate action works in Studio, or is not offered as a button.
 * The request bodies built here are the ones apps/api
 * tests/test_oracle_empty_not_null_round5.py sends to /preflight/oracle-empty/sign.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { buildPreflightMappings, declaredDefaultValue, engineTransformToUi, type EditableMapping } from "./mapping";
import {
  ORACLE_EMPTY_GATE_ID,
  oracleEmptyActions,
  performOracleEmptyAction,
  type OracleEmptyAction,
  type OracleEmptySignBody,
  type OracleEmptySignResponse,
} from "./oracleEmptyActions";
import { studioPerforms } from "./studioActionKinds";
import type { PreflightResult } from "./types";

const webRoot = join(dirname(fileURLToPath(import.meta.url)), "..");

const MAPS: EditableMapping[] = [
  { source: "id", target: "ID", confidence: 0.99, destType: "VARCHAR2(40)", transform: "none" },
  { source: "note", target: "NOTE", confidence: 0.99, destType: "VARCHAR2(200)", transform: "none" },
] as EditableMapping[];

const DEST = { connector: "conn-ora-1", schema: "app", table: "dest" };
const AS_NULL: OracleEmptyAction = {
  id: "oracle_empty_as_null", kind: "oracle_empty_as_null",
  label: "Create NOTE as NULL-able; empty strings land as NULL",
  risk_acceptance: { option: "oracle_empty_as_null", columns: ["NOTE"], destination: DEST },
};
const CHECK_NOW: OracleEmptyAction = {
  id: "oracle_empty_check_now", kind: "oracle_empty_check_now",
  label: "Check note now: Datawrap reads the whole column, within a total of 5 minutes for this check",
  risk_acceptance: { option: "oracle_empty_check_now", columns: ["NOTE"] },
};
const CHECK_AGAIN: OracleEmptyAction = {
  ...CHECK_NOW, id: "oracle_empty_check_again", label: "Check note again now: (the warehouse bills the scan)",
};
const DEFAULT: OracleEmptyAction = {
  id: "oracle_default_transform", kind: "oracle_default_transform", column: "NOTE", transform: "default:<value>",
  label: "Map the transform default:<value> on note",
};
const FIX_CONNECTION: OracleEmptyAction = {
  id: "oracle_fix_connection", kind: "oracle_fix_connection", label: "Fix the source connection, then validate again",
};
const FIX_SOURCE: OracleEmptyAction = {
  id: "oracle_fix_source_values", kind: "oracle_fix_source_values", column: "NOTE",
  label: "Replace the empty values of note in the source, then validate again",
};

function preflight(status: "block" | "warn" | "pass", actions: OracleEmptyAction[]): PreflightResult {
  return {
    passed: status !== "block", passed_count: 1, total_gates: 2, readiness_score: 50, blockers: [],
    gates: [{ id: ORACLE_EMPTY_GATE_ID, status, message: "Oracle stores '' as NULL…", duration_ms: 0,
      details: { suggested_actions: actions } }],
  } as unknown as PreflightResult;
}

function deps(answer: Record<string, string> | null, sign?: (b: OracleEmptySignBody) => OracleEmptySignResponse) {
  const sent: OracleEmptySignBody[] = [];
  return {
    sent,
    deps: {
      mappings: MAPS,
      destTable: "DEST",
      ask: async () => answer,
      sign: async (body: OracleEmptySignBody) => {
        sent.push(body);
        if (!sign) throw new Error("not expected");
        return sign(body);
      },
    },
  };
}

test("a blocked or warning gate offers a button for each action Studio performs, text for the rest", () => {
  const { buttons, notes } = oracleEmptyActions(preflight("block", [AS_NULL, CHECK_NOW, DEFAULT, FIX_CONNECTION, FIX_SOURCE]));
  assert.deepEqual(buttons.map((a) => a.id), [
    "oracle_empty_as_null", "oracle_empty_check_now", "oracle_default_transform", "oracle_fix_connection",
  ]);
  assert.deepEqual(notes, [FIX_SOURCE.label]);
  const warn = oracleEmptyActions(preflight("warn", [CHECK_AGAIN]));
  assert.deepEqual(warn.buttons.map((a) => a.id), ["oracle_empty_check_again"]);
  assert.equal(oracleEmptyActions(preflight("pass", [CHECK_AGAIN])).buttons.length, 0);
});

test("Map default:<value> asks for the value and sends default:<value> with the next Validate", async () => {
  const { deps: d, sent } = deps({ value: "N/A" });
  const out = await performOracleEmptyAction(DEFAULT, d);
  assert.equal(out.status, "applied");
  if (out.status !== "applied") return;
  const note = out.next.find((m) => m.target === "NOTE")!;
  assert.equal(declaredDefaultValue(note), "N/A");
  assert.equal(engineTransformToUi(note.engineTransform), "default_value");
  const wire = buildPreflightMappings([], out.next);
  assert.equal(wire.find((m) => m.target === "NOTE")?.transform, "default:N/A");
  assert.equal(wire.find((m) => m.target === "ID")?.transform, undefined);
  assert.equal(sent.length, 0); // no signing needed
  // A blank value or a cancelled dialog changes nothing.
  assert.equal((await performOracleEmptyAction(DEFAULT, deps({ value: "  " }).deps)).status, "failed");
  assert.equal((await performOracleEmptyAction(DEFAULT, deps(null).deps)).status, "cancelled");
});

test("Create NULL-able asks for a reason, has the API sign it and sends the signed contract", async () => {
  const contract = { risk_id: "mrc-1", signature: "mrc-sha256:x", loss_classification: "oracle_empty_string_as_null" };
  const { deps: d, sent } = deps({ reason: "legacy rows" }, (body) => ({
    option: body.risk_acceptance.option, key: "empty_as_null_contract",
    signed: [{ source: "note", target: "NOTE", empty_as_null_contract: contract }],
  }));
  const out = await performOracleEmptyAction(AS_NULL, d);
  assert.deepEqual(sent, [{
    mappings: [{ source: "note", target: "NOTE", target_type: "VARCHAR2(200)", transform: undefined }],
    risk_acceptance: { option: "oracle_empty_as_null", columns: ["NOTE"], reason: "legacy rows", destination: DEST },
    dest_table: "DEST",
  }]);
  assert.equal(out.status, "applied");
  if (out.status !== "applied") return;
  const wire = buildPreflightMappings([], out.next);
  assert.deepEqual(wire.find((m) => m.target === "NOTE")?.empty_as_null_contract, contract);
  assert.equal(wire.find((m) => m.target === "ID")?.empty_as_null_contract, undefined);
});

test("Check now and Check again have the API issue the signed request and send it", async () => {
  for (const action of [CHECK_NOW, CHECK_AGAIN]) {
    const stamp = { key_id: "ak_1", signature: "abc", table: "DEST", source: "note", target: "NOTE" };
    const { deps: d, sent } = deps({}, () => ({
      option: "oracle_empty_check_now", key: "empty_check_now",
      signed: [{ source: "note", target: "NOTE", empty_check_now: stamp }],
    }));
    const out = await performOracleEmptyAction(action, d);
    assert.equal(sent[0].risk_acceptance.option, "oracle_empty_check_now");
    assert.deepEqual(sent[0].risk_acceptance.columns, ["NOTE"]);
    assert.equal(out.status, "applied");
    if (out.status !== "applied") continue;
    assert.deepEqual(buildPreflightMappings([], out.next).find((m) => m.target === "NOTE")?.empty_check_now, stamp);
  }
});

test("a refused signing or a cancelled dialog changes nothing and claims nothing", async () => {
  const refused = deps({ reason: "r" }, () => { throw new Error("risk_acceptance needs a signed-in user"); });
  const out = await performOracleEmptyAction(AS_NULL, refused.deps);
  assert.equal(out.status, "failed");
  if (out.status === "failed") assert.match(out.message, /signed-in user/);
  assert.equal((await performOracleEmptyAction(CHECK_NOW, deps(null).deps)).status, "cancelled");
  const nothing = deps({}, () => ({ option: "oracle_empty_check_now", key: "empty_check_now", signed: [] }));
  assert.equal((await performOracleEmptyAction(CHECK_NOW, nothing.deps)).status, "failed");
});

test("Fix the source connection opens Connectors", async () => {
  assert.equal((await performOracleEmptyAction(FIX_CONNECTION, deps(null).deps)).status, "open_connectors");
});

test("an action kind Studio does not perform is never a button and never 'Applied'", () => {
  for (const kind of ["oracle_empty_as_null", "oracle_empty_check_now", "oracle_default_transform",
    "oracle_fix_connection", "change_target_type", "add_transform"]) {
    assert.ok(studioPerforms(kind), kind);
  }
  for (const kind of ["oracle_fix_source_values", "open_advanced", "rename_destination", "made_up"]) {
    assert.ok(!studioPerforms(kind), kind);
  }
  const dash = readFileSync(join(webRoot, "components/transfer/ValidateDashboard.tsx"), "utf8");
  // The explain step's buttons are filtered to what Studio performs, and the
  // generic "Applied — re-running validation" branch is unreachable for others.
  assert.match(dash, /studioPerforms\(action\.kind\)\s*\n\s*&& action\.kind !== "open_mapping_proof"/);
  const generic = dash.indexOf('if (!studioPerforms(action.kind)) {');
  assert.ok(generic > 0 && generic < dash.indexOf('"Applied — re-running validation",\n      [\n        `Action:'));
  assert.match(dash, /data-testid="oracle-empty-actions"/);
  const page = readFileSync(join(webRoot, "pages/TransferPage.tsx"), "utf8");
  assert.match(page, /STUDIO_ORACLE_EMPTY_KINDS\.has\(String\(action\.kind\)\)\) \{\n\s*void applyOracleEmptyAction\(action\)/);
  assert.match(page, /default:\n\s*\/\/ Nothing was done: say so, never "Applied"\./);
});

test("Map default is refused, naming it, when the column already maps another transform", async () => {
  const upper = MAPS.map((m) => (m.target === "NOTE" ? { ...m, transform: "upper" as const } : m));
  const d = { ...deps({ value: "NA" }).deps, mappings: upper };
  const out = await performOracleEmptyAction(DEFAULT, d);
  assert.equal(out.status, "failed");
  if (out.status === "failed") assert.match(out.message, /already maps the transform upper/);
  // The gate's own action for such a column is explained as text, never a button.
  const { buttons, notes } = oracleEmptyActions(preflight("block", [
    { ...DEFAULT, existing_transform: "upper", label: "note already maps the transform upper…" },
  ]));
  assert.equal(buttons.length, 0);
  assert.deepEqual(notes, ["note already maps the transform upper…"]);
  // A column whose transform is already a default may change its value.
  const withDefault = MAPS.map((m) => (m.target === "NOTE" ? { ...m, transform: "default_value" as const,
    engineTransform: "default:old" } : m));
  const again = await performOracleEmptyAction(DEFAULT, { ...deps({ value: "NA" }).deps, mappings: withDefault });
  assert.equal(again.status, "applied");
});
