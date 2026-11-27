/**
 * The Map step names its source the way the run submits it (round 3 item 3).
 * The same cases drive the API test that posts these through /transfer/mapping-witness
 * and /transfer/run (apps/api/tests/fixtures/witness/source_ref_cases.json).
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { witnessSourceRef } from "./mappingWitness.js";
import { callableSourceExtra } from "./sourceReadMode.js";

type Case = {
  id: string;
  ui: Parameters<typeof witnessSourceRef>[0] & { readMode?: string; statement?: string };
  map_source_ref: { kind: string; name: string; statement: string };
  run_form: Record<string, string>;
  run_source_extra: Record<string, unknown> | null;
  /** false: the run submits another source, so this check must not attach. */
  attaches: boolean;
};

const { cases } = JSON.parse(
  readFileSync(new URL("../../../api/tests/fixtures/witness/source_ref_cases.json", import.meta.url), "utf8"),
) as { cases: Case[] };

describe("witnessSourceRef", () => {
  for (const c of cases) {
    it(`${c.id}: Map names the source as the run submits it`, () => {
      assert.deepEqual(witnessSourceRef(c.ui), c.map_source_ref);
      if (c.ui.sourceKind === "database" && c.attaches) {
        // The run's source extra comes from the same Map state (TransferPage's sourceExtra).
        assert.deepEqual(callableSourceExtra(c.ui.readMode, c.ui.statement || "") ?? null, c.run_source_extra);
      }
    });
  }
});
