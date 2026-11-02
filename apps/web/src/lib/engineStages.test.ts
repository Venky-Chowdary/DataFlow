/**
 * The nine core checks must be presentable as engineering stages.
 *
 * A client migrating a million rows was shown "Engine running G1–G9" and could
 * not tell which internal id was inspecting their data, so every core gate needs
 * a stage name and a phrase for what it is doing while it runs.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  CORE_ENGINE_GATE_IDS,
  ENGINE_STAGES,
  engineStageLabel,
} from "./preflightGates";

describe("engine stages", () => {
  it("covers every core gate exactly once, in engine order", () => {
    assert.deepEqual(ENGINE_STAGES.map((s) => s.id), [...CORE_ENGINE_GATE_IDS]);
  });

  it("never shows a raw gate id as a stage name", () => {
    for (const stage of ENGINE_STAGES) {
      assert.doesNotMatch(stage.stage, /^g\d+/i);
      assert.ok(stage.stage.length > 3);
      assert.doesNotMatch(stage.running, /G1|G9|gate/i);
    }
  });

  it("resolves aliases to the same stage", () => {
    assert.equal(engineStageLabel("g3_schema"), engineStageLabel("g3_schema_contract"));
    assert.equal(engineStageLabel("g6_ddl"), "DDL compilation");
  });

  it("falls back to the gate label for a non-core check", () => {
    assert.equal(engineStageLabel("g13_source_coverage"), "Source column coverage");
  });
});
