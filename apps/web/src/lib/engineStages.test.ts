/**
 * The nine core checks must be presentable as engineering stages.
 *
 * A client migrating a million rows was shown "Engine running G1–G9" and could
 * not tell which internal id was inspecting their data, so every core gate needs
 * a stage name and a phrase for what it is doing while it runs.
 */
import { describe, expect, it } from "vitest";
import {
  CORE_ENGINE_GATE_IDS,
  ENGINE_STAGES,
  engineStageLabel,
} from "./preflightGates";

describe("engine stages", () => {
  it("covers every core gate exactly once, in engine order", () => {
    expect(ENGINE_STAGES.map((s) => s.id)).toEqual([...CORE_ENGINE_GATE_IDS]);
  });

  it("never shows a raw gate id as a stage name", () => {
    for (const stage of ENGINE_STAGES) {
      expect(stage.stage).not.toMatch(/^g\d+/i);
      expect(stage.stage.length).toBeGreaterThan(3);
      expect(stage.running).not.toMatch(/G1|G9|gate/i);
    }
  });

  it("resolves aliases to the same stage", () => {
    expect(engineStageLabel("g3_schema")).toBe(engineStageLabel("g3_schema_contract"));
    expect(engineStageLabel("g6_ddl")).toBe("DDL compilation");
  });

  it("falls back to the gate label for a non-core check", () => {
    expect(engineStageLabel("g13_source_coverage")).toBe("Source column coverage");
  });
});
