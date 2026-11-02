import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  STUDIO_CANVAS_KEY,
  STUDIO_SCHEMA_BROWSER_KEY,
  describeStudioFlags,
  readLocalStudioFlag,
  resolveStudioFlag,
  writeLocalStudioFlag,
} from "./studioFlags.js";

describe("studio flags", () => {
  it("requires the server and the browser", () => {
    assert.equal(resolveStudioFlag({ server: true, local: true }), true);
    assert.equal(resolveStudioFlag({ server: true, local: false }), false);
    assert.equal(resolveStudioFlag({ server: false, local: true }), false);
    assert.equal(resolveStudioFlag({ server: undefined, local: true }), false);
  });

  it("stores an explicit on bit and treats anything else as off", () => {
    const bag = new Map<string, string>();
    const storage = {
      getItem: (key: string) => bag.get(key) ?? null,
      setItem: (key: string, value: string) => { bag.set(key, value); },
      removeItem: (key: string) => { bag.delete(key); },
    };
    assert.equal(readLocalStudioFlag(STUDIO_CANVAS_KEY, storage), false);
    writeLocalStudioFlag(STUDIO_SCHEMA_BROWSER_KEY, true, storage);
    assert.equal(readLocalStudioFlag(STUDIO_SCHEMA_BROWSER_KEY, storage), true);
    writeLocalStudioFlag(STUDIO_SCHEMA_BROWSER_KEY, false, storage);
    assert.equal(readLocalStudioFlag(STUDIO_SCHEMA_BROWSER_KEY, storage), false);
    assert.equal(readLocalStudioFlag(STUDIO_CANVAS_KEY, null), false);
  });

  it("asks this browser to opt in and surfaces a failed settings read", () => {
    const pending = describeStudioFlags({
      serverSchema: true,
      serverCanvas: true,
      localSchema: false,
      localCanvas: false,
    });
    assert.equal(pending.canvas, false);
    assert.equal(pending.canvasOptIn, true);
    assert.equal(pending.schemaOptIn, true);
    const on = describeStudioFlags({
      serverSchema: true,
      serverCanvas: true,
      localSchema: true,
      localCanvas: true,
    });
    assert.equal(on.canvas, true);
    assert.equal(on.canvasOptIn, false);
    const failed = describeStudioFlags({
      serverSchema: true,
      serverCanvas: true,
      localSchema: true,
      localCanvas: true,
      error: "Failed to load workspace settings",
    });
    assert.equal(failed.canvas, false);
    assert.equal(failed.schemaBrowser, false);
    assert.equal(failed.canvasOptIn, false);
    assert.match(failed.error, /workspace settings/);
  });
});
