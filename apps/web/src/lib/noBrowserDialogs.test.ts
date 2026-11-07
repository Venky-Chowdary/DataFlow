/**
 * no-alert and no-restricted-globals.
 * Browser alert, confirm, and prompt cannot return. The scanner is the lint
 * gate `npm test` and `npm run lint` both run.
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { describe, it } from "node:test";
import { fileURLToPath } from "node:url";

const WEB = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

describe("no-alert / no-restricted-globals", () => {
  it("rejects browser alert, confirm, and prompt in the web app", () => {
    const result = spawnSync(process.execPath, [path.join(WEB, "scripts/check-no-browser-dialogs.mjs")], {
      encoding: "utf8",
    });
    assert.equal(result.status, 0, `${result.stdout || ""}\n${result.stderr || ""}`);
    assert.match(result.stdout, /no-alert \/ no-restricted-globals: clean/);
  });
});
