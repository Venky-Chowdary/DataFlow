/**
 * UX-44: the switcher's "· 1" did not say what it counted, the Team tab said
 * "— 1 member(s)", and the header chips ("Live · Just now", "0 logins") wrapped
 * into narrow columns; the invite note was set in the code face.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { memberCountLabel, workspaceChoices } from "./workspace.js";

const src = join(dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel: string) => readFileSync(join(src, rel), "utf8");

describe("workspace member counts are labelled", () => {
  it("says members, with the right plural", () => {
    assert.equal(memberCountLabel(1), "1 member");
    assert.equal(memberCountLabel(3), "3 members");
    assert.equal(memberCountLabel(0), "0 members");
    assert.equal(workspaceChoices([{ id: "w", name: "Ops", member_count: 3 }])[0].label, "Ops · 3 members");
  });
  it("the Team tab uses the same label, not member(s)", () => {
    const team = read("pages/settings/TeamSettings.tsx");
    assert.doesNotMatch(team, /member\(s\)/);
    assert.match(team, /memberCountLabel\(w\.member_count\)/);
  });
});

describe("Team settings spacing", () => {
  const css = read("styles/settings-enterprise.css");
  it("header chips stay on one line", () => {
    assert.match(css, /\.df2-settings-section-head > \.df2-badge,\s*\.df2-team-block-head > \.df2-cell-meta \{\s*flex: none;\s*white-space: nowrap;/);
  });
  it("the live stamp is not title-cased and the invite note is not monospace", () => {
    assert.match(css, /\.df2-settings-section-head > \.df2-badge \{\s*text-transform: none;/);
    assert.match(css, /\.df2-team-invite-footer > \.df2-cell-meta \{\s*font-family: inherit;/);
  });
});
