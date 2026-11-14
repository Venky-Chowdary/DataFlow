/**
 * UX-16: rows that open a drawer are either list items with their own button
 * (connections, contracts, schedules, related jobs) or buttons in a labelled
 * group (transforms) — never a button around buttons, never a non-item in a list.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const src = join(dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel: string) => readFileSync(join(src, rel), "utf8");

describe("row semantics", () => {
  it("schedule rows are list items", () => {
    const card = read("components/ui/PipelineCard.tsx");
    const compact = card.slice(0, card.indexOf("<article"));
    assert.match(compact, /role="listitem"/);
    assert.doesNotMatch(compact, /role=\{onSelect \? "button"/);
  });
  it("the transforms container is a group, since its rows are buttons", () => {
    assert.match(read("pages/TransformsPage.tsx"), /className="df2-pipeline-rows df2-xform-rows" role="group" aria-label="Transforms" /);
  });
  it("a related job stays a list item with a real button inside", () => {
    const drawer = read("components/ConnectorDetailDrawer.tsx");
    assert.doesNotMatch(drawer, /role=\{onOpenJob \? "button" : undefined\}/);
    assert.match(drawer, /className="df2-drawer-related-open"/);
  });
});
