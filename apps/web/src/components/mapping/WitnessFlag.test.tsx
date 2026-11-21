/**
 * Mapping Witness on the Map step and in the Trust Report: a flag is compact,
 * says why when asked, offers exactly one action (Review), and the report
 * section states what was compared without inventing a pass.
 */
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { WitnessFlag } from "./WitnessFlag.js";
import { MappingEvidenceView, witnessHeadline } from "../trust/MappingEvidenceSection.js";
import { flagsBySource, witnessPairsKey, type WitnessPair, type WitnessRecord } from "../../lib/mappingWitness.js";

const swapPair: WitnessPair = {
  source: "given",
  target: "last_name",
  status: "flagged",
  reason: "60 of 60 matching keys hold this value in first_name.",
  flag: {
    kind: "swap",
    reason: "last_name and first_name look swapped: values headed for last_name match first_name on the destination keys.",
    suggested_target: "first_name",
    basis: "keyed",
    action: "review",
  },
  evidence: { mode: "keyed", compared: 60, agreed: 0 },
};

const witnessed: WitnessPair = {
  source: "mail",
  target: "email",
  status: "witnessed",
  reason: "60 of 60 matching keys hold the same value in email.",
  flag: null,
  evidence: {},
};

function record(pairs: WitnessPair[], decisions: WitnessRecord["decisions"] = []): WitnessRecord {
  const count = (s: string) => pairs.filter((p) => p.status === s).length;
  return {
    mapping_digest: "a".repeat(64),
    decisions,
    report: {
      version: "mapping_witness.v1",
      mode: "keyed",
      generated_at: "2026-11-21T10:00:00Z",
      key: { source: "cust_id", target: "customer_id" },
      sample: { source_rows: 60, destination_rows: 60, matching_keys: 60 },
      pairs,
      summary: {
        pairs: pairs.length,
        witnessed: count("witnessed"),
        consistent: count("consistent"),
        flagged: count("flagged"),
        differs: count("differs"),
        no_evidence: count("no_evidence"),
      },
      inputs_sha256: "b".repeat(64),
    },
  };
}

function text(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/&amp;/g, "&").replace(/\s+/g, " ");
}

describe("Witness flag on a Map row", () => {
  let dom: JSDOM;
  before(() => {
    dom = new JSDOM("<!doctype html><html><body><div id=root></div></body></html>", { pretendToBeVisual: true });
    Object.assign(globalThis, { window: dom.window, document: dom.window.document, IS_REACT_ACT_ENVIRONMENT: true });
  });
  after(() => dom.window.close());

  it("is a compact chip until asked why", () => {
    const html = renderToStaticMarkup(<WitnessFlag pair={swapPair} onReview={() => {}} />);
    assert.match(text(html), /Looks swapped/);
    assert.doesNotMatch(text(html), /look swapped: values headed/);
    assert.match(html, /aria-expanded="false"/);
  });

  it("discloses the reason and offers one action, Review", async () => {
    const { createRoot } = await import("react-dom/client");
    const { act } = await import("react");
    const host = document.getElementById("root")!;
    const reviewed: string[] = [];
    const root = createRoot(host);
    await act(async () => root.render(<WitnessFlag pair={swapPair} onReview={(s) => reviewed.push(s)} />));
    const chip = host.querySelector("button")!;
    await act(async () => chip.dispatchEvent(new dom.window.MouseEvent("click", { bubbles: true })));
    assert.equal(chip.getAttribute("aria-expanded"), "true");
    assert.match(host.textContent ?? "", /values headed for last_name match first_name/);
    const buttons = Array.from(host.querySelectorAll("button"));
    assert.deepEqual(buttons.map((b) => b.textContent?.trim()), ["Looks swapped", "Review"]);
    await act(async () => buttons[1].dispatchEvent(new dom.window.MouseEvent("click", { bubbles: true })));
    assert.deepEqual(reviewed, ["given"]);
    await act(async () => root.unmount());
  });

  it("renders nothing for a pair without a flag", () => {
    assert.equal(renderToStaticMarkup(<WitnessFlag pair={witnessed} />), "");
  });

  it("indexes only flagged pairs, case-insensitively", () => {
    const flags = flagsBySource(record([swapPair, witnessed]).report);
    assert.deepEqual([...flags.keys()], ["given"]);
    assert.equal(witnessPairsKey([{ source: "B", target: "x" }, { source: "a", target: "Y" }]),
      witnessPairsKey([{ source: "a", target: "y" }, { source: "b", target: "X" }, { source: "c", target: "" }]));
  });
});

describe("Mapping evidence in the Trust Report", () => {
  it("states what was compared and lists the flag with its review", () => {
    const rec = record([swapPair, witnessed], [
      { source: "given", target: "last_name", flag_kind: "swap", decision: "changed", new_target: "first_name", by: "ana@example.com", at: "2026-11-21T10:05:00Z" },
    ]);
    const t = text(renderToStaticMarkup(<MappingEvidenceView record={rec} />));
    assert.match(t, /2 mapped columns · 1 confirmed by values · 1 to review/);
    assert.match(t, /Compared on matching keys \(60 matching keys\)/);
    assert.match(t, /Changed to first_name by ana@example\.com/);
    assert.equal(witnessHeadline(rec), "2 mapped columns · 1 confirmed by values · 1 to review");
  });

  it("says plainly when no check ran, instead of implying a pass", () => {
    const t = text(renderToStaticMarkup(<MappingEvidenceView record={null} />));
    assert.match(t, /No mapping check ran for the mapping this job used/);
  });
});
