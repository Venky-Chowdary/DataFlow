/**
 * AI involvement in the Trust Report: each AI decision shows who proposed it,
 * what a person decided and which check confirmed it; only a confirming check is green.
 */
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import { JSDOM } from "jsdom";
import React from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { AiInvolvementView, aiHeadline } from "./AiInvolvementSection.js";
import { checkTone, decisionLabel, originLabel, type AiDecisionEntry, type AiDecisionLedger } from "../../lib/aiDecisions.js";

const entry = (over: Partial<AiDecisionEntry>): AiDecisionEntry => ({
  id: "aid_1",
  kind: "mapping_proposal",
  subject: "mail → email",
  origin: { type: "deterministic", engine: "semantic_mapper", method: "hungarian" },
  inputs_sha256: "c".repeat(64),
  proposal: {},
  decision: { status: "accepted", by: "ana@example.com", at: null, how: "The job ran with this target." },
  verification: { check: "mapping_witness", status: "witnessed", detail: "60 of 60 matching keys agree." },
  ...over,
});

const ledger: AiDecisionLedger = {
  version: "ai_decision_ledger.v1",
  job_id: "job-1",
  ledger_sha256: "d".repeat(64),
  entries: [
    entry({}),
    entry({
      id: "aid_2",
      kind: "model_mapping_suggestion",
      subject: "given → first_name",
      origin: { type: "model", provider: "anthropic", model: "claude-test" },
      decision: { status: "rejected", by: "ana@example.com", at: null, how: "" },
      verification: { check: "mapping_witness", status: "supported" },
    }),
    entry({
      id: "aid_3",
      kind: "witness_flag",
      subject: "given → last_name",
      decision: { status: "unreviewed", by: null, at: null, how: "" },
      verification: { check: "mapping_witness", status: "flagged" },
    }),
    entry({ id: "aid_4", subject: "login → username", verification: { check: "none", status: "not_checked" } }),
  ],
};

function text(html: string): string {
  return html.replace(/<[^>]+>/g, " ").replace(/&#x27;/g, "'").replace(/&amp;/g, "&").replace(/\s+/g, " ");
}

describe("AI involvement section", () => {
  it("counts decisions, model proposals and confirmed checks", () => {
    assert.equal(aiHeadline(ledger), "4 AI decisions · 1 by a model · 2 confirmed by a check");
  });

  it("shows origin, decision and check per row", () => {
    const t = text(renderToStaticMarkup(<AiInvolvementView ledger={ledger} />));
    assert.match(t, /Model suggestion given → first_name Mapping check: supported Proposed by anthropic · claude-test · Not adopted · ana@example\.com/);
    assert.match(t, /Mapping check given → last_name Mapping check: flagged Proposed by the Datawrap mapper · Not reviewed/);
    assert.match(t, /login → username Not checked Proposed by the Datawrap mapper · Accepted · ana@example\.com/);
    assert.match(t, /Ledger SHA-256 d{64}/);
  });

  it("paints green only where a check confirmed the decision", () => {
    assert.deepEqual(ledger.entries.map(checkTone), ["ok", "ok", "warn", "muted"]);
    assert.equal(originLabel({ type: "model", provider: "openai", model: "unrecorded" }), "openai");
    assert.equal(decisionLabel(entry({ decision: { status: "held", by: null, at: null, how: "" } })), "Held for review");
    assert.equal(
      decisionLabel(entry({ decision: { status: "not_reviewed_continued", by: "ann@example.com", at: null, how: "" } })),
      "Not reviewed (continued) · ann@example.com",
    );
  });

  it("names the Map approver, the schedule that ran it, and a partial ledger", () => {
    const scheduled = entry({ decision: { status: "approved", by: "dee@example.com", at: null, how: "",
      run_by: "nightly", run_kind: "schedule" } });
    assert.equal(decisionLabel(scheduled), "Approved in Map · dee@example.com · run by schedule nightly");
    const t = text(renderToStaticMarkup(<AiInvolvementView ledger={{ ...ledger, status: "partial",
      problems: ["The AI-egress chain could not be read, so model calls may be missing."] }} />));
    assert.match(t, /Partial: The AI-egress chain could not be read/);
  });

  it("says so when nothing is recorded or the ledger could not be built", () => {
    assert.match(text(renderToStaticMarkup(<AiInvolvementView ledger={{ ...ledger, entries: [] }} />)),
      /No AI proposals or model calls are recorded for this job/);
    assert.match(text(renderToStaticMarkup(<AiInvolvementView ledger={{ ...ledger, status: "unavailable", reason: "The AI decisions ledger could not be built for this pack.", entries: [] }} />)),
      /could not be built/);
  });
});

describe("AI decisions client", () => {
  let dom: JSDOM;
  before(() => {
    dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
    Object.assign(globalThis, { window: dom.window, document: dom.window.document,
      localStorage: dom.window.localStorage, sessionStorage: dom.window.sessionStorage });
  });
  after(() => dom.window.close());

  it("treats a body that is not a ledger as an error, never as 'no AI involved'", async () => {
    const { fetchAiDecisions } = await import("../../lib/aiDecisions.js");
    const { fetchJobMappingWitness } = await import("../../lib/mappingWitness.js");
    const saved = globalThis.fetch;
    globalThis.fetch = (async () => new Response(JSON.stringify({ verdict: "proven" }), { status: 200 })) as typeof fetch;
    try {
      await assert.rejects(fetchAiDecisions("job-1"), /could not be read/);
      assert.equal(await fetchJobMappingWitness("job-1"), null);
      globalThis.fetch = (async () => new Response(JSON.stringify({ witness: { report: {} } }), { status: 200 })) as typeof fetch;
      await assert.rejects(fetchJobMappingWitness("job-1"), /could not be read/);
    } finally {
      globalThis.fetch = saved;
    }
  });
});
