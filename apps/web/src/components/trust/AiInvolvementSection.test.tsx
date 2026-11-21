/**
 * AI involvement in the Trust Report: each AI decision shows who proposed it,
 * what a person decided and which check confirmed it; only a confirming check is green.
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
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
    assert.match(t, /Model suggestion given → first_name anthropic · claude-test Not adopted · ana@example\.com Mapping check: supported/);
    assert.match(t, /Mapping check given → last_name Deterministic Not reviewed Mapping check: flagged/);
    assert.match(t, /login → username Deterministic Accepted · ana@example\.com Not checked/);
    assert.match(t, /Ledger SHA-256 d{64}/);
  });

  it("paints green only where a check confirmed the decision", () => {
    assert.deepEqual(ledger.entries.map(checkTone), ["ok", "ok", "warn", "muted"]);
    assert.equal(originLabel({ type: "model", provider: "openai", model: "unrecorded" }), "openai");
    assert.equal(decisionLabel(entry({ decision: { status: "held", by: null, at: null, how: "" } })), "Held for review");
  });

  it("says so when nothing is recorded or the ledger could not be built", () => {
    assert.match(text(renderToStaticMarkup(<AiInvolvementView ledger={{ ...ledger, entries: [] }} />)),
      /No AI proposals or model calls are recorded for this job/);
    assert.match(text(renderToStaticMarkup(<AiInvolvementView ledger={{ ...ledger, status: "unavailable", reason: "The AI decisions ledger could not be built for this pack.", entries: [] }} />)),
      /could not be built/);
  });
});
