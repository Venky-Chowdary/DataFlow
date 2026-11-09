import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import {
  certificationBadgeClass,
  certificationLabel,
  certificationModeDetail,
  routeCertificationOf,
  type RouteCertificationMode,
} from "./routeCertification.js";

const mode = (over: Partial<RouteCertificationMode>): RouteCertificationMode => ({
  sync_mode: "full_refresh_overwrite",
  status: "unverified",
  checks_missing: [],
  cases_missing: [],
  skip_reasons: [],
  refusal_reason: null,
  ...over,
});

describe("route certification", () => {
  it("never shows a sold route without evidence as verified", () => {
    const cert = routeCertificationOf({});
    assert.equal(cert.status, "unverified");
    assert.equal(certificationLabel(cert.status), "No evidence");
    assert.ok(!certificationBadgeClass(cert.status).includes("success"));
  });

  it("gives passed, failed, skipped, partial and absent evidence distinct labels", () => {
    const labels = ["verified", "failed", "skipped", "unverified", "partial"].map(certificationLabel);
    assert.equal(new Set(labels).size, labels.length);
    assert.ok(certificationBadgeClass("verified").includes("success"));
    assert.ok(certificationBadgeClass("failed").includes("error"));
  });

  it("treats an unknown status as no evidence", () => {
    assert.equal(certificationLabel("certified"), "No evidence");
    const cert = routeCertificationOf({ certification: { status: "green" as never, modes: [], generated_at: null } });
    assert.equal(cert.status, "unverified");
  });

  it("names the missing checks, skip reason and refusal reason", () => {
    assert.equal(
      certificationModeDetail(mode({ status: "partial", checks_missing: ["independent_readback", "rerun"] })),
      "Overwrite: not proven — independent read-back, rerun",
    );
    assert.equal(
      certificationModeDetail(mode({ sync_mode: "upsert", status: "skipped", skip_reasons: ["MongoDB not reachable"] })),
      "Upsert: skipped — MongoDB not reachable",
    );
    assert.equal(
      certificationModeDetail(mode({ sync_mode: "full_refresh_append", status: "refused", refusal_reason: "Kafka topics are append-only" })),
      "Append: refused — Kafka topics are append-only",
    );
    assert.equal(certificationModeDetail(mode({ status: "verified" })), "Overwrite: verified");
  });

  it("Benchmarks keeps availability and evidence in separate columns", () => {
    const page = readFileSync(new URL("../pages/BenchmarksPage.tsx", import.meta.url), "utf8");
    assert.match(page, /<th>Availability<\/th>\s*<th>Evidence<\/th>/);
    assert.match(page, /routeCertificationOf\(r\)/);
    assert.doesNotMatch(page, /sold vs claimed/);
  });
});
