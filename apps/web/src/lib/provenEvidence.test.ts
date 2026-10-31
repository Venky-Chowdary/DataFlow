import assert from "node:assert/strict";
import { readFileSync, readdirSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  BACKEND_SUITE,
  MARKETING_STACK,
  NOT_PROVEN,
  PROVEN_EVIDENCE,
  TRANSFER_READY_DRIVERS,
  catalogHonestyLead,
} from "./provenEvidence";

const SRC = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PUBLIC_DIRS = [
  path.join(SRC, "pages", "marketing"),
  path.join(SRC, "components", "landing"),
  path.join(SRC, "components", "marketing"),
];

function sourceFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((entry) => {
    const full = path.join(dir, entry);
    if (statSync(full).isDirectory()) return sourceFiles(full);
    return full.endsWith(".tsx") || full.endsWith(".ts") ? [full] : [];
  });
}

test("every evidence row names the artifact that backs it", () => {
  assert.ok(PROVEN_EVIDENCE.length > 0);
  for (const row of PROVEN_EVIDENCE) {
    assert.ok(row.cases > 0, `${row.claim} claims no cases`);
    assert.match(row.artifact, /\.json/, `${row.claim} has no artifact file`);
    assert.ok(row.engines.trim().length > 0, `${row.claim} names no engine`);
  }
});

test("the backend suite is only published while it is green", () => {
  assert.equal(BACKEND_SUITE.failed, 0);
  assert.ok(BACKEND_SUITE.passed > 0);
});

test("unproven areas carry a reason, never a promise", () => {
  for (const gap of NOT_PROVEN) {
    assert.ok(gap.reason.trim().length > 20, `${gap.area} has no real reason`);
    assert.doesNotMatch(gap.reason, /\b(soon|shortly|next release|coming)\b/i);
  }
});

test("public pages hold no certification claim we cannot show a certificate for", () => {
  // "SOC 2 Type II" / "ISO 27001 certified" read as an audit that has happened.
  // Controls are mapped, not certified — the copy has to say which.
  const forbidden = /(SOC ?2 Type ?I{1,2}|ISO ?27001 (certified|compliant)|HIPAA compliant|SOC ?2 certified)/i;
  for (const dir of PUBLIC_DIRS) {
    for (const file of sourceFiles(dir)) {
      const body = readFileSync(file, "utf8");
      const hit = body.match(forbidden);
      assert.equal(hit, null, `${path.relative(SRC, file)} claims ${hit?.[0]}`);
    }
  }
});

test("marketing pages never publish CI blockers as product gaps", () => {
  const forbidden =
    /build machine|No credentials provisioned|What we will not claim yet|certify (the route|those routes|warehouse)|on your tenant during onboarding|we do not display a certificate|Formal SOC 2/i;
  const marketingDirs = [
    path.join(SRC, "pages", "marketing"),
    path.join(SRC, "pages", "LandingPage.tsx"),
    path.join(SRC, "components", "landing"),
    path.join(SRC, "components", "marketing"),
  ];
  for (const dir of marketingDirs) {
    const files = dir.endsWith(".tsx") ? [dir] : sourceFiles(dir);
    for (const file of files) {
      const body = readFileSync(file, "utf8");
      const hit = body.match(forbidden);
      assert.equal(hit, null, `${path.relative(SRC, file)} publishes ${hit?.[0]}`);
    }
  }
});

test("catalog lead states the measured driver count, never the tile count", () => {
  const lead = catalogHonestyLead();
  assert.match(lead, new RegExp(`^${TRANSFER_READY_DRIVERS} drivers are transfer-ready`));
  assert.doesNotMatch(lead, /650|700\+|717|certified connectors/i);
});

test("marketing stack names only transfer-ready drivers", () => {
  // Catalog status `planned` in docs/CONNECTOR_READINESS_MATRIX.md. Naming one
  // of these on a public page would sell a tile as a live transfer.
  const planned = /\b(Redshift|Databricks|ClickHouse|MariaDB|CockroachDB|Shopify)\b/;
  assert.ok(MARKETING_STACK.length > 0);
  for (const row of MARKETING_STACK) {
    assert.equal(row.badge, "Transfer-ready");
    assert.doesNotMatch(row.items, planned, `${row.family} names a planned driver`);
    assert.doesNotMatch(row.note, planned, `${row.family} note names a planned driver`);
    assert.doesNotMatch(row.note, /Native MERGE/i);
  }
});

test("public pages never name a planned connector or an unbuilt control", () => {
  // MFA enforcement is design-only (docs/MFA_ENFORCEMENT_DESIGN.md); the IP
  // allowlist only applies to tenant custom domains; zero-loss absolutes and
  // planned connectors are left out (docs/MARKETING_AUDIT_2026-10.md §4).
  const forbidden =
    /\b(MFA|multi-factor|IP allow-?list|zero data loss|0 silent drops|Redshift|Databricks|ClickHouse|Shopify)\b|"(redshift|databricks|clickhouse|shopify)"/i;
  const marketingFiles = [
    ...sourceFiles(path.join(SRC, "pages", "marketing")),
    path.join(SRC, "pages", "LandingPage.tsx"),
    ...sourceFiles(path.join(SRC, "components", "landing")),
    ...sourceFiles(path.join(SRC, "components", "marketing")),
  ].filter((file) => !file.endsWith(".test.tsx") && !file.endsWith(".test.ts"));
  for (const file of marketingFiles) {
    const hit = readFileSync(file, "utf8").match(forbidden);
    assert.equal(hit, null, `${path.relative(SRC, file)} claims ${hit?.[0]}`);
  }
});

test("public pages carry no disclaimer copy", () => {
  // Owner policy 2026-10-31: positive copy only. What is not real is left out
  // instead of being announced as missing.
  const disclaimers =
    /not transfer-live|stays? Planned|at-least-once upsert until|We do not invent|no SOC|is not claimed|not yet|Honest labels/i;
  const marketingFiles = [
    ...sourceFiles(path.join(SRC, "pages", "marketing")),
    path.join(SRC, "pages", "LandingPage.tsx"),
    ...sourceFiles(path.join(SRC, "components", "landing")),
    ...sourceFiles(path.join(SRC, "components", "marketing")),
  ].filter((file) => !file.endsWith(".test.tsx") && !file.endsWith(".test.ts"));
  for (const file of marketingFiles) {
    const hit = readFileSync(file, "utf8").match(disclaimers);
    assert.equal(hit, null, `${path.relative(SRC, file)} publishes "${hit?.[0]}"`);
  }
});

test("public pages do not sell the catalog as live or native MERGE", () => {
  const forbidden = /Browse the live catalog|Native MERGE loaders|transfer-live drivers labeled in Studio/i;
  const marketingDirs = [
    path.join(SRC, "pages", "marketing"),
    path.join(SRC, "pages", "LandingPage.tsx"),
    path.join(SRC, "pages", "DocsPage.tsx"),
    path.join(SRC, "components", "landing"),
    path.join(SRC, "components", "marketing"),
    path.join(SRC, "lib", "helpDocs.ts"),
  ];
  for (const dir of marketingDirs) {
    const files = dir.endsWith(".tsx") || dir.endsWith(".ts") ? [dir] : sourceFiles(dir);
    for (const file of files) {
      const body = readFileSync(file, "utf8");
      const hit = body.match(forbidden);
      assert.equal(hit, null, `${path.relative(SRC, file)} claims ${hit?.[0]}`);
    }
  }
});

test("docs help never invents 130 live drivers or 734 catalog tiles", () => {
  const docs = readFileSync(path.join(SRC, "pages", "DocsPage.tsx"), "utf8");
  assert.doesNotMatch(docs, /\?\? 130/);
  assert.doesNotMatch(docs, /\?\? 734/);
  const help = readFileSync(path.join(SRC, "lib", "helpDocs.ts"), "utf8");
  assert.match(help, /\*\*Certified\*\*/);
  assert.doesNotMatch(help, /\*\*Live\*\* — governed transfer/);
  assert.doesNotMatch(help, /Redshift.*ship with upsert/);
  assert.doesNotMatch(help, /Viewer \/ Editor \/ Owner/);
});

test("offline form routing includes yaml and fixed-width; degraded live excludes email", () => {
  const types = readFileSync(path.join(SRC, "lib", "connectorTypes.ts"), "utf8");
  assert.match(types, /"yaml"/);
  assert.match(types, /"fixed_width"/);
  assert.match(types, /"kafka"/);
  assert.match(types, /d !== "email"/);
  const forms = readFileSync(path.join(SRC, "lib", "connectorFormConfig.ts"), "utf8");
  assert.match(forms, /"yaml", "fixed_width"/);
});

test("degraded transfer-live check does not greenwash certified:false brands", async () => {
  const { CATALOG_PLANNED_DRIVER_TYPES, isTransferLiveType, setTransferLiveDrivers } =
    await import("./connectorTypes.ts");
  const previous = ["postgresql", "stripe", "redshift", "salesforce", "hubspot"];
  setTransferLiveDrivers(previous);
  assert.equal(isTransferLiveType("postgresql"), true);
  assert.equal(isTransferLiveType("salesforce"), true);
  assert.equal(isTransferLiveType("hubspot"), true);
  assert.equal(isTransferLiveType("stripe"), true);
  assert.equal(isTransferLiveType("redshift"), false);
  assert.ok(CATALOG_PLANNED_DRIVER_TYPES.has("shopify"));
  assert.equal(CATALOG_PLANNED_DRIVER_TYPES.has("stripe"), false);
  setTransferLiveDrivers(null);
});

test("public pages carry no attributed customer quote", () => {
  // Datawrap is pre-reference. An invented quote is the same class of unbacked
  // claim the product refuses to make about a transfer.
  const quoteShape = /quote:\s*["'`]/;
  for (const dir of PUBLIC_DIRS) {
    for (const file of sourceFiles(dir)) {
      const body = readFileSync(file, "utf8");
      assert.doesNotMatch(body, quoteShape, `${path.relative(SRC, file)} ships a testimonial`);
    }
  }
});
