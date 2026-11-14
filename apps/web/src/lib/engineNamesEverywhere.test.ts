/**
 * UX-30 follow-up: engine types reached users raw ("postgresql · 127.0.0.1…" in the
 * connection drawer, "postgresql" in its TYPE tile). Every engine render uses connectorDisplayName.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { connectorDisplayName } from "./connectorTypes.js";

const src = join(dirname(fileURLToPath(import.meta.url)), "..");
const read = (rel: string) => readFileSync(join(src, rel), "utf8");

describe("engine names are display names wherever users see them", () => {
  it("the helper names the engines the drawer showed raw", () => {
    assert.equal(connectorDisplayName("postgresql"), "PostgreSQL");
    assert.equal(connectorDisplayName("sqlserver"), "SQL Server");
    assert.equal(connectorDisplayName("mysql"), "MySQL");
  });

  it("the connection drawer subtitle, TYPE tile and job lines use the helper", () => {
    const drawer = read("components/ConnectorDetailDrawer.tsx");
    assert.match(drawer, /subtitle=\{`\$\{connectorDisplayName\(c\.type\)\} · \$\{endpoint\}`\}/);
    assert.match(drawer, /<strong>\{connectorDisplayName\(c\.type\)\}<\/strong>/);
    assert.doesNotMatch(drawer, /c\.type\.replace\(/);
    assert.doesNotMatch(drawer, /\{job\.source_type\} → \{job\.destination_type\}/);
  });

  const sites: Array<[string, RegExp]> = [
    ["components/ui/WorkspaceSearch.tsx", /meta: `\$\{c\.type\} ·/],
    ["components/exit/SystemFormDrawer.tsx", /\(\{c\.type\}\)/],
    ["components/ConnectionWorkbench.tsx", /\{c\.name\} · \{c\.type\}|\{job\.source_type\} → \{job\.destination_type\}/],
    ["components/transfer/SourceStepAside.tsx", /<span>\{(sourceConnector|c)\.type\}/],
    ["pages/TransferPage.tsx", /`\$\{sourceConnector\.type\}\$\{/],
    ["pages/TransformsPage.tsx", /\{c\.name\} · \{c\.type\}/],
    ["components/MappingProofDrawer.tsx", /· \{proof\.destination_db_type\}/],
    ["pages/DashboardPage.tsx", /\{c\.type\}\{c\.database|\{job\.source_type\} → \{job\.destination_type\}/],
  ];
  for (const [file, raw] of sites) {
    it(`${file} renders no raw engine id`, () => {
      const text = read(file);
      assert.doesNotMatch(text, raw);
      assert.match(text, /connectorDisplayName\(/);
    });
  }
});
