/**
 * UX-30: identifiers were title-cased by CSS ("Postgresql", "Ux_audit_dst").
 * Engines show their own names; database and table names show as stored.
 */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { ConnectorCard } from "./ConnectorCard.js";

describe("connection row identifiers", () => {
  it("names the engine properly and keeps the database name as stored", () => {
    const html = renderToStaticMarkup(
      createElement(ConnectorCard, {
        connector: { id: "c1", name: "Orders", type: "postgresql", database: "ux_audit_dst", host: "db.internal", last_test_ok: true } as never,
        compact: true,
        onTest: () => {},
      }),
    );
    assert.match(html, /PostgreSQL/);
    assert.match(html, /ux_audit_dst/);
    assert.doesNotMatch(html, /Postgresql|Ux_audit_dst/);
  });

  it("no stylesheet capitalizes the row's identifiers", () => {
    const css = readFileSync(join(dirname(fileURLToPath(import.meta.url)), "..", "..", "styles", "connectors-page.css"), "utf8");
    const rule = css.slice(css.indexOf(".df2-connector-row-meta {"), css.indexOf("}", css.indexOf(".df2-connector-row-meta {")));
    assert.doesNotMatch(rule, /text-transform:\s*capitalize/);
  });
});
