import assert from "node:assert/strict";
import { test } from "node:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { JSDOM } from "jsdom";
import { DtIcon } from "./DtIcon";
import { StructurePreview } from "./ui/StructurePreview";

test("entity icons use one library with stable decorative sizing", () => {
  for (const name of ["folder", "folder-open", "table", "file", "database", "cloud"]) {
    for (const chrome of ["nav", "product"] as const) {
      for (const size of [16, 20, 24]) {
        const markup = renderToStaticMarkup(createElement(DtIcon, { name, chrome, size }));
        const document = new JSDOM(markup).window.document;
        const svg = document.querySelector("svg");
        assert.ok(svg);
        assert.equal(document.querySelectorAll("svg").length, 1);
        assert.equal(svg.getAttribute("width"), String(size));
        assert.equal(svg.getAttribute("height"), String(size));
        assert.equal(svg.getAttribute("aria-hidden"), "true");
        assert.ok(svg.classList.contains("lucide"));
        assert.ok(svg.childElementCount > 0);
        assert.equal(Boolean(document.querySelector(".dt-nav-icon")), chrome === "nav");
      }
    }
  }
});

test("table preview uses the table symbol rather than database or layers", () => {
  const markup = renderToStaticMarkup(createElement(StructurePreview, {
    columns: ["id"], rows: [{ id: 1 }], allowJson: true,
  }));
  const document = new JSDOM(markup).window.document;
  const tab = document.querySelector('[role="tab"]');
  assert.ok(tab);
  assert.match(tab.textContent || "", /Table/);
  const table = new JSDOM(renderToStaticMarkup(createElement(DtIcon, { name: "table", size: 13 })))
    .window.document.querySelector("svg");
  const database = new JSDOM(renderToStaticMarkup(createElement(DtIcon, { name: "database", size: 13 })))
    .window.document.querySelector("svg");
  assert.equal(tab.querySelector("svg")?.innerHTML, table?.innerHTML);
  assert.notEqual(table?.innerHTML, database?.innerHTML);
});

test("unknown entity names retain the navigation database fallback", () => {
  const fallback = renderToStaticMarkup(createElement(DtIcon, { name: "database" }));
  for (const name of ["unknown-entity", "constructor", "toString"]) {
    assert.equal(renderToStaticMarkup(createElement(DtIcon, { name })), fallback);
  }
});