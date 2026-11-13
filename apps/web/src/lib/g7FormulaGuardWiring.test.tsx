/**
 * G7-F2 / G7-F3: the export controls and every CSV the browser builds, tested on
 * behaviour (rendered components, hooks, produced bytes) — not on page source.
 */
import assert from "node:assert/strict";
import { after, afterEach, describe, it } from "node:test";
import { JSDOM } from "jsdom";

const dom = new JSDOM("<!doctype html><html><body></body></html>", { url: "http://localhost/" });
Object.assign(globalThis, {
  window: dom.window,
  document: dom.window.document,
  HTMLElement: dom.window.HTMLElement,
  Element: dom.window.Element,
  Node: dom.window.Node,
  IS_REACT_ACT_ENVIRONMENT: true,
});
Object.defineProperty(globalThis, "navigator", { value: dom.window.navigator, configurable: true });

const { act, createElement } = await import("react");
const { createRoot } = await import("react-dom/client");
const { FormulaProtectionToggle } = await import("../components/export/FormulaProtectionToggle");
const { TsvNullMarkerField } = await import("../components/transfer/TsvNullMarkerField");
const { useFileExportOptions, useQueryExportOptions } = await import("./exportOptions");
const { downloadJobQuarantineCsv } = await import("./api");
const { stepPreviewCsv } = await import("./stepPreviewCsv");
const { FILE_EXPORT_FORMULA_PROTECTION_HELP } = await import("./fileExportDestination");

let root: ReturnType<typeof createRoot> | undefined;

async function render(node: ReturnType<typeof createElement>) {
  if (!root) {
    const host = document.createElement("div");
    document.body.appendChild(host);
    root = createRoot(host);
  }
  await act(async () => root!.render(node));
}

afterEach(async () => {
  await act(async () => root?.unmount());
  root = undefined;
  document.body.replaceChildren();
});

after(() => dom.window.close());

/** Render a hook and hand back its latest value. */
async function hook<T>(use: () => T): Promise<{ current: () => T }> {
  let latest: T;
  function Probe() {
    latest = use();
    return null;
  }
  await render(createElement(Probe));
  return { current: () => latest };
}

describe("Studio file_export options (useFileExportOptions)", () => {
  it("a NEW job protects by default and sends formula_protection prefix", async () => {
    const opts = await hook(useFileExportOptions);
    assert.equal(opts.current().formulaProtection, true);
    assert.deepEqual(opts.current().endpoint({ format: "csv", database: "", outputPath: "" }), {
      kind: "file_export", format: "csv", database: "", output_path: "", extra: { formula_protection: "prefix" },
    });
    assert.deepEqual(opts.current().destExtra("tsv"), { formula_protection: "prefix" });
  });

  it("reopening a saved job without the field is off (byte-exact); New transfer turns it back on", async () => {
    const opts = await hook(useFileExportOptions);
    await act(async () => opts.current().restore({ formulaProtection: false, tsvNull: "\\N" }));
    assert.equal(opts.current().formulaProtection, false);
    assert.deepEqual(opts.current().endpoint({ format: "tsv", database: "", outputPath: "" }).extra, { tsv_null: "\\N" });
    assert.deepEqual(opts.current().objectStoreExtra({ type: "s3", objectName: "o.tsv" }), { tsv_null: "\\N" });
    await act(async () => opts.current().reset());
    assert.equal(opts.current().formulaProtection, true);
    assert.equal(opts.current().tsvNull, "empty");
  });
});

describe("Query console options (useQueryExportOptions)", () => {
  it("protects by default and sends tsv_null only for TSV", async () => {
    const opts = await hook(useQueryExportOptions);
    assert.deepEqual(opts.current().payload("csv"), { formula_protection: "prefix", tsv_null: "empty" });
    await act(async () => {
      opts.current().setFormulaProtection(false);
      opts.current().setTsvNull("\\N");
    });
    assert.deepEqual(opts.current().payload("tsv"), { formula_protection: "off", tsv_null: "\\N" });
    assert.deepEqual(opts.current().payload("csv"), { formula_protection: "off", tsv_null: "empty" });
  });
});

describe("FormulaProtectionToggle", () => {
  it("toggles, and its hint is the description, read once", async () => {
    const seen: boolean[] = [];
    await render(createElement(FormulaProtectionToggle, {
      id: "t1", checked: true, onChange: (v: boolean) => seen.push(v), help: FILE_EXPORT_FORMULA_PROTECTION_HELP,
    }));
    const box = document.getElementById("t1") as HTMLInputElement;
    assert.equal(box.checked, true);
    const label = box.closest("label")!;
    assert.equal(label.textContent, "Protect against spreadsheet formulas");
    const hint = document.getElementById(box.getAttribute("aria-describedby")!)!;
    assert.equal(label.contains(hint), false, "the hint is not also part of the accessible name");
    assert.match(hint.textContent || "", /' prefix so spreadsheets don't execute them.*reversible/);
    await act(async () => box.click());
    assert.deepEqual(seen, [false]);
  });
});

describe("TsvNullMarkerField", () => {
  it("offers Empty cell (default) and \\N and reports the choice", async () => {
    const seen: string[] = [];
    await render(createElement(TsvNullMarkerField, { id: "n1", value: "empty", onChange: (v: string) => seen.push(v) }));
    const select = document.getElementById("n1") as HTMLSelectElement;
    assert.deepEqual([...select.options].map((o) => o.textContent), ["Empty cell — default", "\\N"]);
    await act(async () => {
      select.value = "\\N";
      select.dispatchEvent(new dom.window.Event("change", { bubbles: true }));
    });
    assert.deepEqual(seen, ["\\N"]);
  });
});

describe("browser-built CSVs use the shared guard", () => {
  it("step-preview.csv defuses cells and headers, keeps numerals", () => {
    assert.equal(stepPreviewCsv(["=h", "n", "b"], [{ "=h": "@x", n: -5, b: true }]), "'=h,n,b\n'@x,-5,true");
  });

  it("the quarantine CSV fallback defuses formula cells like the server's, every cell quoted", async () => {
    const { blob } = await downloadJobQuarantineCsv("j1", [
      {
        row: 3, column: "note", target: "note", value: '=HYPERLINK("http://evil.example")',
        reason: "-1 too small", policy: "quarantine", suggested_transform: "", suggested_fix: "@fix",
        suggested_target_type: "",
      } as never,
    ]);
    assert.equal(
      (await blob.text()).split("\n")[1],
      '"3","note","note","\'=HYPERLINK(""http://evil.example"")","\'-1 too small","quarantine","","\'@fix",""',
    );
  });
});
