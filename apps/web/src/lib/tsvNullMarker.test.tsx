/** G7-F3: the "NULL marker" choice for TSV file destinations (Studio + Query console). */
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { describe, it } from "node:test";
import { renderToStaticMarkup } from "react-dom/server";
import { TsvNullMarkerField } from "../components/transfer/TsvNullMarkerField";
import { delimitedCell } from "./csvSafety";
import {
  TSV_NULL_DEFAULT,
  TSV_NULL_TEXT,
  destTsvNull,
  fileExportDestExtra,
  fileExportEndpoint,
  objectStoreDestExtra,
  tsvNullApplies,
} from "./fileExportDestination";
import { planJobStudioRoute } from "./jobStudioRoute";
import { serializeRows } from "./localFileExport";

const src = (rel: string) => readFileSync(new URL(`../${rel}`, import.meta.url), "utf8");

describe("TSV NULL marker", () => {
  it("applies to a TSV file_export and to an object-store .tsv object only", () => {
    assert.equal(tsvNullApplies({ kind: "file_export", format: "tsv" }), true);
    assert.equal(tsvNullApplies({ kind: "file_export", format: "csv" }), false);
    assert.equal(tsvNullApplies({ kind: "database", type: "s3", objectName: "out/orders.tsv" }), true);
    assert.equal(tsvNullApplies({ kind: "database", type: "sftp", objectName: "/in/x.TSV" }), true);
    assert.equal(tsvNullApplies({ kind: "database", type: "s3", objectName: "out/orders.csv" }), false);
    assert.equal(tsvNullApplies({ kind: "database", type: "postgresql", objectName: "orders.tsv" }), false);
  });

  it("rides the destination extra only when chosen and applicable", () => {
    assert.deepEqual(fileExportDestExtra({ format: "tsv", formulaProtection: false, tsvNull: TSV_NULL_TEXT }), { tsv_null: "\\N" });
    assert.deepEqual(fileExportDestExtra({ format: "tsv", formulaProtection: false, tsvNull: TSV_NULL_DEFAULT }), {});
    assert.deepEqual(fileExportDestExtra({ format: "csv", formulaProtection: false, tsvNull: TSV_NULL_TEXT }), {});
    assert.deepEqual(
      fileExportEndpoint({ format: "tsv", database: "", outputPath: "", formulaProtection: true, tsvNull: TSV_NULL_TEXT }).extra,
      { formula_protection: "prefix", tsv_null: "\\N" },
    );
    assert.deepEqual(objectStoreDestExtra({ type: "s3", objectName: "o.tsv", tsvNull: TSV_NULL_TEXT }), { tsv_null: "\\N" });
    assert.deepEqual(objectStoreDestExtra({ type: "s3", objectName: "o.json", tsvNull: TSV_NULL_TEXT }), {});
  });

  it("reopening a job restores the marker (file_export and object store); absent is the default", () => {
    const job = (destination: Record<string, unknown>) => ({
      transfer_request: { source: { kind: "file", extra: { file_id: "f1" } }, destination },
    });
    const fe = planJobStudioRoute(job({ kind: "file_export", format: "tsv", extra: { tsv_null: "\\N" } }), []);
    assert.equal(fe.destination.tsvNull, "\\N");
    const s3 = planJobStudioRoute(job({ kind: "database", format: "s3", table: "o.tsv", extra: { tsv_null: "\\N" } }), []);
    assert.equal(s3.destination.kind, "database");
    assert.equal(s3.destination.tsvNull, "\\N");
    const plain = planJobStudioRoute(job({ kind: "file_export", format: "tsv" }), []);
    assert.equal(plain.destination.tsvNull, "empty");
    assert.equal(destTsvNull({ tsv_null: "\\N" }), "\\N");
  });

  it("the offline browser TSV export writes NULL as \\N and '' as an empty cell", async () => {
    const rows = [{ id: "1", note: "" }, { id: "2", note: null }, { id: "3", note: "\\N" }];
    const marked = await serializeRows(rows, "tsv", { tsvNull: TSV_NULL_TEXT }).blob.text();
    assert.equal(marked, 'id\tnote\n1\t\n2\t\\N\n3\t"\\N"');
    const plain = await serializeRows(rows, "tsv").blob.text();
    assert.equal(plain, "id\tnote\n1\t\n2\t\n3\t\\N");
    assert.equal(delimitedCell(undefined, { delimiter: "\t", nullMarker: "\\N" }), "\\N");
  });

  it("the field offers Empty cell (default) and \\N", () => {
    const html = renderToStaticMarkup(<TsvNullMarkerField id="x" value="empty" onChange={() => undefined} />);
    assert.match(html, /NULL marker/);
    assert.match(html, /<option value="empty" selected="">Empty cell — default<\/option>/);
    assert.match(html, /<option value="\\N">\\N<\/option>/);
  });

  it("Studio shows it for file_export TSV and object-store .tsv, and round-trips it", () => {
    const page = src("pages/TransferPage.tsx");
    assert.equal((page.match(/<TsvNullMarkerField /g) || []).length, 2);
    // file_export: plan, analyze, contract, run; object store: endpoint extra + run.
    assert.equal((page.match(/tsvNull: tsvNullMarker \}\)/g) || []).length, 6);
    assert.match(page, /objectStoreDestExtra\(\{ type: destDriverType, objectName: targetCollection, tsvNull: tsvNullMarker \}\)/);
    assert.equal((page.match(/setTsvNullMarker\(dst\.tsvNull\)/g) || []).length, 2);
    assert.match(page, /setTsvNullMarker\(TSV_NULL_DEFAULT\)/);
    assert.match(page, /tsvNull: tsvNullMarker,\n\s*\}\);/); // offline export
  });

  it("the Query console TSV export sends tsv_null", () => {
    const page = src("pages/QueryPage.tsx");
    assert.match(page, /tsv_null: exportFormat === "tsv" \? tsvNull : TSV_NULL_DEFAULT/);
    assert.match(page, /<TsvNullMarkerField/);
  });
});
