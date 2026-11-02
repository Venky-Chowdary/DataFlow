import assert from "node:assert/strict";
import { test } from "node:test";
import {
  declaredReadOptionCount,
  readOptionsPayload,
  readWindowFromDraft,
  type ReadWindowDraft,
} from "./readOptions";

const DEFAULT_DRAFT: ReadWindowDraft = {
  sheet: "",
  headerRow: "1",
  headerless: false,
  skipRows: "0",
  skipFooter: "0",
  encoding: "",
  delimiter: "",
};

const both = { isWorkbook: true, isDelimited: true };

test("a default window sends nothing, so today's behaviour is untouched", () => {
  assert.equal(readOptionsPayload(undefined), "");
  assert.equal(readOptionsPayload({}), "");
  const resolved = readWindowFromDraft(DEFAULT_DRAFT, both);
  assert.deepEqual(resolved.options, {});
  assert.equal(readOptionsPayload(resolved.options ?? {}), "");
  assert.equal(declaredReadOptionCount(resolved.options ?? {}), 0);
});

test("an unset sheet index is not a declaration", () => {
  assert.equal(readOptionsPayload({ sheet_index: -1 }), "");
  assert.equal(readOptionsPayload({ sheet_index: 0 }), '{"sheet_index":0}');
});

test("only the declared fields ride the wire", () => {
  const payload = readOptionsPayload({ sheet: "Q3", header_row: 4, skip_footer: 2 });
  assert.deepEqual(JSON.parse(payload), { sheet: "Q3", header_row: 4, skip_footer: 2 });
});

test("a workbook window carries the sheet and drops the codec", () => {
  const resolved = readWindowFromDraft(
    { ...DEFAULT_DRAFT, sheet: "Q3 Actuals", headerRow: "3", skipFooter: "1", encoding: "cp1252", delimiter: ";" },
    { isWorkbook: true, isDelimited: false },
  );
  assert.deepEqual(resolved.options, { sheet: "Q3 Actuals", header_row: 3, skip_footer: 1 });
});

test("a delimited window carries the codec and drops the sheet", () => {
  const resolved = readWindowFromDraft(
    { ...DEFAULT_DRAFT, sheet: "Sheet2", encoding: "latin-1", delimiter: "\t", skipRows: "2" },
    { isWorkbook: false, isDelimited: true },
  );
  assert.deepEqual(resolved.options, { skip_rows: 2, encoding: "latin-1", delimiter: "\t" });
});

test("headerless mode declares header row 0 and keeps it on the wire", () => {
  const resolved = readWindowFromDraft({ ...DEFAULT_DRAFT, headerless: true }, both);
  assert.deepEqual(resolved.options, { header_row: 0 });
  assert.deepEqual(JSON.parse(readOptionsPayload(resolved.options ?? {})), { header_row: 0 });
  assert.equal(declaredReadOptionCount(resolved.options ?? {}), 1);
});

test("a non-numeric or negative offset is refused, not rounded", () => {
  for (const bad of ["-1", "2.5", "abc", "1e3"]) {
    const resolved = readWindowFromDraft({ ...DEFAULT_DRAFT, skipRows: bad }, both);
    assert.equal(resolved.options, null, `skipRows=${bad} should be refused`);
    assert.match(resolved.error, /whole numbers/);
  }
});

test("header row 0 without the headerless toggle is refused", () => {
  const resolved = readWindowFromDraft({ ...DEFAULT_DRAFT, headerRow: "0" }, both);
  assert.equal(resolved.options, null);
  assert.match(resolved.error, /1-based/);
});

test("an empty offset field reads as zero rather than failing", () => {
  const resolved = readWindowFromDraft({ ...DEFAULT_DRAFT, skipRows: "  ", skipFooter: "" }, both);
  assert.deepEqual(resolved.options, {});
});
