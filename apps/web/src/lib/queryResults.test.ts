import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  classifyCell,
  compareValues,
  filterRows,
  nextSort,
  sortRows,
  toCsv,
  toMarkdown,
  typeTone,
} from "./queryResults";

describe("classifyCell", () => {
  it("distinguishes a missing key from an explicit null", () => {
    // The whole point: a document that lacks the field is not the same fact as
    // a row whose column is NULL, and the engine writes them differently.
    assert.equal(classifyCell({}, "email").kind, "missing");
    assert.equal(classifyCell({ email: null }, "email").kind, "null");
  });

  it("treats undefined as null rather than missing when the key exists", () => {
    const row: Record<string, unknown> = { email: undefined };
    assert.equal(classifyCell(row, "email").kind, "null");
  });

  it("distinguishes an empty string from null", () => {
    const empty = classifyCell({ note: "" }, "note");
    assert.equal(empty.kind, "empty");
    assert.equal(empty.text, "(empty)");
    assert.equal(classifyCell({ note: null }, "note").text, "NULL");
  });

  it("does not treat a whitespace string as empty", () => {
    assert.equal(classifyCell({ note: " " }, "note").kind, "value");
  });

  it("serialises objects and arrays as json", () => {
    assert.deepEqual(classifyCell({ meta: { a: 1 } }, "meta"), {
      kind: "json",
      text: '{"a":1}',
    });
    assert.equal(classifyCell({ tags: [1, 2] }, "tags").kind, "json");
  });

  it("preserves exact numeric text rather than reformatting it", () => {
    // Precision-sensitive product: the grid must not round or re-format.
    assert.equal(classifyCell({ amt: "1.7500" }, "amt").text, "1.7500");
    assert.equal(
      classifyCell({ id: "9223372036854775807" }, "id").text,
      "9223372036854775807",
    );
  });

  it("renders booleans and zero as values, not blanks", () => {
    assert.deepEqual(classifyCell({ ok: false }, "ok"), { kind: "value", text: "false" });
    assert.deepEqual(classifyCell({ n: 0 }, "n"), { kind: "value", text: "0" });
  });

  it("does not read inherited prototype keys as present", () => {
    assert.equal(classifyCell({}, "toString").kind, "missing");
  });
});

describe("typeTone", () => {
  it("reports unknown when the catalog reported nothing", () => {
    assert.equal(typeTone(undefined), "unknown");
    assert.equal(typeTone(""), "unknown");
    assert.equal(typeTone("unknown"), "unknown");
  });

  it("classifies families case-insensitively", () => {
    assert.equal(typeTone("bigint"), "number");
    assert.equal(typeTone("DECIMAL(20,0)"), "number");
    assert.equal(typeTone("TIMESTAMP WITH TIME ZONE"), "time");
    assert.equal(typeTone("boolean"), "bool");
    assert.equal(typeTone("jsonb"), "struct");
    assert.equal(typeTone("BYTEA"), "binary");
    assert.equal(typeTone("varchar(50)"), "text");
  });
});

describe("compareValues", () => {
  it("sorts numerically, not lexically", () => {
    assert.ok(compareValues(9, 10) < 0);
    assert.ok(compareValues("9", "10") < 0);
  });

  it("puts nulls last in both directions of the comparator", () => {
    assert.ok(compareValues(null, 1) > 0);
    assert.ok(compareValues(1, null) < 0);
    assert.equal(compareValues(null, undefined), 0);
  });

  it("falls back to natural string comparison for text", () => {
    assert.ok(compareValues("alpha", "beta") < 0);
    assert.ok(compareValues("item2", "item10") < 0);
  });

  it("does not coerce empty strings into zero", () => {
    assert.ok(compareValues("", "0") !== 0);
  });
});

describe("sortRows", () => {
  const rows = [{ n: 10 }, { n: 9 }, { n: null }];

  it("returns the original array untouched when unsorted", () => {
    assert.equal(sortRows(rows, null), rows);
  });

  it("does not mutate the fetched rows", () => {
    const before = [...rows];
    sortRows(rows, { column: "n", dir: "asc" });
    assert.deepEqual(rows, before);
  });

  it("sorts ascending and descending with nulls trailing ascending order", () => {
    assert.deepEqual(
      sortRows(rows, { column: "n", dir: "asc" }).map((r) => r.n),
      [9, 10, null],
    );
    assert.deepEqual(
      sortRows(rows, { column: "n", dir: "desc" }).map((r) => r.n),
      [null, 10, 9],
    );
  });
});

describe("filterRows", () => {
  const rows: Record<string, unknown>[] = [
    { id: 1, name: "ada", note: null },
    { id: 2, name: "grace" },
    { id: 3, name: "", note: "x" },
  ];

  it("returns the same reference when no filter is active", () => {
    assert.equal(filterRows(rows, {}), rows);
    assert.equal(filterRows(rows, { name: "  " }), rows);
  });

  it("matches on rendered text case-insensitively", () => {
    assert.deepEqual(
      filterRows(rows, { name: "AD" }).map((r) => r.id),
      [1],
    );
  });

  it("can filter on the NULL / missing / empty markers themselves", () => {
    assert.deepEqual(
      filterRows(rows, { note: "null" }).map((r) => r.id),
      [1],
    );
    assert.deepEqual(
      filterRows(rows, { note: "missing" }).map((r) => r.id),
      [2],
    );
    assert.deepEqual(
      filterRows(rows, { name: "empty" }).map((r) => r.id),
      [3],
    );
  });

  it("ands multiple column filters together", () => {
    assert.deepEqual(
      filterRows(rows, { name: "a", note: "missing" }).map((r) => r.id),
      [2],
    );
    // "ada" has note NULL, not missing — so the conjunction excludes it.
    assert.deepEqual(filterRows(rows, { name: "ada", note: "missing" }), []);
  });
});

describe("nextSort", () => {
  it("cycles asc, desc, then back to result order", () => {
    const a = nextSort(null, "id");
    assert.deepEqual(a, { column: "id", dir: "asc" });
    const b = nextSort(a, "id");
    assert.deepEqual(b, { column: "id", dir: "desc" });
    assert.equal(nextSort(b, "id"), null);
  });

  it("restarts ascending when a different column is clicked", () => {
    assert.deepEqual(nextSort({ column: "id", dir: "desc" }, "name"), {
      column: "name",
      dir: "asc",
    });
  });
});

describe("toCsv", () => {
  it("quotes separators, quotes and newlines", () => {
    const csv = toCsv(["a", "b"], [{ a: 'say "hi", ok', b: "line1\nline2" }]);
    assert.equal(csv, 'a,b\n"say ""hi"", ok","line1\nline2"');
  });

  it("emits an empty field for null and missing alike", () => {
    assert.equal(toCsv(["a", "b"], [{ a: null }]), "a,b\n,");
  });

  it("serialises objects instead of writing [object Object]", () => {
    assert.equal(toCsv(["m"], [{ m: { k: 1 } }]), 'm\n"{""k"":1}"');
  });
});

describe("toMarkdown", () => {
  it("escapes pipes so the table does not break", () => {
    const md = toMarkdown(["a"], [{ a: "x|y" }]);
    assert.equal(md, "| a |\n| --- |\n| x\\|y |");
  });

  it("writes NULL explicitly rather than a blank cell", () => {
    assert.ok(toMarkdown(["a"], [{ a: null }]).endsWith("| NULL |"));
  });
});
