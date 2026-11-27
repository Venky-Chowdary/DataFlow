import assert from "node:assert/strict";
import { test } from "node:test";
import { plural, pluralWord } from "./plural";

test("counts a noun without (s)", () => {
  assert.equal(plural(1, "table"), "1 table");
  assert.equal(plural(3, "table"), "3 tables");
  assert.equal(plural(0, "table"), "0 tables");
  assert.equal(plural(-1, "row"), "-1 row");
});

test("groups large counts and handles regular and irregular plurals", () => {
  assert.equal(plural(1200000, "row"), "1,200,000 rows");
  assert.equal(plural(2, "entry"), "2 entries");
  assert.equal(plural(2, "key"), "2 keys");
  assert.equal(plural(2, "match"), "2 matches");
  assert.equal(plural(2, "index", "indexes"), "2 indexes");
  assert.equal(plural(2, "person", "people"), "2 people");
});

test("pluralWord returns only the word", () => {
  assert.equal(pluralWord(1, "column"), "column");
  assert.equal(pluralWord(5, "column"), "columns");
});
