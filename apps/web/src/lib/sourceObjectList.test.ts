/**
 * Run: npx --yes tsx --test apps/web/src/lib/sourceObjectList.test.ts
 */
import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { appendSourceObject } from "./sourceObjectList.js";

describe("appendSourceObject", () => {
  it("starts a list, appends a second name, and ignores a duplicate", () => {
    assert.equal(appendSourceObject("", "orders"), "orders");
    assert.equal(appendSourceObject("orders", "customers"), "orders, customers");
    assert.equal(appendSourceObject("orders, customers", "orders"), "orders, customers");
  });

  it("ignores a blank name and surrounding whitespace", () => {
    assert.equal(appendSourceObject("orders", "  "), "orders");
    assert.equal(appendSourceObject(" orders , customers ", "payments"), "orders, customers, payments");
  });
});
