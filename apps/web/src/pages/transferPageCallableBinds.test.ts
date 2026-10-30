import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

/**
 * QA (demo videos): `WHERE amount >= :min_amount` with the Bound value set to
 * 100 failed Preview with "A value is required for bind parameter
 * 'min_amount'". The Preview callback (`introspectOneStream`) read
 * `procedureParams` but left it out of its hook dependencies, so it kept the
 * empty binds from the render where the query was typed and sent the query
 * without the value the operator entered afterwards. Every hook in the page
 * that reads the binds must re-create when they change.
 */
const page = readFileSync(
  join(dirname(fileURLToPath(import.meta.url)), "TransferPage.tsx"),
  "utf8",
);

function hookBodies(source: string): Array<{ line: number; body: string; deps: string }> {
  const out: Array<{ line: number; body: string; deps: string }> = [];
  const re = /\buse(?:Callback|Memo|Effect)\(/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(source))) {
    let i = source.indexOf("(", m.index);
    let depth = 0;
    for (; i < source.length; i += 1) {
      const c = source[i];
      if (c === "(") depth += 1;
      else if (c === ")") {
        depth -= 1;
        if (depth === 0) break;
      }
    }
    const text = source.slice(m.index, i);
    const k = text.lastIndexOf(", [");
    if (k < 0) continue;
    out.push({
      line: source.slice(0, m.index).split("\n").length,
      body: text.slice(0, k),
      deps: text.slice(k),
    });
  }
  return out;
}

describe("TransferPage callable binds", () => {
  it("re-creates every hook that reads the bound values when they change", () => {
    const readers = hookBodies(page).filter((h) => /\bprocedureParams\b/.test(h.body));
    assert.ok(readers.length > 0, "expected hooks that read procedureParams");
    const stale = readers
      .filter((h) => !/\bprocedureParams\b/.test(h.deps))
      .map((h) => `TransferPage.tsx:${h.line}`);
    assert.deepEqual(stale, []);
  });
});
