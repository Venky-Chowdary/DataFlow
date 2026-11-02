import assert from "node:assert/strict";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const SRC = join(dirname(fileURLToPath(import.meta.url)), "..");

function tsxFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return tsxFiles(path);
    return path.endsWith(".tsx") && !path.includes(".test.") ? [path] : [];
  });
}

describe("filenames are never case-transformed", () => {
  it("the filename badge opts out of the badge's capitalize", () => {
    const css = readFileSync(join(SRC, "styles", "dataflow-ui.css"), "utf8");
    assert.match(css, /\.df2-badge\s*\{[^}]*text-transform:\s*capitalize/);
    assert.match(css, /\.df2-badge\.df2-badge-filename\s*\{\s*text-transform:\s*none;\s*\}/);
  });

  it("every df2-badge that renders a file name carries df2-badge-filename", () => {
    const offenders: string[] = [];
    for (const path of tsxFiles(SRC)) {
      const lines = readFileSync(path, "utf8").split("\n");
      lines.forEach((line, i) => {
        if (/className="[^"]*\bdf2-badge\b/.test(line) && /\{[\w.?]*\b(name|fileName|filename|file_name)\}/.test(line) && /\bfile/i.test(line)) {
          if (!/df2-badge-filename/.test(line)) offenders.push(`${path}:${i + 1}`);
        }
      });
    }
    assert.deepEqual(offenders, []);
  });
});
