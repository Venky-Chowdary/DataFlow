import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";
import { catalogFromApi, mapTargetIssues, summarizeMapTargets } from "./destColumnCatalog";
import type { EditableMapping } from "./mapping";

/**
 * Review M6: the Map step holds exactly what Validate blocks. The fixture's
 * catalogs and expected holds are proven against G14 and the DDL gate by
 * apps/api/tests/test_map_hold_parity.py.
 */
const FIXTURE = join(dirname(fileURLToPath(import.meta.url)), "__fixtures__", "map_hold_parity.json");
const { cases } = JSON.parse(readFileSync(FIXTURE, "utf8")) as {
  cases: Array<{
    name: string;
    catalog: Record<string, unknown>;
    table_recreated: boolean;
    mappings: Array<{ source: string; target: string }>;
    expected: string[];
  }>;
};

describe("Map holds = server gates (parity fixture)", () => {
  for (const c of cases) {
    it(c.name, () => {
      const catalog = catalogFromApi(c.catalog);
      const rows: EditableMapping[] = c.mappings.map((m) => ({
        source: m.source, target: m.target, confidence: 0.99, approved: true, transform: "none",
      }));
      const codes = new Set(
        mapTargetIssues(summarizeMapTargets(catalog, rows), rows, { tableRecreated: c.table_recreated }).map((i) => i.code),
      );
      assert.deepEqual([...codes].sort(), [...c.expected].sort());
    });
  }
});
