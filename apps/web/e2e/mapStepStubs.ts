import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import type { Page } from "@playwright/test";
import { installHermeticStubs } from "./hermetic";

/**
 * Hermetic stubs for the Map step (shared by map-step.spec.ts and
 * studio-chrome-map-step.spec.ts). The destination is a Postgres table
 * `orders_dest` whose columns are named differently from the sample orders
 * CSV; the catalog, type grades and auto-map fixtures in ./fixtures/map-step
 * were produced by the API's own builders.
 */
const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), "fixtures", "map-step");
export const fixture = (name: string) => readFileSync(join(FIXTURES, name), "utf8");

export interface Captured {
  autoMap: Array<Record<string, unknown>>;
  memory: Array<Record<string, unknown>>;
}

export async function stubMapStudio(page: Page): Promise<Captured> {
  const captured: Captured = { autoMap: [], memory: [] };
  await installHermeticStubs(page);
  const json = (body: string) => ({ status: 200, contentType: "application/json", body });
  await page.route("**/api/v1/transfer/introspect", (route) => route.fulfill(json(fixture("transfer_introspect.json"))));
  await page.route("**/api/v1/transfer/mapping/destination-columns", (route) =>
    route.fulfill(json(fixture("destination_columns.json"))),
  );
  await page.route("**/api/v1/transfer/mapping/type-compatibility", async (route) => {
    const graded = JSON.parse(fixture("type_compatibility.json")).results as Array<Record<string, unknown>>;
    const pairs = (route.request().postDataJSON().pairs ?? []) as Array<Record<string, string>>;
    if (process.env.MAP_STEP_DEBUG) console.log("PAIRS", JSON.stringify(pairs));
    const results = pairs.map(
      (p) =>
        graded.find(
          (g) => g.source_type === p.source_type && g.target_type === p.target_type
            && g.transform === (p.transform || "none"),
        ) ?? {
          source_type: p.source_type, target_type: p.target_type, verdict: "unknown",
          conversion_class: "", reasons: [], requires_risk_contract: false,
        },
    );
    await route.fulfill(json(JSON.stringify({ results })));
  });
  await page.route("**/api/v1/transfer/mapping/auto-map", async (route) => {
    captured.autoMap.push(route.request().postDataJSON());
    await route.fulfill(json(fixture("auto_map.json")));
  });
  await page.route("**/api/v1/shape/expression", async (route) => {
    const expression = String(route.request().postDataJSON().expression ?? "");
    await route.fulfill(json(JSON.stringify(
      /surname/.test(expression)
        ? { valid: false, error: "unknown column 'surname'" }
        : { valid: true, columns: ["customer_email", "status"] },
    )));
  });
  await page.route("**/api/v1/transfer/mapping/export", async (route) => {
    await route.fulfill({
      status: 200,
      contentType: "application/yaml",
      headers: { "Content-Disposition": 'attachment; filename="sample-orders-orders_dest.mapping.yaml"' },
      body: "apiVersion: datawrap.io/mapping/v1\nkind: ColumnMapping\n",
    });
  });
  await page.route("**/api/v1/transfer/mapping/memory", async (route) => {
    captured.memory.push(route.request().postDataJSON());
    await route.fulfill(json('{"recorded":1,"total":1}'));
  });
  return captured;
}

