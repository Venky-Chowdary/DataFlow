import { createRequire } from "node:module";
import { readFileSync } from "node:fs";

import { expect, test, type Page } from "@playwright/test";
import { installHermeticStubs } from "./hermetic";
import { clippedBox, overlapArea, type Box } from "./layout";

/**
 * Query workspace layout and states, at 1440 / 1366 / 1024 / 390 in both
 * themes. Hermetic: the API is stubbed with a realistic workspace (three
 * connectors, 46 objects with long names, a 25-column x 500-row result with
 * NULLs, long text, JSON, numbers and dates, a taxonomy error, an empty result
 * and a slow query).
 *
 * Every check is a soft assertion so one run reports every defect at once.
 */

const require = createRequire(import.meta.url);
const AXE_SOURCE = readFileSync(process.env.AXE_CORE_PATH || require.resolve("axe-core/axe.min.js"), "utf8");
const WCAG_TAGS = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];

const COLUMNS: Array<[string, string]> = [
  ["order_id", "INTEGER"], ["customer_email", "TEXT"], ["status", "TEXT"], ["order_total", "NUMERIC"],
  ["discount_pct", "REAL"], ["currency", "TEXT"], ["created_at", "TIMESTAMP"], ["shipped_at", "TIMESTAMP"],
  ["ship_to_country", "TEXT"], ["ship_to_postal_code", "TEXT"], ["notes", "TEXT"], ["payload_json", "JSON"],
  ["is_gift", "BOOLEAN"], ["items_count", "INTEGER"], ["warehouse_code", "TEXT"], ["carrier", "TEXT"],
  ["tracking_number", "TEXT"], ["refund_amount", "NUMERIC"], ["tax_amount", "NUMERIC"], ["customer_segment", "TEXT"],
  ["channel", "TEXT"], ["utm_campaign_with_a_very_long_column_name", "TEXT"], ["sku_list", "TEXT"],
  ["fulfilled_by", "TEXT"], ["last_modified_by", "TEXT"],
];
const LONG_NOTE = "Customer asked for the package to be left with the concierge at the front desk; call ahead if delayed. ".repeat(3);
const ROWS = Array.from({ length: 500 }, (_, k) => {
  const i = k + 1;
  return {
    order_id: i,
    customer_email: `customer${i}@example-enterprise-domain.com`,
    status: ["paid", "open", "refunded", "shipped"][i % 4],
    order_total: Math.round(i * 731) / 100,
    discount_pct: i % 5 === 0 ? null : (i * 7) % 30,
    currency: ["USD", "EUR", "GBP"][i % 3],
    created_at: `2026-10-${String((i % 28) + 1).padStart(2, "0")}T10:00:00Z`,
    shipped_at: i % 3 === 0 ? null : `2026-10-${String((i % 28) + 1).padStart(2, "0")}T18:00:00Z`,
    ship_to_country: ["US", "DE", "GB", "IN", "JP"][i % 5],
    ship_to_postal_code: String(10000 + i),
    notes: i % 7 === 0 ? LONG_NOTE : i % 11 === 0 ? "" : null,
    payload_json: { source: "web", items: [{ sku: `SKU-${i}`, qty: (i % 4) + 1 }] },
    is_gift: i % 2 === 0,
    items_count: (i % 6) + 1,
    warehouse_code: `WH-${i % 9}`,
    carrier: ["UPS", "DHL", "FedEx", null][i % 4],
    tracking_number: `1Z${String(i).padStart(10, "0")}`,
    refund_amount: i % 4 ? null : i / 2,
    tax_amount: Math.round(i * 62) / 100,
    customer_segment: ["enterprise", "smb", "mid-market"][i % 3],
    channel: ["web", "mobile", "partner"][i % 3],
    utm_campaign_with_a_very_long_column_name: `autumn_promo_${i % 5}`,
    sku_list: `SKU-${i},SKU-${i + 1}`,
    fulfilled_by: "warehouse-team",
    last_modified_by: i % 2 ? null : "etl_service_account",
  };
});
const OBJECT_NAMES = [
  "customer_lifetime_value_by_acquisition_channel_and_region_monthly",
  "fact_marketing_attribution_multi_touch_weighted_daily",
  "stg_salesforce_opportunity_line_items",
  ...Array.from({ length: 42 }, (_, i) => `table_${String(i).padStart(2, "0")}_${["orders", "events", "ledger"][i % 3]}`),
  "wide_orders",
];

const REFERENCE = "176183fa-cd16-464b-aa3f-9139bb9ea740";

async function stubQueryApi(page: Page): Promise<void> {
  const json = (body: unknown, status = 200) => ({ status, contentType: "application/json", body: JSON.stringify(body) });
  await page.route("**/api/v1/query/schemas", (route) => route.fulfill(json({ connected: true, nodes: [], listed_live: true })));
  await page.route("**/api/v1/query/schema", async (route) => {
    const body = route.request().postDataJSON() as { object_name?: string };
    if (body.object_name) {
      return route.fulfill(json({
        connected: true, connector_type: "postgresql", type_source: "connector_introspection",
        objects: [{ name: body.object_name, type: "table", schema_name: "public", columns: COLUMNS.map(([name, type]) => ({ name, type })) }],
      }));
    }
    return route.fulfill(json({
      connected: true, connector_type: "postgresql", database: "public", type_source: "connector_introspection",
      objects: OBJECT_NAMES.map((name) => ({ name, type: "table", schema_name: "public", columns: [] })),
    }));
  });
  await page.route("**/api/v1/query/execute", async (route) => {
    const body = route.request().postDataJSON() as { query: string; limit: number };
    if (/missing_table/.test(body.query)) {
      return route.fulfill(json({
        error: {
          code: "object.table_not_found", title: "Table not found", message: 'Table "missing_table" does not exist.',
          action: { label: "Check the table name in the query.", ui: "edit_query", api: "" },
          object: { table: "missing_table", side: "query" }, retriable: false, reference: REFERENCE,
          driver: { engine: "postgresql", code: "42P01" }, legacy_code: "table_not_found",
        },
        detail: `Table "missing_table" does not exist. Check the table name in the query. Reference: ${REFERENCE}`,
      }, 400));
    }
    if (/1 = 0/.test(body.query)) {
      return route.fulfill(json({
        columns: ["id", "name"], rows: [], row_count: 0, truncated: false, duration_ms: 12,
        column_schema: { id: "INTEGER", name: "TEXT" }, column_type_source: "driver",
      }));
    }
    if (/pg_sleep/.test(body.query)) {
      await new Promise((resolve) => setTimeout(resolve, 20_000));
      return route.fulfill(json({ columns: ["x"], rows: [{ x: 1 }], row_count: 1, truncated: false, duration_ms: 20_000 })).catch(() => {});
    }
    const rows = ROWS.slice(0, body.limit);
    return route.fulfill(json({
      columns: COLUMNS.map(([name]) => name), rows, row_count: rows.length, truncated: rows.length >= body.limit,
      duration_ms: 148, column_schema: Object.fromEntries(COLUMNS), column_type_source: "driver",
    }));
  });
}

async function openQuery(page: Page, theme: "light" | "dark"): Promise<void> {
  await page.addInitScript((value) => {
    localStorage.setItem("df2.theme", value);
  }, theme);
  await page.emulateMedia({ reducedMotion: "reduce", colorScheme: theme });
  await installHermeticStubs(page);
  await stubQueryApi(page);
  await page.goto("/#/transfer");
  const preview = page.locator("button", { hasText: "Preview workspace (UI only)" });
  await expect(preview).toBeAttached();
  await preview.evaluate((node: HTMLButtonElement) => node.click());
  await expect(page.getByRole("button", { name: /Load sample orders CSV/ })).toBeVisible();
  // The dev-preview notice is the shell's, not the Query page's: dismiss it.
  await page.getByRole("button", { name: "Dismiss notification" }).first().click({ timeout: 2_000 }).catch(() => {});
  await page.evaluate(() => { window.location.hash = "#/query"; });
  const connector = page.locator("#query-connector");
  await expect(connector).toBeAttached();
  await expect(connector.locator("option:not([value=''])").first()).toBeAttached();
  const first = await connector.locator("option").evaluateAll(
    (options) => options.map((o) => (o as HTMLOptionElement).value).filter(Boolean)[0],
  );
  await connector.selectOption(first);
}

async function axeBlocking(page: Page): Promise<string[]> {
  await page.addScriptTag({ content: AXE_SOURCE });
  return page.evaluate(async (tags) => {
    const axe = (window as unknown as { axe: { run: (ctx: unknown, opts: unknown) => Promise<{ violations: Array<{ id: string; impact: string | null; nodes: Array<{ target: string[] }> }> }> } }).axe;
    const result = await axe.run(document.querySelector(".df2-page-query") ?? document, { runOnly: { type: "tag", values: tags } });
    return result.violations
      .filter((v) => v.impact === "critical" || v.impact === "serious")
      .map((v) => `${v.impact} ${v.id}: ${v.nodes.slice(0, 3).map((n) => n.target.join(" ")).join(" | ")}`);
  }, WCAG_TAGS);
}

/** Elements inside `root` (itself included) that scroll vertically right now. */
async function verticalScrollers(page: Page, testId: string): Promise<number> {
  return page.evaluate((id) => {
    const root = document.querySelector(`[data-testid="${id}"]`);
    if (!root) return -1;
    return [root, ...root.querySelectorAll("*")].filter((el) => {
      const style = getComputedStyle(el);
      return /(auto|scroll)/.test(style.overflowY) && el.scrollHeight > el.clientHeight + 1;
    }).length;
  }, testId);
}

async function boxOf(page: Page, testId: string): Promise<Box | null> {
  const loc = page.getByTestId(testId);
  if ((await loc.count()) === 0) return null;
  return loc.first().boundingBox();
}

const VIEWS = [
  { width: 1440, height: 900 },
  { width: 1366, height: 768 },
  { width: 1024, height: 768 },
  { width: 390, height: 844 },
];

for (const view of VIEWS) {
  for (const theme of ["light", "dark"] as const) {
    test(`query workspace layout and states at ${view.width}x${view.height} (${theme})`, async ({ browser }) => {
      test.setTimeout(150_000);
      const context = await browser.newContext({ viewport: view });
      const page = await context.newPage();
      const desktop = view.width >= 1024;
      await openQuery(page, theme);
      await expect(page.locator(".df2-qw-node").first()).toBeVisible({ timeout: desktop ? 20_000 : 1 }).catch(() => {});

      // --- Connector picker: the engine is named once, and a long label ends in "…".
      const repeats = await page.locator("#query-connector option").evaluateAll((options) =>
        options
          .map((o) => o.textContent ?? "")
          .filter((text) => ["PostgreSQL", "Snowflake"].some((engine) => (text.toLowerCase().match(new RegExp(engine.toLowerCase(), "g")) ?? []).length > 1)),
      );
      expect.soft(repeats, "no option repeats the engine its name already says").toEqual([]);
      if (desktop) {
        const picker = await page.locator("#query-connector").evaluate((el) => {
          const style = getComputedStyle(el);
          return { overflow: style.textOverflow, clip: style.overflowX, nowrap: style.whiteSpace, title: el.getAttribute("title") ?? "" };
        });
        expect.soft(picker.overflow, "a long connector label ends in an ellipsis").toBe("ellipsis");
        expect.soft(picker.nowrap, "the connector label stays on one line").toBe("nowrap");
        expect.soft(picker.title, "the full label is the picker's title").toMatch(/\S/);
      } else {
        // Phones: every connection field visible inside the card; nothing scrolls sideways.
        const bar = page.getByTestId("query-action-bar");
        const barBox = await bar.boundingBox({ timeout: 2_000 }).catch(() => null);
        const sideways = await bar.evaluate((el) => {
          const scrolls = [el, ...el.querySelectorAll("*")].filter((n) => n.scrollWidth > n.clientWidth + 1 && /(auto|scroll)/.test(getComputedStyle(n).overflowX));
          return scrolls.length;
        }, undefined, { timeout: 2_000 }).catch(() => -1);
        expect.soft(sideways, "phone: no sideways scroll in the connection area").toBe(0);
        for (const id of ["#query-connector", "#query-database", "#query-collection", "#query-row-limit"]) {
          const box = await page.locator(id).boundingBox({ timeout: 2_000 }).catch(() => null);
          const visible = box ? await clippedBox(page, id).catch(() => null) : null;
          const inside = Boolean(box && barBox && box.x >= barBox.x - 1 && box.x + box.width <= barBox.x + barBox.width + 1 && box.x + box.width <= view.width);
          expect.soft(inside, `phone: ${id} sits fully inside the card`).toBe(true);
          expect.soft(Boolean(box && visible && Math.abs(visible.width - box.width) <= 1), `phone: ${id} is not clipped`).toBe(true);
        }
      }

      // --- Run: one primary button, reachable, with the shortcut it honours.
      const run = page.getByTestId("query-run");
      await expect.soft(run, "Run button present and on screen").toBeInViewport({ timeout: 2_000 });
      await expect.soft(run, "Run names its shortcut").toHaveAttribute("aria-keyshortcuts", /Control\+Enter/, { timeout: 1_000 });

      // A 500-row limit, so the 500-row answer is cut short and must say so.
      const limitSet = await page.locator("#query-row-limit").selectOption("500", { timeout: 5_000 }).then(() => true, () => false);
      expect.soft(limitSet, "Row limit can be operated").toBe(true);
      if (!limitSet) await page.locator("#query-row-limit").evaluate((el: HTMLSelectElement) => {
        el.value = "500";
        el.dispatchEvent(new Event("change", { bubbles: true }));
      });

      // --- Keyboard: type with the completion popup open, then Ctrl+Enter runs.
      const editor = page.getByLabel("Query editor");
      await editor.click();
      await page.keyboard.type("SELECT * FROM wide_orders");
      await page.keyboard.press("Control+Enter");
      const status = page.getByTestId("query-results-status");
      await expect.soft(status, "Ctrl+Enter ran the query (status line shows the rows)").toContainText("500 rows", { timeout: 15_000 });
      if (!(await page.locator(".df2-qw-grid-count").isVisible())) {
        // Old page: keep going so every other defect is counted too.
        await page.keyboard.press("Escape");
        await page.getByRole("button", { name: "Run all" }).click();
        await expect(page.locator(".df2-qw-grid-count")).toContainText("500 rows");
      }
      if (!desktop) {
        // Phones: the first result rows are on screen after a run, without scrolling.
        await page.evaluate(() => document.querySelector(".df2-content")?.scrollTo(0, 0));
        const firstRow = await page.locator(".df2-qw-grid-scroll tbody tr").first().boundingBox({ timeout: 5_000 }).catch(() => null);
        expect.soft(Boolean(firstRow && firstRow.y + firstRow.height <= view.height), "phone: the first result row is visible after a run").toBe(true);
      }
      await expect.soft(status, "status says the row limit cut the result").toContainText(/limited to 500 rows/i, { timeout: 1_000 });
      await expect.soft(status, "status shows elapsed time").toContainText(/\d+ ms/, { timeout: 1_000 });

      // --- Layout: no page scroll at desktop sizes, one scroller per pane, no overlaps.
      const pageOverflow = await page.evaluate(() => {
        const content = document.querySelector<HTMLElement>(".df2-content");
        const de = document.documentElement;
        return {
          y: content ? content.scrollHeight - content.clientHeight : de.scrollHeight - de.clientHeight,
          x: de.scrollWidth - de.clientWidth,
        };
      });
      expect.soft(pageOverflow.x, "no sideways page scroll").toBeLessThanOrEqual(0);
      if (desktop) {
        expect.soft(pageOverflow.y, "the page itself does not scroll at >=1024").toBeLessThanOrEqual(1);
        for (const pane of ["query-pane-schema", "query-pane-editor", "query-pane-results"]) {
          const n = await verticalScrollers(page, pane);
          expect.soft(n, `${pane} exists`).not.toBe(-1);
          expect.soft(n, `${pane} has at most one scroll region`).toBeLessThanOrEqual(1);
        }
        const boxes = await Promise.all(
          ["query-action-bar", "query-pane-schema", "query-pane-editor", "query-pane-results"].map((id) => boxOf(page, id)),
        );
        expect.soft(boxes.every(Boolean), "action bar and three panes all render").toBe(true);
        let overlaps = 0;
        for (let i = 0; i < boxes.length; i += 1) {
          for (let j = i + 1; j < boxes.length; j += 1) {
            if (boxes[i] && boxes[j] && overlapArea(boxes[i]!, boxes[j]!) > 0) overlaps += 1;
          }
        }
        expect.soft(overlaps, "panes do not overlap").toBe(0);
        const results = await boxOf(page, "query-pane-results");
        expect.soft(results && results.y + results.height <= view.height + 1, "results pane ends inside the viewport").toBe(true);
        expect.soft(results && results.height >= 160, "results pane has room for rows").toBe(true);
      } else {
        const toggle = page.getByTestId("query-schema-toggle");
        await expect.soft(toggle, "phone: a schema button opens the browser").toBeVisible({ timeout: 1_000 });
        if (await toggle.isVisible()) {
          await toggle.click();
          const drawer = page.getByTestId("query-schema-drawer");
          await expect.soft(drawer, "phone: the schema browser opens as a drawer").toBeInViewport();
          await page.keyboard.press("Escape");
          await expect.soft(drawer, "phone: Escape closes the drawer").toHaveCount(0);
        }
      }

      // --- Results grid: sticky header, numeric right, NULL distinct, truncation.
      const scroller = page.locator(".df2-qw-grid-scroll");
      await scroller.evaluate((el) => el.scrollBy(0, 2_000));
      const headPinned = await scroller.evaluate((el) => {
        const th = el.querySelector("thead th");
        if (!th) return false;
        return Math.abs(th.getBoundingClientRect().top - el.getBoundingClientRect().top) <= 2;
      });
      expect.soft(headPinned, "result header stays pinned while rows scroll").toBe(true);
      const align = await page.evaluate(() => {
        const ths = [...document.querySelectorAll(".df2-qw-grid-scroll thead tr:first-child th")];
        const idx = (name: string) => ths.findIndex((th) => th.textContent?.includes(name));
        const row = document.querySelector(".df2-qw-grid-scroll tbody tr");
        const cell = (i: number) => (row ? (row.children[i] as HTMLElement) : null);
        const num = cell(idx("order_total"));
        const txt = cell(idx("customer_email"));
        return {
          num: num ? getComputedStyle(num).textAlign : "",
          txt: txt ? getComputedStyle(txt).textAlign : "",
        };
      });
      expect.soft(align.num, "numbers align right").toBe("right");
      expect.soft(["left", "start"]).toContain(align.txt);
      const nullLook = await page.evaluate(() => {
        const nul = document.querySelector<HTMLElement>('.df2-qw-grid-scroll td[data-cell="null"] .df2-qw-cell');
        const val = document.querySelector<HTMLElement>('.df2-qw-grid-scroll td[data-cell="value"] .df2-qw-cell');
        if (!nul || !val) return { distinct: false };
        const a = getComputedStyle(nul);
        const b = getComputedStyle(val);
        return { distinct: a.backgroundColor !== b.backgroundColor || a.color !== b.color };
      });
      expect.soft(nullLook.distinct, "NULL looks different from a value").toBe(true);

      if (desktop) {
        const axe = await axeBlocking(page);
        expect.soft(axe, "axe: no serious/critical violations with results").toEqual([]);
      }

      // --- Error: taxonomy copy with a reference, in the page (not only a toast).
      await editor.fill("SELECT * FROM missing_table");
      await page.keyboard.press("Escape");
      await page.getByRole("button", { name: "Run all" }).click();
      const error = page.getByTestId("query-error");
      await expect.soft(error, "error panel shows").toBeVisible({ timeout: 15_000 });
      await expect.soft(error, "error says what failed").toContainText("Table not found", { timeout: 1_000 });
      await expect.soft(error, "error names the next action").toContainText("Check the table name in the query.", { timeout: 1_000 });
      await expect.soft(error, "error carries the support reference").toContainText(REFERENCE, { timeout: 1_000 });
      await expect.soft(error, "no driver code in the copy").not.toContainText("42P01", { timeout: 1_000 });
      if (desktop) {
        const axe = await axeBlocking(page);
        expect.soft(axe, "axe: no serious/critical violations with an error").toEqual([]);
      }

      // --- Zero rows.
      await editor.fill("SELECT * FROM t WHERE 1 = 0");
      await page.keyboard.press("Escape");
      await page.getByRole("button", { name: "Run all" }).click();
      await expect.soft(page.getByTestId("query-zero-rows"), "zero rows says the query ran").toContainText(/Query ran.*0 rows/, { timeout: 15_000 });

      // --- Running: elapsed time, Stop, and a live announcement.
      await editor.fill("SELECT pg_sleep(30)");
      await page.keyboard.press("Escape");
      await page.getByRole("button", { name: "Run all" }).click();
      const running = page.getByTestId("query-run-status");
      await expect.soft(running, "running state shows elapsed time").toContainText(/Running.*\d+(\.\d)?\s?s/, { timeout: 10_000 });
      const stop = page.getByTestId("query-stop");
      await expect.soft(stop, "Stop is offered while running").toBeVisible({ timeout: 1_000 });
      if (await stop.isVisible()) {
        await stop.focus();
        await page.keyboard.press("Enter");
        await expect.soft(page.getByTestId("query-live"), "the stop is announced").toContainText(/stopped/i, { timeout: 2_000 });
        await expect.soft(page.getByTestId("query-run"), "Run is back after Stop").toBeEnabled({ timeout: 2_000 });
      }
      await context.close();
    });
  }
}
