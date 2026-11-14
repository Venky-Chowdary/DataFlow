import { expect, test, type Page } from "@playwright/test";
import { installHermeticStubs } from "./hermetic";

/**
 * Query workspace: the tabs row and the connection bar (Connector, Database /
 * Schema, Table, Row limit) stay pinned under the app top bar while the
 * operator scrolls a long result set, and the results bar (row count, Filter,
 * Copy, Export) stays reachable under it. Hermetic: no API runs.
 */
const COLS = ["id", "customer_email", "status", "order_total", "created_at", "region"];
const ROWS = Array.from({ length: 500 }, (_, i) => ({
  id: i + 1,
  customer_email: `user${i}@example.com`,
  status: ["paid", "open", "refunded"][i % 3],
  order_total: (i * 7.31).toFixed(2),
  created_at: `2026-10-${String((i % 28) + 1).padStart(2, "0")}T10:00:00Z`,
  region: ["emea", "amer", "apac"][i % 3],
}));

async function openQueryWithResults(page: Page): Promise<void> {
  await installHermeticStubs(page);
  const json = (body: unknown) => ({ status: 200, contentType: "application/json", body: JSON.stringify(body) });
  await page.route("**/api/v1/query/execute", (route) =>
    route.fulfill(json({
      columns: COLS, rows: ROWS, row_count: ROWS.length, truncated: false, duration_ms: 42,
      column_schema: Object.fromEntries(COLS.map((c) => [c, "string"])),
    })),
  );
  await page.route("**/api/v1/query/schema", (route) =>
    route.fulfill(json({
      connected: true, connector_type: "postgresql", database: "public", type_source: "connector_introspection",
      objects: Array.from({ length: 15 }, (_, i) => ({
        name: `table_${i}`, type: "table", schema_name: "public", columns: COLS.map((c) => ({ name: c, type: "text" })),
      })),
    })),
  );
  await page.route("**/api/v1/query/schemas", (route) => route.fulfill(json({ connected: true, nodes: [] })));
  await page.goto("/#/transfer");
  const preview = page.locator("button", { hasText: "Preview workspace (UI only)" });
  await expect(preview).toBeAttached();
  await preview.evaluate((node: HTMLButtonElement) => node.click());
  await expect(page.getByRole("button", { name: /Load sample orders CSV/ })).toBeVisible();
  await page.evaluate(() => { window.location.hash = "#/query"; });
  const connector = page.locator("#query-connector");
  await expect(connector).toBeVisible();
  const first = await connector.locator("option").evaluateAll(
    (options) => options.map((o) => (o as HTMLOptionElement).value).filter(Boolean)[0],
  );
  await connector.selectOption(first);
  await page.getByLabel("Query editor").fill("SELECT * FROM orders");
  await page.getByRole("button", { name: "Run all" }).click();
  await expect(page.locator(".df2-qw-grid-count")).toContainText("500 rows");
  // W6 round 2: a plain successful run does not toast over the grid; the run bar says it.
  await page.waitForTimeout(600);
  await expect(page.locator(".dt-toast", { hasText: /^Ran |500 rows in/ })).toHaveCount(0);
  // The connector fixes the dialect: a read-only label, not a select.
  await expect(page.getByRole("combobox", { name: "Query language" })).toHaveCount(0);
  await expect(page.locator(".df2-query-editor-lang-fixed")).toHaveText("PostgreSQL");
}

for (const view of [{ width: 1366, height: 768 }, { width: 390, height: 844 }]) {
  test(`the tabs and connection bar stay pinned over 2,000 px of results (${view.width}x${view.height})`, async ({ browser }) => {
    const context = await browser.newContext({ viewport: view });
    const page = await context.newPage();
    await openQueryWithResults(page);
    const header = page.getByTestId("query-sticky-header");
    const before = await header.boundingBox();
    expect(before).toBeTruthy();

    // Scroll the page and the result grid through 2,000 px of rows.
    await page.evaluate(() => {
      const scroller = document.querySelector<HTMLElement>(".df2-content");
      const grid = document.querySelector<HTMLElement>(".df2-qw-grid-scroll");
      grid?.scrollBy(0, 2000);
      scroller?.scrollBy(0, 2000);
    });
    await expect(header).toHaveAttribute("data-stuck", "true");

    const topBar = await page.locator("header.df2-topbar").first().boundingBox();
    const box = await header.boundingBox();
    expect(box).toBeTruthy();
    // Still on screen, directly under the app top bar, not under it.
    expect(box!.y).toBeGreaterThanOrEqual((topBar?.y ?? 0) + (topBar?.height ?? 0) - 1);
    expect(box!.y).toBeLessThan(view.height / 3);
    await expect(header.getByRole("tablist", { name: "Query tabs" })).toBeInViewport();
    await expect(page.locator("#query-connector")).toBeInViewport();
    await expect(header.getByRole("button", { name: /History/ })).toBeInViewport();

    // Nothing scrolled under the header shows through it, and the results bar stays reachable.
    const bar = await page.locator(".df2-qw-grid-bar").boundingBox();
    expect(bar).toBeTruthy();
    expect(bar!.y).toBeGreaterThanOrEqual(box!.y + box!.height - 2);
    await expect(page.getByRole("button", { name: "Copy CSV" })).toBeInViewport();

    // No page-level sideways scroll at this width.
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
    expect(overflow).toBeLessThanOrEqual(0);
    await context.close();
  });
}
