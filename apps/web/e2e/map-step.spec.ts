import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test, type Browser, type BrowserContext, type Page } from "@playwright/test";
import { fixture, stubMapStudio } from "./mapStepStubs";
import { expectStudioShellClear, mapStepLayoutReport, type MapStepLayoutReport } from "./layout";
import { openStudio, settleToasts } from "./support";

/**
 * Map step: remap source columns onto an EXISTING destination table's own
 * columns — picker, destination panel, drag and drop (pointer and keyboard),
 * live type grades, auto-map preview, undo/redo.
 *
 * Hermetic like the rest of the suite: no API runs. The destination is a
 * Postgres table `orders_dest` whose columns are named differently from the
 * sample orders CSV. The catalog, type grades and auto-map fixtures in
 * ./fixtures/map-step were produced by the API's own builders
 * (services.mapping_dest_catalog / mapping_type_compat / mapping_name_match).
 */
const SHOTS = process.env.MAP_STEP_SHOTS ?? "";

async function reachMap(page: Page): Promise<void> {
  await openStudio(page);
  // Toasts dock in-flow in the studio header (D-BP-20): one arriving or leaving
  // moves everything under it, so a click aimed at a footer button can land a
  // few pixels off. Keep them out of the layout for the whole walk.
  await page.addStyleTag({ content: ".dt-toast, .dt-toast-more { display: none !important; }" });
  await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
  await page.getByRole("button", { name: /Continue to Destination/ }).click();
  await page.getByRole("radio", { name: /QA Postgres Local/ }).click();
  await page.locator("#dest-col").fill("orders_dest");
  await page.locator("#dest-col").press("Enter");
  // Editing the table re-reads the destination 350 ms later. That read used to
  // disable Continue, and a click landing as it started was dropped (the W7
  // "Continue without transforming" time-outs). Continue now stays enabled and
  // waits for the read; the re-read spec at the end of this file pins the race.
  await page.locator("#dest-col").blur();
  await page.waitForLoadState("networkidle");
  const toTransform = page.getByRole("button", { name: /Continue to Transform/ });
  await expect(toTransform).toBeEnabled();
  await toTransform.click();
  await page.getByRole("button", { name: /Continue without transforming/ }).click();
  await expect(page.getByRole("toolbar", { name: "Mapping actions" })).toBeVisible();
  await settleToasts(page);
}

async function expectTheme(page: Page, theme: "light" | "dark"): Promise<void> {
  if (theme === "dark") await expect(page.locator("html")).toHaveClass(/df2-theme-dark/);
  else await expect(page.locator("html")).not.toHaveClass(/df2-theme-dark/);
}

const panel = (page: Page) => page.getByRole("complementary", { name: /Destination columns of/ });
const destRow = (page: Page, name: string) => panel(page).locator(`[data-dest-column="${name}"]`);
const picker = (page: Page, source: string) => page.getByRole("combobox", { name: `Destination column for ${source}` });
const grip = (page: Page, source: string) => page.getByRole("button", { name: `Move ${source} to a destination column` });
const live = (page: Page) => page.getByTestId("map-live-region");

async function showAllRows(page: Page): Promise<void> {
  const expand = page.getByRole("button", { name: "Expand columns" });
  if (await expand.isVisible()) await expand.click();
  await expect(picker(page, "order_id")).toBeVisible();
}

test.describe("Map step: remap onto existing destination columns", () => {
  test("pick, drag, keyboard-drag, auto-map, undo — against the destination's own columns", async ({ page }) => {
    const captured = await stubMapStudio(page);
    await reachMap(page);

    // The destination panel lists every existing column with its facts and fate.
    await expect(panel(page)).toBeVisible();
    await expect(panel(page).getByRole("button", { name: "All 8" })).toBeVisible();
    await expect(destRow(page, "legacy_ref")).toContainText("BIGINT · PK · NOT NULL");
    await expect(destRow(page, "legacy_ref")).toContainText("Required");
    await expect(destRow(page, "tenant_id")).toContainText("Required");
    await expect(destRow(page, "total_amount")).toContainText("new rows get the default (0)");
    await expect(destRow(page, "notes")).toContainText("new rows get NULL");
    // Required + unmapped columns are Validate blockers, enforced here as well.
    await expect(page.getByTestId("map-target-issues")).toContainText("2 destination column issues block Validate");
    await expect(page.getByRole("button", { name: /Continue to Validate/ })).toBeDisabled();

    await showAllRows(page);

    // 1. Picker: the row's combobox lists the existing columns with type, nullability, key.
    await picker(page, "order_id").click();
    const listbox = page.getByRole("listbox", { name: "Destination columns for order_id" });
    await expect(listbox).toBeVisible();
    const legacy = listbox.getByRole("option", { name: /^legacy_ref/ });
    await expect(legacy).toContainText("BIGINT · PK · NOT NULL");
    await expect(listbox.getByRole("option", { name: /^total_amount/ })).toContainText("DECIMAL(12,2) · NOT NULL · default 0");
    await legacy.click();
    await expect(picker(page, "order_id")).toHaveValue("legacy_ref");
    await expect(destRow(page, "legacy_ref")).toContainText("Mapped");
    await expect(destRow(page, "legacy_ref")).toContainText("order_id");
    // Live grade from the API grader: BIGINT → BIGINT.
    const orderRow = page.locator('tr[data-source="order_id"]');
    await expect(orderRow.locator("[data-verdict]")).toHaveAttribute("data-verdict", "safe");

    // 2. Pointer drag: customer_email onto email_address.
    await grip(page, "customer_email").dragTo(destRow(page, "email_address"));
    await expect(picker(page, "customer_email")).toHaveValue("email_address");
    const emailChip = page.locator('tr[data-source="customer_email"] [data-verdict]');
    await expect(emailChip).toHaveAttribute("data-verdict", "lossy");
    await emailChip.focus();
    await expect(page.getByRole("tooltip").filter({ hasText: "VARCHAR → VARCHAR(120)" })).toBeVisible();

    // 3. Keyboard drag: Space picks up, arrows move, Enter drops — announced.
    await grip(page, "status").focus();
    await page.keyboard.press("Space");
    await expect(live(page)).toContainText("Picked up status");
    for (let i = 0; i < 8; i += 1) {
      if (await destRow(page, "order_status").evaluate((n) => n.classList.contains("is-drop-over"))) break;
      await page.keyboard.press("ArrowDown");
    }
    await expect(destRow(page, "order_status")).toHaveClass(/is-drop-over/);
    await expect(live(page)).toContainText("Over order_status");
    await page.keyboard.press("Enter");
    await expect(picker(page, "status")).toHaveValue("order_status");
    await expect(live(page)).toContainText("status now writes order_status");

    // 4. Undo / redo every edit.
    await page.locator("body").click({ position: { x: 5, y: 5 } });
    await page.keyboard.press("Control+z");
    await expect(picker(page, "status")).toHaveValue("status");
    await page.keyboard.press("Control+Shift+z");
    await expect(picker(page, "status")).toHaveValue("order_status");

    // 5. One source per target: taking email_address for order_date unmaps customer_email.
    await picker(page, "order_date").click();
    const claimed = page.getByRole("listbox", { name: "Destination columns for order_date" }).getByRole("option", { name: /^email_address/ });
    await expect(claimed).toContainText("mapped from customer_email");
    await claimed.click();
    await expect(picker(page, "customer_email")).toHaveValue("");
    await expect(live(page)).toContainText("customer_email unmapped from it");
    await page.getByRole("button", { name: "Undo", exact: true }).click();
    await expect(picker(page, "customer_email")).toHaveValue("email_address");
    await expect(picker(page, "order_date")).toHaveValue("order_date");

    // 6. Auto-map with a preview: confidence and match kind before anything changes.
    await page.getByRole("button", { name: "Auto-map…" }).click();
    const dialog = page.getByRole("dialog", { name: "Auto-map columns" });
    await expect(dialog).toBeVisible();
    const amtRow = dialog.getByRole("row", { name: /order_amt/ });
    await expect(amtRow).toContainText("total_amount");
    await expect(amtRow).toContainText("Concept");
    await expect(amtRow).toContainText("78%");
    expect(captured.autoMap[0]?.dest_table).toBe("orders_dest");
    expect((captured.autoMap[0]?.destination as { table?: string })?.table).toBe("orders_dest");
    // A 78% concept match is under the strict 0.85 floor: it starts unselected
    // and is applied only by choice (autoMapPreselected).
    const amtPick = dialog.getByRole("checkbox", { name: "Apply order_amt to total_amount" });
    await expect(amtPick).not.toBeChecked();
    await amtPick.check();
    await dialog.getByRole("button", { name: /^Apply \d+ mappings?$/ }).click();
    await expect(dialog).toBeHidden();
    await expect(picker(page, "order_amt")).toHaveValue("total_amount");

    // tenant_id is still NOT NULL with nothing mapped: Validate stays blocked, by name.
    await expect(page.getByTestId("map-target-issues")).toContainText("tenant_id");
    await expect(page.getByRole("button", { name: /Continue to Validate/ })).toBeDisabled();
    await expect(destRow(page, "tenant_id")).toContainText("Required");
  });

  test("the Map step stays clear of clipping and overlap at laptop, desktop and phone sizes", async ({ browser }) => {
    test.setTimeout(300_000);
    const views = [
      { width: 1366, height: 768 }, { width: 1440, height: 900 }, { width: 390, height: 844 },
    ];
    for (const view of views) {
      for (const theme of ["light", "dark"] as const) {
        // The theme is chosen before the app boots, the way a saved preference
        // applies. Setting data-theme alone afterwards left the html class on
        // light, so "dark" measured a half-light page (W7).
        const context = await browser.newContext({ viewport: view });
        await context.addInitScript((t) => localStorage.setItem("df2.theme", t), theme);
        const page = await context.newPage();
        await stubMapStudio(page);
        await reachMap(page);
        await expectTheme(page, theme);
        if (view.width < 1024) {
          await page.getByRole("button", { name: "Show destination columns" }).click();
        }
        await showAllRows(page);
        // The grid's "Why" text is a deliberate 3-line clamp with a More toggle.
        // Open every one, as a reader would: the check then covers the longest layout.
        const more = page.getByRole("button", { name: /^Show full reason for / });
        for (let i = 0; i < 20 && (await more.count()) > 0; i += 1) {
          await more.first().scrollIntoViewIfNeeded();
          await more.first().click();
        }
        await expectStudioShellClear(page, `map ${view.width}x${view.height} ${theme}`);
        await context.close();
      }
    }
  });

  test("touch: a finger drags a source onto a destination column (tablet)", async ({ browser }) => {
    const context = await browser.newContext({
      viewport: { width: 1180, height: 820 }, hasTouch: true, isMobile: true,
    });
    const page = await context.newPage();
    await stubMapStudio(page);
    await reachMap(page);
    await showAllRows(page);
    // Below 1440 px the destination panel starts closed (W6, UX-23): open it first.
    await page.getByRole("button", { name: "Show destination columns" }).click();
    const row = destRow(page, "email_address");
    await grip(page, "customer_email").scrollIntoViewIfNeeded();
    // Away from the list's edges, where a resting finger scrolls it.
    await row.evaluate((el) => el.scrollIntoView({ block: "center" }));
    const from = await grip(page, "customer_email").boundingBox();
    const to = await row.boundingBox();
    expect(from && to).toBeTruthy();
    // Real touch input: Chromium turns it into pointer events with pointerType "touch".
    const cdp = await context.newCDPSession(page);
    const at = (x: number, y: number) => [{ x: Math.round(x), y: Math.round(y), id: 1 }];
    const start = { x: from!.x + from!.width / 2, y: from!.y + from!.height / 2 };
    const end = { x: to!.x + to!.width / 2, y: to!.y + to!.height / 2 };
    await cdp.send("Input.dispatchTouchEvent", { type: "touchStart", touchPoints: at(start.x, start.y) });
    for (let i = 1; i <= 8; i += 1) {
      await cdp.send("Input.dispatchTouchEvent", {
        type: "touchMove",
        touchPoints: at(start.x + ((end.x - start.x) * i) / 8, start.y + ((end.y - start.y) * i) / 8),
      });
    }
    await expect(row).toHaveClass(/is-drop-over/);
    await cdp.send("Input.dispatchTouchEvent", { type: "touchEnd", touchPoints: [] });
    await expect(picker(page, "customer_email")).toHaveValue("email_address");
    await expect(live(page)).toContainText("customer_email now writes email_address");
    await context.close();
  });

  test("the picker offers 'will create column' for a new name, labelled as such", async ({ page }) => {
    await stubMapStudio(page);
    await reachMap(page);
    await showAllRows(page);
    await picker(page, "order_date").fill("ordered_at");
    const create = page.getByRole("option", { name: /will create column ordered_at/ });
    await expect(create).toBeVisible();
    await page.keyboard.press("ArrowDown");
    await create.click();
    await expect(picker(page, "order_date")).toHaveValue("ordered_at");
    await expect(page.locator('tr[data-source="order_date"]')).toContainText("new column");
  });

  test("a computed column goes to Transform; the mapping exports as a document", async ({ page }) => {
    await stubMapStudio(page);
    await reachMap(page);
    const download = page.waitForEvent("download");
    // Export and Computed column live in the toolbar's More menu (W6).
    await page.getByRole("button", { name: "More mapping actions" }).click();
    await page.getByRole("menuitem", { name: "Export YAML" }).click();
    expect((await download).suggestedFilename()).toBe("sample-orders-orders_dest.mapping.yaml");
    await expect(page.getByTestId("map-code-status")).toContainText("checksum included");

    await page.getByRole("button", { name: "More mapping actions" }).click();
    await page.getByRole("menuitem", { name: "Computed column…" }).click();
    const dialog = page.getByRole("dialog", { name: "Computed destination column" });
    await dialog.getByLabel("Destination column", { exact: true }).fill("notes");
    await dialog.getByRole("textbox", { name: "Expression" }).fill("CONCAT(customer_email, ' / ', status)");
    await expect(dialog.getByRole("status").filter({ hasText: "Valid" })).toBeVisible();
    await dialog.getByRole("button", { name: "Add to Transform" }).click();
    await expect(page.getByRole("heading", { name: "Transform", exact: true })).toBeVisible();
    await expect(page.locator("body")).toContainText("Computed column added to Transform");
  });

  test("captures the Map step for review (light, dark, phone)", async ({ browser }) => {
    test.skip(!SHOTS, "MAP_STEP_SHOTS names the folder for the screenshots");
    test.setTimeout(300_000);
    const views = [
      { tag: "light-1440", width: 1440, height: 900, theme: "light" },
      { tag: "dark-1440", width: 1440, height: 900, theme: "dark" },
      { tag: "light-390", width: 390, height: 844, theme: "light" },
    ] as const;
    for (const view of views) {
      const context = await browser.newContext({
        viewport: { width: view.width, height: view.height },
        deviceScaleFactor: 2,
      });
      const page = await context.newPage();
      await stubMapStudio(page);
      await reachMap(page);
      await page.evaluate((theme) => {
        localStorage.setItem("df2.theme", theme);
        document.documentElement.setAttribute("data-theme", theme);
        document.documentElement.classList.toggle("df2-theme-dark", theme === "dark");
      }, view.theme);
      if (view.width >= 1024) {
        await showAllRows(page);
        await picker(page, "order_id").click();
        await page.getByRole("option", { name: /^legacy_ref/ }).click();
        await grip(page, "customer_email").dragTo(destRow(page, "email_address"));
        await picker(page, "order_amt").click();
        await expect(page.getByRole("listbox")).toBeVisible();
      } else {
        await page.getByRole("button", { name: "Show destination columns" }).click();
        await expect(panel(page)).toBeVisible();
      }
      await page.waitForTimeout(400);
      await page.screenshot({ path: join(SHOTS, `map-step-${view.tag}.png`) });
      if (view.width < 1024) {
        await panel(page).scrollIntoViewIfNeeded();
        await page.waitForTimeout(300);
        await page.screenshot({ path: join(SHOTS, `map-step-${view.tag}-panel.png`) });
      }
      await context.close();
    }
  });
});

/* ---- W7: one clear Map step at every supported size ---------------------- */

const require = createRequire(import.meta.url);
const AXE_SOURCE = readFileSync(process.env.AXE_CORE_PATH || require.resolve("axe-core/axe.min.js"), "utf8");
const HERMETIC = join(dirname(fileURLToPath(import.meta.url)), "fixtures", "hermetic");
const hermetic = (name: string) => readFileSync(join(HERMETIC, name), "utf8");
const W7_VIEWS = [
  { width: 1920, height: 1080 }, { width: 1440, height: 900 }, { width: 1366, height: 768 },
  { width: 1280, height: 720 }, { width: 1024, height: 768 }, { width: 390, height: 844 },
] as const;
type View = { width: number; height: number };

async function openMap(
  browser: Browser,
  view: View,
  theme: "light" | "dark",
  setup?: (page: Page) => Promise<void>,
): Promise<{ context: BrowserContext; page: Page }> {
  const context = await browser.newContext({ viewport: view });
  await context.addInitScript((t) => localStorage.setItem("df2.theme", t), theme);
  const page = await context.newPage();
  await stubMapStudio(page);
  if (setup) await setup(page);
  await reachMap(page);
  await expectTheme(page, theme);
  // Grades and the catalog answer after the step paints; measure the settled step.
  await page.waitForLoadState("networkidle");
  return { context, page };
}

function expectClean(report: MapStepLayoutReport, where: string): void {
  const list = (items: string[]) => items.slice(0, 10).join("\n");
  expect(report.overlaps, `${where}: controls overlap\n${list(report.overlaps)}`).toEqual([]);
  expect(report.clipped, `${where}: controls cut off\n${list(report.clipped)}`).toEqual([]);
  expect(report.spill, `${where}: a control runs out of its column\n${list(report.spill)}`).toEqual([]);
  expect(report.cramped, `${where}: controls closer than 8 px\n${list(report.cramped)}`).toEqual([]);
  expect(report.misaligned, `${where}: a row's first line is not one line\n${list(report.misaligned)}`).toEqual([]);
  expect(report.small, `${where}: targets under 24×24 px\n${list(report.small)}`).toEqual([]);
  expect(report.truncated, `${where}: text cut with no way to read it\n${list(report.truncated)}`).toEqual([]);
  expect(report.primary, `${where}: Continue to Validate is missing`).not.toBeNull();
  expect(report.primary!.inView, `${where}: Continue to Validate is outside the viewport ${JSON.stringify(report.primary)}`).toBe(true);
  expect(report.pageScrollsX, `${where}: the page scrolls sideways`).toBe(false);
}

/** WCAG 2.2 AA contrast and target size, over the Map step only. */
async function mapAxe(page: Page): Promise<string[]> {
  if (!(await page.evaluate(() => "axe" in window))) await page.addScriptTag({ content: AXE_SOURCE });
  // Entrance transitions blend a fading chip with its background; let them end.
  await page.evaluate(async () => {
    const finite = document.getAnimations().filter((a) => Number.isFinite(a.effect?.getComputedTiming().endTime as number));
    await Promise.all(finite.map((a) => a.finished.catch(() => undefined)));
  });
  return page.evaluate(async () => {
    type AxeNode = { target: string[]; any: Array<{ message: string }> };
    type AxeResult = { violations: Array<{ id: string; nodes: AxeNode[] }> };
    const axe = (window as unknown as { axe: { run: (ctx: unknown, opts: unknown) => Promise<AxeResult> } }).axe;
    const result = await axe.run(
      { include: [[".df2-map-step-panel"]] },
      { runOnly: { type: "rule", values: ["color-contrast", "target-size"] } },
    );
    return result.violations.flatMap((v) =>
      v.nodes.slice(0, 6).map((n) => `${v.id}: ${n.target.join(" ")} — ${(n.any[0]?.message ?? "").slice(0, 140)}`),
    );
  });
}

/** The destination table does not exist yet: every mapped column is created. */
async function newDestinationTable(page: Page): Promise<void> {
  const intro = JSON.parse(fixture("transfer_introspect.json"));
  intro.destination = { ...intro.destination, table_exists: false, columns: [], schema: {}, schema_nullability: {},
    objects: [{ name: "customers", type: "table" }] };
  const catalog = { ...JSON.parse(fixture("destination_columns.json")), table_exists: false, columns: [] };
  const json = (body: unknown) => ({ status: 200, contentType: "application/json", body: JSON.stringify(body) });
  await page.route("**/api/v1/transfer/introspect", (route) => route.fulfill(json(intro)));
  await page.route("**/api/v1/transfer/mapping/destination-columns", (route) => route.fulfill(json(catalog)));
}

/**
 * 44 source columns, most with long names, onto a 44-column destination whose
 * two NOT NULL columns have no source. Built from the same captured fixtures:
 * every list or map keyed by the five sample columns is widened to 44.
 */
const BASE_COLUMNS = ["order_id", "customer_email", "order_amt", "order_date", "status"];
const LONG_WORDS = [
  "billing_address_line_two_secondary_unit_designator", "customer_lifetime_value_estimate",
  "shipping_carrier_tracking_reference_number", "promo", "marketing_attribution_campaign_utm_source_medium",
  "tax_jurisdiction_code", "loyalty_programme_tier_effective_from_timestamp", "qty",
];
const WIDE_COLUMNS = Array.from({ length: 44 }, (_, i) =>
  (i < 5 ? BASE_COLUMNS[i] : `${LONG_WORDS[i % LONG_WORDS.length]}_${String(i).padStart(2, "0")}`));

function renamed(value: unknown, from: string, to: string): unknown {
  if (from === to) return value;
  if (typeof value === "string") return value === from ? to : value;
  if (Array.isArray(value)) return value.map((v) => renamed(v, from, to));
  if (value && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, renamed(v, from, to)]));
  }
  return value;
}

function widened(value: unknown): unknown {
  const columnOf = (v: unknown): number => {
    if (typeof v === "string") return BASE_COLUMNS.indexOf(v);
    if (v && typeof v === "object") {
      for (const key of ["source", "column_name", "name", "column"]) {
        const named = (v as Record<string, unknown>)[key];
        if (typeof named === "string" && BASE_COLUMNS.includes(named)) return BASE_COLUMNS.indexOf(named);
      }
    }
    return -1;
  };
  if (Array.isArray(value)) {
    if (value.length > 0 && value.every((v) => columnOf(v) >= 0)) {
      return WIDE_COLUMNS.map((name, i) => {
        const model = value[i % value.length];
        return renamed(widened(model), BASE_COLUMNS[columnOf(model)], name);
      });
    }
    return value.map(widened);
  }
  if (value && typeof value === "object") {
    const record = value as Record<string, unknown>;
    const keys = Object.keys(record);
    const out: Record<string, unknown> = {};
    if (keys.length > 0 && BASE_COLUMNS.every((b) => keys.includes(b))) {
      for (const k of keys) if (!BASE_COLUMNS.includes(k)) out[k] = widened(record[k]);
      WIDE_COLUMNS.forEach((name, i) => { out[name] = widened(record[BASE_COLUMNS[i % 5]]); });
      return out;
    }
    for (const k of keys) out[k] = widened(record[k]);
    return out;
  }
  return value;
}

async function wideSchema(page: Page): Promise<void> {
  const json = (body: unknown) => ({ status: 200, contentType: "application/json", body: JSON.stringify(body) });
  const sources: Array<[string, string]> = [
    ["**/api/v1/connectors/upload", "connectors_upload.json"],
    ["**/api/v1/transfer/analyze", "transfer_analyze.json"],
    ["**/api/v1/ai/analyze/enhanced", "ai_analyze_enhanced.json"],
    ["**/api/v1/transfer/map", "transfer_map.json"],
    ["**/api/v1/shape/profile", "shape_profile.json"],
    ["**/api/v1/shape/preview", "shape_preview.json"],
    ["**/api/v1/transfer/plans/*/mappings*", "plan_mappings.json"],
  ];
  for (const [pattern, name] of sources) {
    const body = widened(JSON.parse(hermetic(name)));
    await page.route(pattern, (route) => route.fulfill(json(body)));
  }
  const catalog = JSON.parse(fixture("destination_columns.json"));
  const nullable = catalog.columns[1];
  const columns = [
    catalog.columns[0], catalog.columns[5],
    ...WIDE_COLUMNS.slice(0, 42).map((name, i) => ({ ...nullable, name: `${name}_dest`, match_key: `${name}_dest`, ordinal: i + 2 })),
  ];
  catalog.columns = columns;
  const intro = JSON.parse(fixture("transfer_introspect.json"));
  intro.destination.columns = columns.map((c: { name: string }) => c.name);
  intro.destination.schema = Object.fromEntries(columns.map((c: { name: string; type: string }) => [c.name, c.type]));
  intro.destination.schema_nullability = Object.fromEntries(
    columns.map((c: { name: string; nullable: boolean }) => [c.name, c.nullable]),
  );
  await page.route("**/api/v1/transfer/introspect", (route) => route.fulfill(json(intro)));
  await page.route("**/api/v1/transfer/mapping/destination-columns", (route) => route.fulfill(json(catalog)));
}

test.describe("Map step layout at every supported size (W7)", () => {
  for (const view of W7_VIEWS) {
    for (const theme of ["light", "dark"] as const) {
      test(`${view.width}x${view.height} ${theme}: no control overlaps, spills, cramps or is cut; rows align; Continue in view`, async ({ browser }) => {
        test.setTimeout(180_000);
        const where = `${view.width}x${view.height} ${theme}`;
        const { context, page } = await openMap(browser, view, theme);

        const opened = await mapStepLayoutReport(page);
        expectClean(opened, `${where} as opened`);
        if (theme === "dark" && view.width >= 640) {
          // The grid's header row follows the theme (it was a white strip in dark).
          const luminance = await page.locator(".df2-column-review-table thead th").first().evaluate((th) => {
            const [r, g, b] = getComputedStyle(th).backgroundColor.match(/\d+(\.\d+)?/g)!.map(Number);
            return (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255;
          });
          expect(luminance, `${where}: the grid header is light on a dark page`).toBeLessThan(0.3);
        }
        if (view.width >= 1280) {
          // About eight rows fit at 1440×900: one compact line per row.
          expect(opened.rowHeights.length, `${where}: no mapping rows on screen`).toBeGreaterThan(0);
          for (const h of opened.rowHeights) expect(h, `${where}: a mapping row is ${h}px tall`).toBeLessThanOrEqual(56);
        }

        await page.getByRole("toolbar", { name: "Mapping actions" })
          .getByRole("button", { name: /destination columns$/ }).click();
        expectClean(await mapStepLayoutReport(page), `${where} destination panel toggled`);

        // By place, not name: the name flips to "Hide details for …" once open.
        const details = page.locator('tr[data-source="order_id"] .df2-column-expand');
        await details.scrollIntoViewIfNeeded();
        await details.click();
        await expect(details).toHaveAttribute("aria-expanded", "true");
        expectClean(await mapStepLayoutReport(page), `${where} row details open`);

        const axe = await mapAxe(page);
        expect(axe, `${where}: WCAG 2.2 AA contrast / target size\n${axe.join("\n")}`).toEqual([]);
        await context.close();
      });
    }
  }

  test("44 long-named columns stay clear and readable at 1440, 1366 and 390", async ({ browser }) => {
    test.setTimeout(240_000);
    for (const view of [W7_VIEWS[1], W7_VIEWS[2], W7_VIEWS[5]]) {
      const { context, page } = await openMap(browser, view, "light", wideSchema);
      await expect(page.locator("tr.df2-column-row").first()).toBeVisible();
      expectClean(await mapStepLayoutReport(page), `${view.width} wide schema`);
      if (view.width < 1440) {
        await page.getByRole("toolbar", { name: "Mapping actions" })
          .getByRole("button", { name: "Show destination columns" }).click();
      }
      await expect(panel(page)).toBeVisible();
      expectClean(await mapStepLayoutReport(page), `${view.width} wide schema, panel open`);
      await context.close();
    }
  });

  test("one place names each blocker; Show takes you to the required columns", async ({ browser }) => {
    // 1366: the destination panel starts closed, so Show has to open it.
    const { context, page } = await openMap(browser, W7_VIEWS[2], "light");
    const banner = page.getByTestId("map-target-issues");
    await expect(banner).toContainText("2 destination column issues block Validate");
    await expect(banner).toContainText("legacy_ref");
    await expect(banner).toContainText("tenant_id");
    const box = await banner.boundingBox();
    expect(box!.height, "the blocker banner is one compact line").toBeLessThanOrEqual(48);

    // The header counts; it does not restate the blockers.
    await expect(page.getByTestId("map-status-line")).not.toContainText("legacy_ref");

    await banner.getByRole("button", { name: "Show", exact: true }).click();
    await expect(panel(page)).toBeVisible();
    await expect(panel(page).getByRole("button", { name: "Required 2" })).toHaveAttribute("aria-pressed", "true");
    await expect(panel(page).locator("[data-dest-column]")).toHaveCount(2);
    await expect(destRow(page, "legacy_ref")).toContainText("Required");
    await expect(destRow(page, "tenant_id")).toContainText("Required");

    // The panel references the banner instead of repeating it.
    await expect(panel(page).locator(".df2-ms-panel-blockers")).toHaveCount(0);
    await expect(page.getByTestId("dest-panel-summary")).not.toContainText(/required/i);
    const restated = await page.evaluate(() => {
      const banner = document.querySelector('[data-testid="map-target-issues"]');
      return [...document.querySelectorAll(".df2-map-step-panel *")]
        .filter((el) => el.children.length === 0 && /NOT NULL with no default|need a source \(NOT NULL/.test(el.textContent ?? ""))
        .filter((el) => !banner?.contains(el))
        .map((el) => (el.textContent ?? "").slice(0, 80));
    });
    expect(restated, `blocker sentences outside the banner:\n${restated.join("\n")}`).toEqual([]);
    await context.close();
  });

  test("the toolbar: Review shows only when a row needs a decision; one primary action", async ({ browser }) => {
    const { context, page } = await openMap(browser, W7_VIEWS[1], "light");
    const toolbar = page.getByRole("toolbar", { name: "Mapping actions" });
    await expect(toolbar.getByRole("button", { name: /^Review/ })).toBeHidden();
    await expect(toolbar.getByRole("button", { name: "Auto-map…" })).toBeVisible();
    await expect(toolbar.getByRole("button", { name: "More mapping actions" })).toBeVisible();
    // Undo, Redo, the panel toggle and More sit together as the secondary cluster.
    const cluster = toolbar.locator(".df2-ms-toolbar-secondary");
    await expect(cluster.getByRole("button", { name: "Undo" })).toBeVisible();
    await expect(cluster.getByRole("button", { name: "More mapping actions" })).toBeVisible();
    await expect(page.locator(".df2-map-step-panel .df2-btn-primary:visible")).toHaveCount(1);
    await context.close();
  });

  test("the step starts right under the stepper", async ({ browser }) => {
    for (const view of [W7_VIEWS[1], W7_VIEWS[2]]) {
      const { context, page } = await openMap(browser, view, "light");
      const gap = await page.evaluate(() => {
        const chrome = document.querySelector(".df2-transfer-studio-chrome")!.getBoundingClientRect();
        const head = document.querySelector(".df2-map-step-head")!.getBoundingClientRect();
        return Math.round(head.top - chrome.bottom);
      });
      expect(gap, `${view.width}: empty band between the stepper and the Map header`).toBeLessThanOrEqual(16);
      await context.close();
    }
  });

  test("a new destination table: the panel says so instead of listing zero counts", async ({ browser }) => {
    for (const view of [W7_VIEWS[1], W7_VIEWS[5]]) {
      const { context, page } = await openMap(browser, view, "light", newDestinationTable);
      if (view.width < 1440) {
        await page.getByRole("toolbar", { name: "Mapping actions" })
          .getByRole("button", { name: "Show destination columns" }).click();
      }
      await expect(panel(page)).toBeVisible();
      await expect(page.getByTestId("dest-panel-summary")).toContainText(/new table/i);
      await expect(page.getByTestId("dest-panel-summary")).not.toContainText("0 existing");
      await expect(panel(page).getByRole("button", { name: "All 0" })).toHaveCount(0);
      await expect(panel(page).getByText("The destination reported no columns.")).toHaveCount(0);
      expectClean(await mapStepLayoutReport(page), `${view.width} new table`);
      await context.close();
    }
  });

  test("phone: the footer takes two rows and the table reads as cards", async ({ browser }) => {
    const { context, page } = await openMap(browser, W7_VIEWS[5], "light");
    const rows = await page.evaluate(() => {
      const tops = [...document.querySelectorAll(".df2-map-footer button")]
        .filter((b) => (b as HTMLElement).offsetParent)
        .map((b) => Math.round(b.getBoundingClientRect().top));
      return new Set(tops).size;
    });
    expect(rows, "footer button rows at 390 px").toBeLessThanOrEqual(2);
    await expect(page.locator(".df2-column-review-table thead")).toBeHidden();
    await context.close();
  });
});

test.describe("Map step chrome in both themes (W7 round 2)", () => {
  test("dark: the grid card, its toolbar rule and the footer's secondary buttons use the toolbar buttons' border", async ({ browser }) => {
    test.setTimeout(180_000);
    for (const view of [W7_VIEWS[1], W7_VIEWS[2], W7_VIEWS[5]]) {
      const { context, page } = await openMap(browser, view, "dark");
      // Nothing keeps focus from the walk: these are resting borders, not a focus ring.
      await page.evaluate(() => (document.activeElement as HTMLElement | null)?.blur());
      const borders = await page.evaluate(() => {
        const color = (el: Element | null | undefined) => (el ? getComputedStyle(el).borderTopColor : "missing");
        const footer = [...document.querySelectorAll(".df2-map-footer .df2-btn:not(.df2-btn-primary)")];
        return {
          toolbar: color([...document.querySelectorAll(".df2-ms-toolbar .df2-btn")].find((b) => /Auto-map/.test((b as HTMLElement).innerText))),
          more: color(document.querySelector(".df2-ms-more")),
          grid: color(document.querySelector(".df2-map-step-panel .df2-column-review-editor")),
          gridToolbarRule: (() => {
            const chrome = document.querySelector(".df2-map-step-panel .df2-column-review-chrome");
            return chrome ? getComputedStyle(chrome).borderBottomColor : "missing";
          })(),
          footer: footer.map((b) => `${(b as HTMLElement).innerText.trim()}=${color(b)}`),
        };
      });
      expect(borders.more, `${view.width}: Auto-map and More share one border`).toBe(borders.toolbar);
      expect(borders.grid, `${view.width}: the grid card's border`).toBe(borders.toolbar);
      expect(borders.gridToolbarRule, `${view.width}: the rule under the grid's toolbar`).toBe(borders.toolbar);
      expect(borders.footer.length, `${view.width}: Back, Clear key and Advanced`).toBeGreaterThanOrEqual(3);
      for (const entry of borders.footer) {
        expect(entry.split("=")[1], `${view.width}: footer ${entry.split("=")[0]} border`).toBe(borders.toolbar);
      }
      await context.close();
    }
  });

  for (const theme of ["light", "dark"] as const) {
    test(`390 ${theme}: the current step's name reads as current, not as an upcoming step`, async ({ browser }) => {
      const { context, page } = await openMap(browser, W7_VIEWS[5], theme);
      const steps = await page.evaluate(() => {
        type Rgba = [number, number, number, number];
        const parse = (c: string): Rgba => {
          const n = (c.match(/[\d.]+/g) ?? ["0", "0", "0", "0"]).map(Number);
          return [n[0], n[1], n[2], n.length > 3 ? n[3] : 1];
        };
        const over = (top: Rgba, under: Rgba): Rgba => {
          const a = top[3];
          return [top[0] * a + under[0] * (1 - a), top[1] * a + under[1] * (1 - a), top[2] * a + under[2] * (1 - a), 1];
        };
        const background = (el: Element): Rgba => {
          const layers: Rgba[] = [];
          for (let n: Element | null = el; n; n = n.parentElement) {
            const c = parse(getComputedStyle(n).backgroundColor);
            if (c[3] > 0) layers.push(c);
            if (c[3] === 1) break;
          }
          let out: Rgba = [255, 255, 255, 1];
          for (const layer of layers.reverse()) out = over(layer, out);
          return out;
        };
        const lum = (c: Rgba) => {
          const ch = c.slice(0, 3).map((v) => {
            const s = v / 255;
            return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
          });
          return 0.2126 * ch[0] + 0.7152 * ch[1] + 0.0722 * ch[2];
        };
        const ratio = (a: Rgba, b: Rgba) => {
          const [x, y] = [lum(a), lum(b)].sort((p, q) => q - p);
          return (x + 0.05) / (y + 0.05);
        };
        const nav = document.querySelector('nav[aria-label="Transfer steps"]')!;
        return [...nav.querySelectorAll(".df2-wizard-step")].map((step) => {
          const label = [...step.querySelectorAll(".df2-wizard-label, .df2-wizard-label-short")]
            .find((l) => getComputedStyle(l).display !== "none" && (l as HTMLElement).innerText.trim())!;
          const bg = background(label);
          const fg = over(parse(getComputedStyle(label).color), bg);
          return {
            name: (label as HTMLElement).innerText.trim(),
            current: step.getAttribute("aria-current") === "step",
            done: step.classList.contains("done"),
            color: getComputedStyle(label).color,
            contrast: Math.round(ratio(fg, bg) * 100) / 100,
          };
        });
      });
      const current = steps.find((s) => s.current);
      expect(current, JSON.stringify(steps)).toBeTruthy();
      expect(current!.contrast, `390 ${theme}: current step "${current!.name}" contrast`).toBeGreaterThanOrEqual(4.5);
      for (const upcoming of steps.filter((s) => !s.current && !s.done)) {
        expect(upcoming.color, `390 ${theme}: "${current!.name}" is drawn like upcoming "${upcoming.name}"`).not.toBe(current!.color);
      }
      await context.close();
    });
  }
});

/**
 * Destination → Transform must never swallow the Continue click.
 *
 * Typing the table name schedules a destination re-read 350 ms later. That read
 * used to disable Continue while it ran, so a press that began before the read
 * and released during it was dropped by the browser (a disabled button gets no
 * click): the read finished, Continue looked ready again, and the operator was
 * still on Destination (the walk timed out about once in 18-100 runs). The read
 * is held open here so the press always straddles it.
 */
test.describe("Destination Continue while the destination is being re-read", () => {
  for (const status of [200, 500]) {
    test(`a press that straddles the re-read still reaches Transform, named and busy meanwhile (read answers ${status})`, async ({ page }) => {
      await stubMapStudio(page);
      const intro = fixture("transfer_introspect.json");
      let release: () => void = () => {};
      const released = new Promise<void>((resolve) => { release = resolve; });
      const readsTable = (body: unknown, table: string) =>
        (body as { destination?: { table?: string } } | null)?.destination?.table === table;
      await page.route("**/api/v1/transfer/introspect", async (route) => {
        if (readsTable(route.request().postDataJSON(), "orders_dest")) {
          await released;
          // A failed read is reported by its own toast and never blocks Continue.
          if (status !== 200) {
            await route.fulfill({ status, contentType: "application/json", body: '{"detail":"catalog unavailable"}' });
            return;
          }
        }
        await route.fulfill({ status: 200, contentType: "application/json", body: intro });
      });
      await openStudio(page);
      await page.addStyleTag({ content: ".dt-toast, .dt-toast-more { display: none !important; }" });
      await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
      await page.getByRole("button", { name: /Continue to Destination/ }).click();
      await page.getByRole("radio", { name: /QA Postgres Local/ }).click();
      const firstRead = page.waitForResponse((res) =>
        res.url().includes("/api/v1/transfer/introspect") && readsTable(res.request().postDataJSON(), "orders"));
      await page.locator("#dest-col").fill("orders");
      await firstRead;

      const next = page.getByRole("button", { name: /Continue to Transform/ });
      await expect(next).toBeEnabled();
      const reread = page.waitForRequest((req) =>
        req.url().includes("/api/v1/transfer/introspect") && readsTable(req.postDataJSON(), "orders_dest"));
      await page.locator("#dest-col").fill("orders_dest");
      await expect(next).toBeEnabled();
      const box = (await next.boundingBox())!;
      await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
      await page.mouse.down();
      await reread;
      await page.mouse.up();

      // Waiting on the read: still named, announced busy.
      await expect.soft(page.getByRole("button", { name: "Continue to Transform" })).toHaveAttribute("aria-busy", "true", { timeout: 5_000 });
      release();
      await expect(page.getByRole("heading", { name: "Transform", exact: true })).toBeVisible();
    });
  }
});
