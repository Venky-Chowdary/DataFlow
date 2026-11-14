import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test, type Browser, type BrowserContext, type Page } from "@playwright/test";
import { installHermeticStubs } from "./hermetic";
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
const FIXTURES = join(dirname(fileURLToPath(import.meta.url)), "fixtures", "map-step");
const fixture = (name: string) => readFileSync(join(FIXTURES, name), "utf8");
const SHOTS = process.env.MAP_STEP_SHOTS ?? "";

interface Captured {
  autoMap: Array<Record<string, unknown>>;
  memory: Array<Record<string, unknown>>;
}

async function stubMapStudio(page: Page): Promise<Captured> {
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
  // Leaving the table field re-reads the destination, and Continue is disabled
  // while that read runs. A click that lands while the button is being disabled
  // is dropped (W7: the "Continue without transforming" time-outs, 1 run in
  // ~18). Let the read finish, then continue, and check the step changed.
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
