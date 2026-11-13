import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test, type Page } from "@playwright/test";
import { installHermeticStubs } from "./hermetic";
import { expectStudioShellClear } from "./layout";
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
  await page.route("**/api/v1/transfer/mapping/memory", async (route) => {
    captured.memory.push(route.request().postDataJSON());
    await route.fulfill(json('{"recorded":1,"total":1}'));
  });
  return captured;
}

async function reachMap(page: Page): Promise<void> {
  await openStudio(page);
  await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
  await page.getByRole("button", { name: /Continue to Destination/ }).click();
  await page.getByRole("radio", { name: /QA Postgres Local/ }).click();
  await page.locator("#dest-col").fill("orders_dest");
  await page.locator("#dest-col").press("Enter");
  await page.getByRole("button", { name: /Continue to Transform/ }).click();
  await page.getByRole("button", { name: /Continue without transforming/ }).click();
  await expect(page.getByRole("toolbar", { name: "Mapping actions" })).toBeVisible();
  await settleToasts(page);
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
        const context = await browser.newContext({ viewport: view });
        const page = await context.newPage();
        await stubMapStudio(page);
        await reachMap(page);
        await page.evaluate((t) => {
          localStorage.setItem("df2.theme", t);
          document.documentElement.setAttribute("data-theme", t);
        }, theme);
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
