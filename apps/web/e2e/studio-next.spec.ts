import { execFileSync } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test, type Page } from "@playwright/test";
import { expectDisjoint, expectHitInside, expectStackedAbove, expectStudioShellClear, expectWithinViewport } from "./layout";
import { openStudio, reachTransform, settleToasts, shoot, tokenColor } from "./support";

/** A toast shifts the narrow page. Remove it before the shot; the mask alone does not. */
async function setPalette(page: Page, open: boolean): Promise<void> {
  const rail = page.locator(".df2-canvas-rail");
  const collapsed = await rail.evaluate((node) => node.classList.contains("is-collapsed"));
  if (collapsed === !open) return;
  await page.getByRole("button", { name: "More actions" }).click();
  await page.getByRole("menuitem", { name: "Palette", exact: true }).click();
}

async function openMenuAction(page: Page, name: string): Promise<void> {
  await page.getByRole("button", { name: "More actions" }).click();
  await page.getByRole("menuitem", { name, exact: true }).click();
}

async function quietChrome(page: Page): Promise<void> {
  await page.locator(".dt-toast").evaluateAll((nodes) => {
    for (const node of nodes) (node as HTMLElement).style.setProperty("display", "none");
  });
}

function channel(value: number): number {
  const unit = value / 255;
  return unit <= 0.03928 ? unit / 12.92 : ((unit + 0.055) / 1.055) ** 2.4;
}

function contrast(foreground: string, background: string): number {
  const parse = (color: string) => {
    const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)/);
    if (!match) return 0;
    const [red, green, blue] = [Number(match[1]), Number(match[2]), Number(match[3])].map(channel);
    return 0.2126 * red + 0.7152 * green + 0.0722 * blue;
  };
  const lighter = Math.max(parse(foreground), parse(background));
  const darker = Math.min(parse(foreground), parse(background));
  return (lighter + 0.05) / (darker + 0.05);
}

async function buildValidOrdersChain(page: Page): Promise<void> {
  await setPalette(page, true);
  await page.locator(".df2-canvas-op[data-op='trim']").click();
  await page.locator(".df2-canvas-op[data-op='drop_column']").click();
  await page.locator(".df2-canvas-op[data-op='filter_rows']").click();
  await page.locator("#canvas-opt-condition-field-0").click();
  await page.getByRole("option", { name: "status", exact: true }).click();
  await page.getByRole("button", { name: "Add step" }).click();
  await expect(page.locator(".df2-canvas-node")).toHaveCount(3);
  await expect(page.getByTestId("canvas-recipe-pill")).toHaveText("Valid");
}

async function nodeCenterGap(page: Page, index: number): Promise<number> {
  return page.evaluate((step) => {
    const stage = document.querySelector("[data-testid='transform-canvas-stage']");
    const node = document.querySelector(`.df2-canvas-node[data-index="${step}"]`);
    if (!stage || !node) return Number.POSITIVE_INFINITY;
    const stageBox = stage.getBoundingClientRect();
    const nodeBox = node.getBoundingClientRect();
    const dx = (nodeBox.x + nodeBox.width / 2) - (stageBox.x + stageBox.width / 2);
    const dy = (nodeBox.y + nodeBox.height / 2) - (stageBox.y + stageBox.height / 2);
    return Math.hypot(dx, dy);
  }, index);
}

/** The Back / Continue bar is a flex row under the canvas, flush with the page. */
async function expectActionBarBelowCanvas(page: Page): Promise<void> {
  const stage = page.getByTestId("transform-canvas-stage");
  const canvas = page.getByTestId("transform-canvas");
  const dock = page.getByTestId("canvas-preview");
  const bar = page.locator(".df2-xform-actions");
  const stageBox = await stage.boundingBox();
  const canvasBox = await canvas.boundingBox();
  const dockBox = await dock.boundingBox();
  const barBox = await bar.boundingBox();
  const viewport = page.viewportSize();
  expect(stageBox && canvasBox && dockBox && barBox && viewport).toBeTruthy();
  expect(barBox!.y).toBeGreaterThanOrEqual(stageBox!.y + stageBox!.height - 1);
  expect(canvasBox!.y + canvasBox!.height).toBeLessThanOrEqual(barBox!.y + 1);
  expect(dockBox!.y + dockBox!.height).toBeLessThanOrEqual(barBox!.y + 1);
  expect(barBox!.y + barBox!.height).toBeGreaterThan(viewport!.height - 48);
  expect(barBox!.y + barBox!.height).toBeLessThanOrEqual(viewport!.height);
}

async function apiUp(page: Page): Promise<boolean> {
  try {
    const response = await page.request.get("/api/v1/health");
    return response.ok();
  } catch {
    return false;
  }
}

test.describe("studio next", () => {
  test.beforeEach(async ({ page }) => {
    // The recipe store is keyed by source and destination, and these tests
    // all load the same sample. A draft saved by one test would come back as
    // extra canvas nodes in the next. Layout checks start from an empty recipe.
    await page.route("**/api/v1/shape/drafts**", async (route) => {
      if (route.request().method() === "GET") {
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({ draft: null }),
        });
        return;
      }
      await route.continue();
    });
    await page.addInitScript(() => {
      localStorage.setItem("df.studioSchemaBrowser", "1");
      localStorage.setItem("df.studioCanvas", "1");
      localStorage.setItem("df2.theme", "light");
    });
    await page.route("**/api/v1/workspace/settings", async (route) => {
      if (route.request().method() !== "GET") {
        await route.continue();
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          org_name: "Datawrap",
          timezone: "UTC",
          retention_days: 90,
          studio_schema_browser: true,
          studio_canvas: true,
        }),
      });
    });
  });

  test("canvas light and dark stay on the chain layout", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    await setPalette(page, true);
    await page.locator(".df2-canvas-op[data-op='trim']").click();
    await expect(page.locator(".react-flow__node")).toHaveCount(1);
    const status = page.getByTestId("canvas-preview-status");
    let seen = "";
    await expect.poll(async () => {
      const text = (await status.innerText()).trim();
      if (!text || text.startsWith("Previewing")) {
        seen = "";
        return "";
      }
      if (text === seen) return text;
      seen = text;
      return "";
    }, { timeout: 20_000 }).not.toBe("");
    await page.addStyleTag({ content: "* { caret-color: transparent !important; transition: none !important; animation: none !important; }" });
    await quietChrome(page);
    await expectActionBarBelowCanvas(page);
    // xyflow edge antialiasing moves about one percent between captures.
    await shoot(page, "canvas-light-1440.png", [], 0.02);
    await page.setViewportSize({ width: 390, height: 844 });
    await quietChrome(page);
    await expectActionBarBelowCanvas(page);
    await shoot(page, "canvas-light-390.png", [], 0.04);
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.evaluate(() => {
      localStorage.setItem("df2.theme", "dark");
      document.documentElement.setAttribute("data-theme", "dark");
      document.documentElement.classList.add("df2-theme-dark");
    });
    await expect(page.locator(".react-flow")).toHaveClass(/dark/);
    await quietChrome(page);
    await expectActionBarBelowCanvas(page);
    await shoot(page, "canvas-dark-1440.png", [], 0.02);
    await page.setViewportSize({ width: 390, height: 844 });
    await quietChrome(page);
    await expectActionBarBelowCanvas(page);
    await shoot(page, "canvas-dark-390.png", [], 0.04);
  });

  test("canvas regions, drawers, and dialogs stay apart", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    await setPalette(page, true);
    await page.locator(".df2-canvas-op[data-op='trim']").click();
    await expect(page.locator(".react-flow__node")).toHaveCount(1);
    const views = [
      { width: 1440, height: 900, theme: "light" },
      { width: 390, height: 844, theme: "light" },
      { width: 1440, height: 900, theme: "dark" },
      { width: 390, height: 844, theme: "dark" },
    ] as const;
    for (const view of views) {
      await page.setViewportSize({ width: view.width, height: view.height });
      await page.evaluate((theme) => {
        localStorage.setItem("df2.theme", theme);
        document.documentElement.setAttribute("data-theme", theme);
        document.documentElement.classList.toggle("df2-theme-dark", theme === "dark");
      }, view.theme);
      await expectActionBarBelowCanvas(page);
      const pairs: Array<[string, string]> = [
        [".df2-canvas-head", ".df2-canvas-body"],
        ["[data-testid='transform-canvas-stage']", "[data-testid='canvas-preview']"],
        ["[data-testid='canvas-preview']", ".df2-xform-actions"],
        ["[data-testid='transform-canvas']", ".df2-xform-actions"],
        [".df2-canvas-rail", ".df2-canvas-inspector"],
        ["[data-testid='canvas-preview']", ".df2-canvas-inspector"],
        ["[aria-label='Transfer steps']", ".df2-xform-actions"],
        [".df2-canvas-head", ".df2-xform-actions"],
      ];
      if (view.width < 768) {
        pairs.push(
          [".df2-canvas-rail", "[data-testid='transform-canvas-stage']"],
          ["[data-testid='transform-canvas-stage']", ".df2-canvas-inspector"],
        );
      }
      for (const [left, right] of pairs) await expectDisjoint(page, left, right);
      for (const selector of [
        ".df2-xform-actions",
        "[data-testid='transform-canvas']",
        "[data-testid='transform-canvas-stage']",
        "[data-testid='canvas-preview']",
      ]) {
        await expectWithinViewport(page, selector);
      }
    }

    await page.setViewportSize({ width: 1440, height: 900 });
    await quietChrome(page);
    await openMenuAction(page, "Rules and suggestions");
    await expect(page.getByRole("heading", { name: "Suggested repairs" })).toBeVisible();
    await expectStackedAbove(page, ".df2-xform-actions", ".df2-drawer-overlay");
    await expectWithinViewport(page, ".df2-drawer-overlay .df2-drawer", 8);
    await expectHitInside(page, page.locator(".df2-drawer-close"), ".df2-drawer");
    await page.locator(".df2-drawer-close").click();

    await quietChrome(page);
    await openMenuAction(page, "Templates");
    const templates = page.getByRole("dialog", { name: "Templates" });
    await expect(templates).toBeVisible();
    await expectStackedAbove(page, ".df2-xform-actions", ".df2-modal-overlay");
    await expectWithinViewport(page, ".df2-modal");
    await expectHitInside(page, templates.getByRole("button", { name: "Close" }), ".df2-modal");
    await templates.getByRole("button", { name: "Close" }).click();

    await page.setViewportSize({ width: 390, height: 844 });
    await quietChrome(page);
    await page.evaluate(() => {
      localStorage.setItem("df2.theme", "dark");
      document.documentElement.setAttribute("data-theme", "dark");
      document.documentElement.classList.add("df2-theme-dark");
    });
    await openMenuAction(page, "Templates");
    await expect(page.getByRole("dialog", { name: "Templates" })).toBeVisible();
    await expectStackedAbove(page, ".df2-xform-actions", ".df2-modal-overlay");
    await expectWithinViewport(page, ".df2-modal");
    await page.getByRole("dialog", { name: "Templates" }).getByRole("button", { name: "Close" }).click();
    await openMenuAction(page, "Rules and suggestions");
    await expect(page.getByRole("heading", { name: "Suggested repairs" })).toBeVisible();
    await expectStackedAbove(page, ".df2-xform-actions", ".df2-drawer-overlay");
    await expectWithinViewport(page, ".df2-drawer-overlay .df2-drawer", 8);
    await expectWithinViewport(page, ".df2-drawer-close");
    await expectHitInside(page, page.locator(".df2-drawer-close"), ".df2-drawer");
  });

  test("canvas is a full-page editor for drop, order, links, and preview", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    const canvas = page.getByTestId("transform-canvas");
    await expect(canvas).toBeVisible();
    await expect(page.getByRole("heading", { name: "Steps, in order" })).toHaveCount(0);
    await expect(page.getByRole("heading", { name: "Suggested repairs" })).toHaveCount(0);
    const canvasBox = await canvas.boundingBox();
    expect(canvasBox).toBeTruthy();
    expect(canvasBox!.y).toBeLessThan(460);
    expect(canvasBox!.y + canvasBox!.height).toBeGreaterThan(500);
    await expectActionBarBelowCanvas(page);

    await openMenuAction(page, "Rules and suggestions");
    await expect(page.getByRole("heading", { name: "Suggested repairs" })).toBeVisible();
    await page.getByRole("button", { name: "Close", exact: true }).click();
    await expect(page.getByRole("heading", { name: "Suggested repairs" })).toHaveCount(0);

    await setPalette(page, true);
    const sql = page.locator(".df2-canvas-sql");
    await expect(sql).toBeVisible();
    await expect(sql.locator("summary")).toHaveText("Warehouse SQL (runs after load)");
    await expect(sql).not.toHaveAttribute("open");

    const trim = page.locator(".df2-canvas-op[data-op='trim']");
    await expect(trim).toBeVisible();
    const stage = page.getByTestId("transform-canvas-stage");
    const status = page.getByTestId("canvas-preview-status");
    await expect(status).toBeVisible();
    await expect(status).not.toHaveText("Previewing the recipe…", { timeout: 20_000 });
    const statusBefore = (await status.innerText()).trim();

    const drop = { x: 400, y: 160 };
    await trim.dragTo(stage, { targetPosition: drop });
    await expect(page.locator(".react-flow__node")).toHaveCount(1);
    const stageBox = await stage.boundingBox();
    const nodeBox = await page.locator(".react-flow__node").first().boundingBox();
    expect(stageBox && nodeBox).toBeTruthy();
    expect(Math.abs(nodeBox!.x - (stageBox!.x + drop.x))).toBeLessThan(24);
    expect(Math.abs(nodeBox!.y - (stageBox!.y + drop.y))).toBeLessThan(24);
    await expect(page.locator("#canvas-inspector")).toContainText(/Edit step|New step/);
    await expect(status).not.toHaveText(statusBefore, { timeout: 20_000 });

    const collapse = page.locator(".df2-canvas-op[data-op='collapse_whitespace']");
    await collapse.click();
    await expect(page.locator(".react-flow__node")).toHaveCount(2);
    const moving = await page.locator(".react-flow__node").nth(1).boundingBox();
    const first = await page.locator(".react-flow__node").nth(0).boundingBox();
    expect(moving && first).toBeTruthy();
    await page.mouse.move(moving!.x + moving!.width / 2, moving!.y + 36);
    await page.mouse.down();
    await page.mouse.move(first!.x - 24, moving!.y + 36, { steps: 15 });
    await page.mouse.up();
    await expect(page.locator(".df2-canvas-node").nth(0)).toHaveAttribute("data-op", "collapse_whitespace");

    const source = page.locator(".react-flow__node").nth(0).locator(".react-flow__handle-right").first();
    const target = page.locator(".react-flow__node").nth(1).locator(".react-flow__handle-left").first();
    await source.dragTo(target, { force: true, steps: 12 });
    await expect(page.getByText(/Connected step 1 to step 2/)).toBeVisible();
    await source.dragTo(page.locator(".react-flow__node").nth(0).locator(".react-flow__handle-left").first(), { force: true, steps: 12 });
    await expect(page.getByRole("alert").filter({ hasText: /itself/ })).toBeVisible();

    const lower = page.locator(".df2-canvas-op[data-op='absolute']");
    const liveStage = await stage.boundingBox();
    const firstBox = await page.locator(".react-flow__node").nth(0).boundingBox();
    const nextBox = await page.locator(".react-flow__node").nth(1).boundingBox();
    expect(firstBox && nextBox && liveStage).toBeTruthy();
    const gap = nextBox!.x - (firstBox!.x + firstBox!.width);
    const midX = (firstBox!.x + firstBox!.width + Math.max(gap, 8) / 2) - liveStage!.x;
    const midY = (firstBox!.y + firstBox!.height / 2) - liveStage!.y;
    await lower.dragTo(stage, { targetPosition: { x: Math.max(24, midX), y: Math.max(24, midY) } });
    await expect(page.locator(".df2-canvas-node").nth(1)).toHaveAttribute("data-op", "absolute");

    const count = await page.locator(".react-flow__node").count();
    await page.getByRole("button", { name: "Undo" }).click();
    await expect(page.locator(".react-flow__node")).toHaveCount(count - 1);
    await page.getByRole("button", { name: "Redo" }).click();
    await expect(page.locator(".react-flow__node")).toHaveCount(count);

    await page.getByRole("button", { name: "Run preview" }).first().click();
    await expect(page.getByTestId("canvas-preview").locator("tbody tr").first()).toBeVisible({ timeout: 20_000 });
    const continueRecipe = page.getByRole("button", { name: "Continue with this transform" });
    await expect(continueRecipe).toBeEnabled();
    const continueFit = await continueRecipe.evaluate((node) => node.scrollWidth <= node.clientWidth + 1);
    expect(continueFit).toBeTruthy();

    const primary = page.locator(".df2-studio-next .df2-btn-primary").first();
    await expect(primary).toBeVisible();
    const color = await primary.evaluate((node) => getComputedStyle(node).backgroundColor);
    // The primary fill is the --df-brand-tile token (studio-next repaints
    // --df-action-bg from it) — assert the resolved token, never a hex
    // literal that goes stale on a brand repaint (UI-11).
    expect(color).toBe(await tokenColor(page, "--df-brand-tile"));
  });

  test("sample export runs from the canvas recipe", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    await setPalette(page, true);
    await page.locator(".df2-canvas-op[data-op='trim']").dragTo(page.getByTestId("transform-canvas-stage"), {
      targetPosition: { x: 400, y: 160 },
    });
    await expect(page.locator(".react-flow__node")).toHaveCount(1);
    await page.getByRole("button", { name: "Run preview" }).first().click();
    await expect(page.getByTestId("canvas-preview").locator("tbody tr").first()).toBeVisible({ timeout: 20_000 });
    const toMap = page.getByRole("button", { name: "Continue with this transform" });
    await expect(toMap).toBeEnabled();
    await toMap.click();
    const execute = page.getByRole("button", { name: /^Execute/ });
    await expect(execute).toBeVisible({ timeout: 30_000 });
    await expect(execute).toBeEnabled({ timeout: 40_000 });
    await execute.click();
    await expect(page.getByRole("heading", { name: "Transfer complete" })).toBeVisible({ timeout: 40_000 });
  });

  test("database source shows the configured schema tree", async ({ page }) => {
    test.skip(!(await apiUp(page)), "API is not running, so the schema tree is not proven live.");
    const dir = mkdtempSync(join(tmpdir(), "studio-src-"));
    const dbPath = join(dir, "studio_src.db");
    execFileSync("python3", ["-c", `
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
conn.execute("create table customers (id integer primary key, email text)")
conn.executemany("insert into customers values (?, ?)", [(i, f"c{i}@example.com") for i in range(1, 13)])
conn.commit()
`, dbPath]);
    const created = await page.request.post("/api/v1/connectors/saved", {
      data: {
        name: "Studio SQLite",
        type: "sqlite",
        role: "both",
        // SQLite has no schema layer. The file path is the database, and
        // a configured schema name is stored as empty.
        database: dbPath,
        connection_string: dbPath,
      },
    });
    expect(created.ok()).toBeTruthy();

    await openStudio(page);
    await page.getByRole("radio", { name: "Database" }).click();
    const source = page.getByLabel("Source connector");
    const sourceValue = await source.evaluate((el) => {
      const option = [...(el as HTMLSelectElement).options].find((item) => item.label.includes("Studio SQLite"));
      return option?.value ?? "";
    });
    expect(sourceValue).toBeTruthy();
    await source.selectOption(sourceValue);
    const browser = page.getByTestId("studio-schema-browser");
    await expect(browser).toBeVisible();
    await expect(browser.getByText(dbPath)).toBeVisible();
    await expect(browser.getByRole("button", { name: "sales" })).toHaveCount(0);
    await expect(browser.getByText("customers")).toBeVisible();
    await expect(page.getByTestId("transform-canvas")).toHaveCount(0);
    const schemaBrowser = page.getByTestId("studio-schema-browser");
    const steps = page.getByRole("navigation", { name: "Transfer steps" });
    const review = page.getByRole("button", { name: /Continue to Review/ });
    const classic = page.getByRole("button", { name: /Continue to Destination/ });
    const proceed = (await review.isVisible()) ? review : classic;
    await expectDisjoint(page, schemaBrowser, steps);
    await expectDisjoint(page, schemaBrowser, proceed);
    await expectHitInside(page, proceed, ".df2-wizard-footer");
    await page.setViewportSize({ width: 390, height: 844 });
    await expectDisjoint(page, schemaBrowser, steps);
    await expectDisjoint(page, schemaBrowser, proceed);
    await expectHitInside(page, proceed, ".df2-wizard-footer");
  });

  test("each node has an icon, a label, and a status, and set operations stay disabled", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    await setPalette(page, true);
    const trim = page.locator(".df2-canvas-op[data-op='trim']");
    await expect(trim).toHaveAttribute("data-icon", "trim");
    await expect(trim).toHaveAttribute("data-stripe", "values");
    await expect(trim).toContainText("Trim");
    await trim.click();
    const node = page.locator(".df2-canvas-node").first();
    await expect(node).toHaveAttribute("data-icon", "trim");
    await expect(node).toHaveAttribute("data-status", "valid");
    await expect(node).toContainText("Trim");
    await expect(node.locator(".df2-canvas-group-in")).toHaveText("Input");
    await expect(node.locator(".df2-canvas-group-out")).toHaveText("Output");
    await expect(node.getByLabel("Input")).toBeVisible();
    await expect(node.getByLabel(/^Output/)).toBeVisible();
    await expect(node).toContainText("Remove leading and trailing whitespace");
    await expect(page.locator(".react-flow__minimap")).toBeVisible();
    await page.getByRole("button", { name: "Fit", exact: true }).click();
    await page.getByRole("button", { name: "Auto-arrange" }).click();
    await expect(node).toBeVisible();
    // join/union/lookup graduated to in-flight row steps; pivot is still post-load only.
    await page.getByLabel("Search operations").fill("pivot");
    const pivot = page.locator(".df2-canvas-op[data-op='pivot']").filter({ visible: true });
    await expect(pivot).toHaveAttribute("data-executable", "false");
    await expect(pivot).toBeDisabled();
    await expect(pivot).toHaveAttribute("data-icon", "pivot");
    await page.getByRole("button", { name: "Preview this step" }).click();
    await expect(page.getByTestId("canvas-node-preview")).toContainText(/sample|Preview|row/i);
    await expect(page.getByTestId("transform-canvas")).toHaveAttribute("data-document", "chain");
    await expect(page.getByTestId("transform-canvas")).toHaveAttribute("data-set-hash", "");

    await page.getByRole("button", { name: "Incoming Fields" }).click();
    await expect(page.locator(".df2-canvas-field-list li").first()).toBeVisible();
    await page.locator(".df2-canvas-tab-rail").getByRole("button", { name: "Trim" }).click();
    await page.locator("#canvas-label").fill("Clean email");
    await page.locator("#canvas-label").blur();
    await expect(node).toContainText("Clean email");
    const beforeUndo = await page.locator(".react-flow__node").count();
    await page.getByTestId("transform-canvas").focus();
    await page.keyboard.press("Control+z");
    await expect(page.locator(".react-flow__node")).toHaveCount(beforeUndo);
    await page.keyboard.press("Control+z");
    await expect(page.locator(".react-flow__node")).toHaveCount(0);
    await page.keyboard.press("Control+Shift+Z");
    await expect(page.locator(".react-flow__node")).toHaveCount(1);
  });

  test("a validation error selects, centers, and focuses that node", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    await buildValidOrdersChain(page);
    await page.locator(".df2-canvas-node[data-index='2']").click();
    await page.getByRole("button", { name: "Advanced expression" }).click();
    await page.locator("#canvas-opt-condition").fill("[order_id] <> ''");
    await page.getByRole("button", { name: "Update step" }).click();
    await expect(page.getByTestId("canvas-recipe-pill")).toHaveText("Invalid");
    await setPalette(page, false);
    await page.locator(".df2-canvas-node[data-index='0']").click();
    await page.getByRole("button", { name: /^Issues/ }).click();
    const validation = page.getByRole("region", { name: "Issues" });
    await expect(validation.getByRole("button", { name: "Step 1 Valid" })).toBeVisible();
    await expect(validation.getByRole("button", { name: "Step 2 Valid" })).toBeVisible();
    await expect(validation.getByRole("button", { name: "Step 3 Invalid" })).toBeVisible();
    await expect(validation).toContainText(/order_id/);
    await validation.getByRole("button", { name: "Step 3 Invalid" }).click();
    await expect(page.locator(".df2-canvas-node.is-selected[data-index='2']")).toBeVisible();
    await expect(page.locator("#canvas-inspector")).toBeFocused();
    await expect.poll(() => nodeCenterGap(page, 2)).toBeLessThan(80);
    await page.getByRole("button", { name: "Fit", exact: true }).click();
    await page.locator(".df2-canvas-node[data-index='0'] .df2-canvas-node-title").click();
    await page.getByRole("button", { name: "Step 3 Invalid" }).click();
    await expect(page.locator(".df2-canvas-node.is-selected[data-index='2']")).toBeVisible();
    await expect(page.locator("#canvas-inspector")).toBeFocused();
    await expect.poll(() => nodeCenterGap(page, 2)).toBeLessThan(80);
    await expect(page.locator(".df2-alert-error")).toHaveCount(0);
  });

  test("canvas icons and the action bar hold in dark and at 390", async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem("df2.theme", "dark");
    });
    await page.setViewportSize({ width: 390, height: 844 });
    await openStudio(page);
    await reachTransform(page);
    // A toast docks in-flow in the header (D-BP-20) and, at 390, leaves the canvas
    // 12px short of its 232px stage plus drawers. Measure the idle layout.
    await settleToasts(page);
    await page.evaluate(() => document.documentElement.setAttribute("data-theme", "dark"));
    const trim = page.locator(".df2-canvas-op[data-op='trim']");
    await expect(trim).toBeVisible();
    await trim.click();
    await expect(page.locator(".df2-canvas-node").first()).toHaveAttribute("data-status", "valid");
    await expect(page.locator(".df2-canvas-group-out").first()).toHaveText("Output");
    await expect(page.locator(".react-flow__minimap")).toBeHidden();
    await expectActionBarBelowCanvas(page);
    const node = page.locator(".df2-canvas-node").first();
    await expectDisjoint(page, node, ".df2-xform-actions");
    await expectHitInside(page, node, "[data-testid='transform-canvas-stage']");
  });

  test("arranged flow stays clear of the minimap and each category has its own color", async ({ page }) => {
  await openStudio(page);
  await reachTransform(page);
  await buildValidOrdersChain(page);
  await expect(page.locator(".df2-alert-error")).toHaveCount(0);
  const status = page.getByTestId("canvas-preview-status");
  await expect.poll(async () => {
    const text = (await status.innerText()).trim();
    if (!text || /Previewing/i.test(text)) return "";
    return text;
  }, { timeout: 20_000 }).toMatch(/row/);
  await expect(status).not.toHaveText(/does not have|not available|refused/i);
  await expect(page.locator(".react-flow__edge-straight")).toHaveCount(2);
  await page.getByRole("button", { name: "Auto-arrange" }).click();
  const stage = page.getByTestId("transform-canvas-stage");
  const minimap = page.locator(".react-flow__minimap");
  await expect(minimap).toBeVisible();
  const stageBox = await stage.boundingBox();
  const mapBox = await minimap.boundingBox();
  expect(stageBox && mapBox).toBeTruthy();
  const nodes = page.locator(".react-flow__node");
  const count = await nodes.count();
  const scale = await page.locator(".react-flow__viewport").evaluate((node) => new DOMMatrix(getComputedStyle(node).transform).a);
  expect(scale).toBeGreaterThanOrEqual(0.75);
  for (let index = 0; index < count; index += 1) {
    const box = await nodes.nth(index).boundingBox();
    expect(box).toBeTruthy();
    expect(box!.width).toBeGreaterThanOrEqual(248 * 0.75 - 1);
    const onStage = box!.x < stageBox!.x + stageBox!.width && box!.x + box!.width > stageBox!.x
      && box!.y < stageBox!.y + stageBox!.height && box!.y + box!.height > stageBox!.y;
    if (index === 0) {
      expect(box!.x).toBeGreaterThanOrEqual(stageBox!.x - 1);
      expect(box!.y).toBeGreaterThanOrEqual(stageBox!.y - 1);
      expect(box!.x + box!.width).toBeLessThanOrEqual(stageBox!.x + stageBox!.width + 1);
      expect(box!.y + box!.height).toBeLessThanOrEqual(stageBox!.y + stageBox!.height + 1);
    }
    if (!onStage) continue;
    const clear = box!.x + box!.width <= mapBox!.x + 1
      || mapBox!.x + mapBox!.width <= box!.x + 1
      || box!.y + box!.height <= mapBox!.y + 1
      || mapBox!.y + mapBox!.height <= box!.y + 1;
    expect(clear).toBeTruthy();
  }
  const stripes = await page.locator(".df2-canvas-node").evaluateAll((cards) => cards.map((card) => card.getAttribute("data-stripe")));
  expect(stripes).toEqual(["values", "columns", "rows"]);
  const paints = await page.locator(".df2-canvas-node").evaluateAll((cards) => cards.map((card) => {
    const band = card.querySelector(".df2-canvas-node-band");
    const mark = card.querySelector(".df2-canvas-node-mark");
    const stripe = card.querySelector(".df2-canvas-node-stripe");
    const title = card.querySelector(".df2-canvas-node-title");
    const bandStyle = band ? getComputedStyle(band) : null;
    const markStyle = mark ? getComputedStyle(mark) : null;
    const titleStyle = title ? getComputedStyle(title) : null;
    return {
      band: bandStyle?.backgroundColor ?? "",
      stripe: stripe ? getComputedStyle(stripe).backgroundColor : "",
      mark: markStyle?.backgroundColor ?? "",
      title: titleStyle?.color ?? "",
      icon: markStyle?.color ?? "",
    };
  }));
  expect(new Set(paints.map((paint) => paint.stripe)).size).toBe(3);
  expect(new Set(paints.map((paint) => paint.mark)).size).toBe(3);
  for (const paint of paints) {
    expect(contrast(paint.title, paint.band)).toBeGreaterThanOrEqual(4.5);
    expect(contrast(paint.icon, paint.mark)).toBeGreaterThanOrEqual(4.5);
  }
  const stageHeight = stageBox!.height;
  const dock = await page.getByTestId("canvas-preview").boundingBox();
  const rail = await page.locator(".df2-canvas-rail").boundingBox();
  const inspector = await page.locator(".df2-canvas-inspector").boundingBox();
  expect(dock && rail && inspector).toBeTruthy();
  expect(stageHeight).toBeGreaterThan(dock!.height);
  expect(rail!.width).toBeGreaterThanOrEqual(180);
  expect(inspector!.width).toBeGreaterThanOrEqual(240);
  expect(inspector!.height).toBeGreaterThanOrEqual(48);

  await page.getByRole("button", { name: "More actions" }).click();
  const overview = page.getByRole("menuitem", { name: "Overview", exact: true });
  await expect(overview).toHaveAttribute("aria-pressed", "true");
  await overview.click();
  await expect(minimap).toBeHidden();
  await page.getByRole("button", { name: "More actions" }).click();
  const overviewAgain = page.getByRole("menuitem", { name: "Overview", exact: true });
  await overviewAgain.click();
  await expect(minimap).toBeVisible();
  await page.getByRole("button", { name: "More actions" }).click();
  await expect(page.getByRole("menuitem", { name: "Overview", exact: true })).toHaveAttribute("aria-pressed", "true");
  await page.getByRole("button", { name: "More actions" }).click();
  await page.getByRole("button", { name: "Issues", exact: true }).click();
  const validation = page.getByRole("region", { name: "Issues" });
  await expect(validation.getByRole("button", { name: "Step 1 Valid" })).toBeVisible();
  await expect(validation.getByRole("button", { name: "Step 2 Valid" })).toBeVisible();
  await expect(validation.getByRole("button", { name: "Step 3 Valid" })).toBeVisible();
  await page.getByRole("button", { name: "Issues", exact: true }).click();
  await openMenuAction(page, "Focus canvas");
  await expect(page.locator(".df2-sidebar")).toBeHidden();
  await expect(page.locator(".df2-transfer-studio-chrome")).toBeHidden();
  for (const size of [{ width: 1440, height: 900 }, { width: 1366, height: 768 }]) {
    await page.setViewportSize(size);
    const canvasBox = await page.getByTestId("transform-canvas").boundingBox();
    const focusedStage = await stage.boundingBox();
    expect(canvasBox && focusedStage).toBeTruthy();
    expect(canvasBox!.height).toBeGreaterThanOrEqual(size.height * 0.7 - 1);
    expect(focusedStage!.height).toBeGreaterThanOrEqual(size.height * 0.7 - 1);
  }
  await page.setViewportSize({ width: 1440, height: 900 });
  await openMenuAction(page, "Exit focus");
  await expect(page.locator(".df2-sidebar")).toBeVisible();
  await page.getByRole("button", { name: "Maximize preview" }).click();
  await expect(page.getByTestId("canvas-preview")).toHaveClass(/is-max/);
  await page.getByRole("button", { name: "Restore preview" }).click();
  await expect(page.getByTestId("canvas-preview")).not.toHaveClass(/is-max/);
  await page.getByRole("button", { name: "Hide preview" }).click();
  const zoomed = page.locator(".react-flow__viewport");
  const zoomBefore = await zoomed.evaluate((node) => new DOMMatrix(getComputedStyle(node).transform).a);
  await page.getByRole("button", { name: "Zoom in", exact: true }).click();
  await expect.poll(async () => zoomed.evaluate((node) => new DOMMatrix(getComputedStyle(node).transform).a)).toBeGreaterThan(zoomBefore + 0.05);
  await page.getByRole("button", { name: "100%", exact: true }).click();
  await expect.poll(async () => zoomed.evaluate((node) => new DOMMatrix(getComputedStyle(node).transform).a)).toBeLessThan(1.02);
  await page.getByRole("button", { name: "Fit", exact: true }).click();

  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: "Auto-arrange" }).click();
  await expect(minimap).toBeHidden();
  await expect.poll(async () => {
    const fitted = await stage.boundingBox();
    const first = await nodes.first().boundingBox();
    const zoom = await page.locator(".react-flow__viewport").evaluate((node) => new DOMMatrix(getComputedStyle(node).transform).a);
    if (!fitted || !first) return false;
    return zoom >= 0.75
      && first.width >= 248 * 0.75 - 1
      && first.x >= fitted.x - 1
      && first.y >= fitted.y - 1
      && first.x + first.width <= fitted.x + fitted.width + 1
      && first.y + first.height <= fitted.y + fitted.height + 1;
  }).toBeTruthy();
  const phoneStage = await stage.boundingBox();
  const phoneInspector = await page.locator(".df2-canvas-inspector").boundingBox();
  const phoneDock = await page.getByTestId("canvas-preview").boundingBox();
  const phoneRail = await page.locator(".df2-canvas-rail").boundingBox();
  expect(phoneStage && phoneInspector && phoneDock && phoneRail).toBeTruthy();
  expect(phoneStage!.height).toBeGreaterThan(120);
  expect(phoneInspector!.height).toBeGreaterThanOrEqual(16);
  expect(phoneDock!.height).toBeGreaterThanOrEqual(48);
  expect(phoneRail!.width).toBeGreaterThanOrEqual(112);
  expect(phoneRail!.height).toBeGreaterThanOrEqual(64);
  for (let index = 0; index < count; index += 1) {
    const box = await nodes.nth(index).boundingBox();
    expect(box).toBeTruthy();
    expect(box!.width).toBeGreaterThanOrEqual(248 * 0.75 - 1);
    if (index !== 0) continue;
    expect(box!.x).toBeGreaterThanOrEqual(phoneStage!.x - 1);
    expect(box!.y).toBeGreaterThanOrEqual(phoneStage!.y - 1);
    expect(box!.x + box!.width).toBeLessThanOrEqual(phoneStage!.x + phoneStage!.width + 1);
    expect(box!.y + box!.height).toBeLessThanOrEqual(phoneStage!.y + phoneStage!.height + 1);
  }

  await page.evaluate(() => document.documentElement.setAttribute("data-theme", "dark"));
  const dark = await page.locator(".df2-canvas-node").evaluateAll((cards) => cards.map((card) => {
    const band = card.querySelector(".df2-canvas-node-band");
    const mark = card.querySelector(".df2-canvas-node-mark");
    const stripe = card.querySelector(".df2-canvas-node-stripe");
    const title = card.querySelector(".df2-canvas-node-title");
    return {
      band: band ? getComputedStyle(band).backgroundColor : "",
      stripe: stripe ? getComputedStyle(stripe).backgroundColor : "",
      mark: mark ? getComputedStyle(mark).backgroundColor : "",
      title: title ? getComputedStyle(title).color : "",
      icon: mark ? getComputedStyle(mark).color : "",
    };
  }));
  expect(new Set(dark.map((paint) => paint.stripe)).size).toBe(3);
  expect(new Set(dark.map((paint) => paint.mark)).size).toBe(3);
  for (const paint of dark) {
    expect(contrast(paint.title, paint.band)).toBeGreaterThanOrEqual(4.5);
    expect(contrast(paint.icon, paint.mark)).toBeGreaterThanOrEqual(4.5);
  }
  });

  test("every step stays clear of overlap, clipping, and a horizontal scrollbar", async ({ page }) => {
    test.setTimeout(120_000);
    const sizes = [
      { width: 1366, height: 768 },
      { width: 1440, height: 900 },
      { width: 1920, height: 1080 },
    ] as const;
    const settle = async () => {
      await page.evaluate(async () => {
        await document.fonts.ready;
        await new Promise<void>((resolve) => {
          requestAnimationFrame(() => requestAnimationFrame(() => resolve()));
        });
      });
      const shell = page.locator(".df2-transfer-studio-shell");
      await expect(shell).toBeVisible();
      let previous = "";
      await expect.poll(async () => {
        const box = await shell.boundingBox();
        const next = box
          ? `${Math.round(box.x)}:${Math.round(box.y)}:${Math.round(box.width)}:${Math.round(box.height)}`
          : "";
        const stable = next !== "" && next === previous;
        previous = next;
        return stable;
      }).toBeTruthy();
    };
    const check = async (name: string, extra?: () => Promise<void>) => {
      for (const size of sizes) {
        await page.setViewportSize(size);
        await settle();
        if (extra) await extra();
        await settle();
        await expectStudioShellClear(page, `${name} ${size.width}x${size.height}`);
      }
      await page.setViewportSize({ width: 1440, height: 900 });
      await settle();
    };

    await openStudio(page);
    await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
    await expect(page.getByRole("heading", { name: "Source", exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Destination", exact: true })).toHaveCount(0);
    await check("connect", async () => {
      const structure = page.locator(".df2-structure-preview");
      await expect(structure).toBeVisible();
      const structureBox = await structure.boundingBox();
      expect(structureBox).toBeTruthy();
      expect(structureBox!.width).toBeGreaterThanOrEqual(360);
      const cellNowrap = await page.locator(".df2-structure-table td").first().evaluate((node) => getComputedStyle(node).whiteSpace);
      expect(cellNowrap).toBe("nowrap");
      const labelFits = await page.locator(".df2-route-bar-label").first().evaluate((node) => {
        const style = getComputedStyle(node);
        return style.whiteSpace === "nowrap" && node.scrollWidth <= node.clientWidth + 1;
      });
      expect(labelFits).toBeTruthy();
    });

    await page.getByRole("button", { name: /Continue to Destination/ }).click();
    await page.getByRole("tab", { name: "File export" }).click();
    await expect(page.getByRole("button", { name: /Continue to Review/ })).toBeEnabled();
    await page.getByRole("button", { name: "Transform (optional)" }).click();
    await expect(page.getByRole("heading", { name: "Transform", exact: true })).toBeVisible();
    const trim = page.locator(".df2-canvas-op[data-op='trim']");
    await trim.click();
    await expect(page.locator(".react-flow__node")).toHaveCount(1);
    await check("transform", async () => {
      await expect(trim).toBeVisible();
      await expect(trim).toHaveJSProperty("draggable", true);
    });

    await page.getByRole("button", { name: "Run preview" }).first().click();
    await expect.poll(async () => {
      if (await page.getByTestId("validate-verdict").isVisible()) return "ready";
      const button = page.getByRole("button", { name: /Continue with this transform/ });
      if (await button.isVisible() && await button.isEnabled()) await button.click();
      return "pending";
    }, { timeout: 40_000 }).toBe("ready");
    await expect(page.locator(".df2-transfer-analyzing-panel")).toBeHidden();
    await check("review");

    await expect(page.getByRole("button", { name: /^Execute/ })).toBeVisible();

    const execute = page.getByRole("button", { name: /^Execute/ });
    if (await execute.isEnabled()) await execute.click();
    await expect(page.getByRole("heading", { name: /Transfer complete|Run/ }).first()).toBeVisible();
    await check("run");
  });

  test("connect lists destination tables after a connection is chosen", async ({ page }) => {
    let introspects = 0;
    await page.route("**/api/v1/connectors/saved", async (route) => {
      if (route.request().method() !== "GET") {
        await route.continue();
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          connectors: [{
            id: "dest-pg",
            name: "Warehouse",
            type: "postgresql",
            host: "db.internal",
            port: 5432,
            database: "analytics",
            role: "destination",
            last_test_ok: true,
          }],
        }),
      });
    });
    await page.route("**/api/v1/transfer/introspect", async (route) => {
      introspects += 1;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          source: { connected: true, columns: [], schema: {}, objects: [], message: "" },
          destination: {
            connected: true,
            columns: [],
            schema: {},
            objects: [{ name: "warehouse_orders" }],
            table_exists: null,
            message: "",
          },
        }),
      });
    });
    await openStudio(page);
    await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
    await page.getByRole("button", { name: /Continue to Destination/ }).click();
    await expect(page.getByRole("heading", { name: "Destination", exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Source", exact: true })).toHaveCount(0);
    await page.getByRole("radio", { name: /Warehouse/ }).click();
    await expect.poll(() => introspects).toBeGreaterThan(0);
    await page.locator("#dest-col").click();
    await expect(page.getByRole("option", { name: "warehouse_orders" })).toBeVisible();
  });

  test("connect source and destination are separate sections", async ({ page }) => {
    await openStudio(page);
    await expect(page.getByRole("heading", { name: "Structure preview", exact: true })).toBeVisible();
    await expect(page.getByText("Supported formats", { exact: true })).toBeVisible();
    for (const size of [
      { width: 1366, height: 768 },
      { width: 1440, height: 900 },
    ]) {
      await page.setViewportSize(size);
      await expect(page.getByRole("heading", { name: "Structure preview", exact: true })).toBeVisible();
      await expect(page.locator(".df2-source-aside-format-chip").first()).toBeVisible();
      await expect(page.locator(".df2-upload-format-chip").first()).toBeVisible();
    }
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
    await expect(page.getByRole("heading", { name: "Source", exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Destination", exact: true })).toHaveCount(0);
    await expect(page.getByRole("heading", { name: "Detected structure", exact: true })).toBeVisible();
    const transform = page.locator(".df2-flow-transform");
    const transformBox = await transform.boundingBox();
    const buttonBox = await transform.getByRole("button").boundingBox();
    expect(transformBox && buttonBox).toBeTruthy();
    expect(buttonBox!.y - transformBox!.y).toBeGreaterThanOrEqual(8);
    expect(buttonBox!.x - transformBox!.x).toBeGreaterThanOrEqual(8);
    await page.getByRole("button", { name: /Continue to Destination/ }).click();
    await expect(page.getByRole("heading", { name: "Destination", exact: true })).toBeVisible();
    await expect(page.locator(".df2-source-step")).toHaveCount(0);
    await page.getByRole("button", { name: "Back to source" }).click();
    await expect(page.getByRole("heading", { name: "Detected structure", exact: true })).toBeVisible();
  });

  test("back from transform and review returns to connect without dropping the source", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    await page.getByRole("button", { name: "Back to Connect" }).click();
    await expect(page.getByRole("heading", { name: "Transform", exact: true })).toHaveCount(0);
    await expect(page.getByRole("heading", { name: "Destination", exact: true })).toBeVisible();
    await page.getByRole("button", { name: "Back to source" }).click();
    await expect(page.getByRole("heading", { name: "Detected structure", exact: true })).toBeVisible();
    await page.getByRole("button", { name: /Continue to Destination/ }).click();
    await page.getByRole("tab", { name: "File export" }).click();
    await page.getByRole("button", { name: /Continue to Review/ }).click();
    // The map title is hidden at laptop widths; the column filter is the review landmark.
    await expect(page.getByRole("tablist", { name: "Filter columns" })).toBeVisible({ timeout: 40_000 });
    await page.getByRole("button", { name: "Back to Connect" }).click();
    await expect(page.getByRole("heading", { name: "Destination", exact: true })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Detected structure", exact: true })).toHaveCount(0);
    await page.getByRole("button", { name: "Back to source" }).click();
    await expect(page.getByRole("heading", { name: "Detected structure", exact: true })).toBeVisible();
  });

  test("recipe toolbar stays in one row with gaps at laptop widths", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    for (const size of [
      { width: 1366, height: 768 },
      { width: 1440, height: 900 },
    ]) {
      await page.setViewportSize(size);
      const buttons = page.getByRole("toolbar", { name: "Recipe actions" }).getByRole("button");
      await expect(buttons).toHaveCount(4);
      const boxes = await buttons.evaluateAll((nodes) => nodes.map((node) => {
        const box = node.getBoundingClientRect();
        return { x: box.x, y: box.y, width: box.width };
      }));
      expect(boxes[0].y).toBeGreaterThan(0);
      for (let index = 1; index < boxes.length; index += 1) {
        const gap = boxes[index].x - (boxes[index - 1].x + boxes[index - 1].width);
        expect(gap).toBeGreaterThanOrEqual(6);
        expect(Math.abs(boxes[index].y - boxes[0].y)).toBeLessThanOrEqual(2);
      }
    }
  });

  test("dragging one node leaves the other in place and the drop stays under the pointer", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    // Toasts dock in-flow in the header chrome (D-BP-20), so one expiring mid-test
    // moves the whole stage up by its 48px height. The drop offset compares a stage
    // box read earlier with a node box read later; let the sample-load toasts clear.
    await settleToasts(page);
    await setPalette(page, true);
    const stage = page.getByTestId("transform-canvas-stage");
    await page.locator(".df2-canvas-op[data-op='trim']").dragTo(stage, { targetPosition: { x: 180, y: 80 } });
    await page.locator(".df2-canvas-op[data-op='collapse_whitespace']").dragTo(stage, { targetPosition: { x: 460, y: 80 } });
    await expect(page.locator(".react-flow__node")).toHaveCount(2);
    const first = page.locator(".react-flow__node").nth(0);
    const second = page.locator(".react-flow__node").nth(1);
    const before = await first.boundingBox();
    const moving = await second.boundingBox();
    expect(before && moving).toBeTruthy();
    await page.mouse.move(moving!.x + 40, moving!.y + 36);
    await page.mouse.down();
    await page.mouse.move(moving!.x + 40, moving!.y + 120, { steps: 8 });
    const mid = await first.boundingBox();
    expect(Math.abs(mid!.x - before!.x)).toBeLessThan(2);
    expect(Math.abs(mid!.y - before!.y)).toBeLessThan(2);
    await page.mouse.up();
    const dropped = await second.boundingBox();
    expect(dropped).toBeTruthy();
    expect(Math.abs(dropped!.x - moving!.x)).toBeLessThan(24);
    expect(dropped!.y).toBeGreaterThan(moving!.y + 40);
    await page.getByRole("combobox", { name: "Zoom percent" }).selectOption("150");
    const zoomedStage = await stage.boundingBox();
    await page.locator(".df2-canvas-op[data-op='absolute']").dragTo(stage, { targetPosition: { x: 220, y: 140 } });
    const added = page.locator(".react-flow__node", { hasText: "Absolute" });
    await expect(added).toBeVisible();
    const addedBox = await added.boundingBox();
    expect(zoomedStage && addedBox).toBeTruthy();
    expect(Math.abs(addedBox!.x - (zoomedStage!.x + 220))).toBeLessThan(28);
    expect(Math.abs(addedBox!.y - (zoomedStage!.y + 140))).toBeLessThan(28);
  });

  test("flatten hover does not move the palette row", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    // A toast docked in the header shifts the palette by its 48px while it lives.
    await settleToasts(page);
    await setPalette(page, true);
    const flatten = page.locator(".df2-canvas-op[data-op='flatten_json']");
    await flatten.scrollIntoViewIfNeeded();
    const before = await flatten.boundingBox();
    await flatten.hover();
    await page.waitForTimeout(250);
    const after = await flatten.boundingBox();
    expect(before && after).toBeTruthy();
    expect(Math.abs(after!.x - before!.x)).toBeLessThan(1);
    expect(Math.abs(after!.y - before!.y)).toBeLessThan(1);
    await expect(page.getByRole("tooltip")).toBeVisible();
    const settled = await flatten.boundingBox();
    expect(Math.abs(settled!.y - before!.y)).toBeLessThan(1);
  });

  test("palette and properties do not cover each other", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    await setPalette(page, true);
    await page.locator(".df2-canvas-op[data-op='trim']").click();
    for (const size of [
      { width: 1366, height: 768 },
      { width: 1440, height: 900 },
    ]) {
      await page.setViewportSize(size);
      const rail = page.locator(".df2-canvas-rail");
      const properties = page.locator(".df2-canvas-properties");
      await expect(rail).toBeVisible();
      await expect(properties).toBeVisible();
      await expectDisjoint(page, rail, properties);
    }
  });
});

test.describe("studio flags off", () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.removeItem("df.studioSchemaBrowser");
      localStorage.removeItem("df.studioCanvas");
      localStorage.setItem("df2.theme", "light");
    });
    await page.route("**/api/v1/workspace/settings", async (route) => {
      if (route.request().method() !== "GET") {
        await route.continue();
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          org_name: "Datawrap",
          timezone: "UTC",
          retention_days: 90,
          studio_schema_browser: false,
          studio_canvas: false,
        }),
      });
    });
  });

  test("classic transform stays on the step list", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    await expect(page.getByTestId("transform-canvas")).toHaveCount(0);
    await expect(page.getByTestId("studio-canvas-opt-in")).toHaveCount(0);
    await expect(page.getByRole("heading", { name: "Steps, in order" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Suggested repairs" })).toBeVisible();
    await expect(page.getByRole("heading", { name: "Business rules", exact: true }).or(page.getByText("Business rules", { exact: true }))).toBeVisible();
    // The baseline is the settled editor. A stable "Previewing…" frame is
    // what CI compared: shorter identity mask, empty repair list.
    await expect(page.getByText("every sampled row is accounted for")).toBeVisible();
    await expect(page.locator(".df2-xform-identity")).toHaveText("No transform declared");
    await expect(page.getByText(/Parse 'order_date' as a date/)).toBeVisible();
    const add = page.getByRole("button", { name: "Add a step" });
    await expect(add).toBeVisible();
    await add.click();
    await expect(page.locator("#xform-op")).toBeVisible();
    await shoot(page, "classic-transform-light-1440.png");
  });
});

test.describe("schedules sync", () => {
  const schedule = {
    id: "sched-orders",
    name: "Nightly orders",
    source_connector_id: "src-1",
    source_table: "orders",
    dest_connector_id: "dst-1",
    dest_table: "warehouse_orders",
    interval: "daily",
    cadence_label: "Daily",
    cron: "",
    timezone: "UTC",
    sync_mode: "incremental",
    validation_mode: "strict",
    schema_policy: "manual_review",
    backfill_new_fields: false,
    cursor_column: "updated_at",
    primary_key: "id",
    cursor_value: "",
    workspace_id: "ws",
    max_retries: 2,
    retry_backoff_seconds: 30,
    notify_on_failure: true,
    notify_on_success: false,
    enabled: true,
    last_run_at: null,
    next_run_at: null,
    last_job_id: "job-parent-11",
    last_status: "completed",
    run_count: 4,
    running: false,
    created_at: "2026-10-01T00:00:00Z",
    contract_id: "contract-orders",
  };

  async function openSchedules(page: Page): Promise<void> {
    await page.route("**/api/v1/schedules/**", async (route) => {
      const url = route.request().url();
      const method = route.request().method();
      if (method === "POST" && url.includes("/sync")) {
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            ack_id: "ack-sched-1",
            preview: {
              confirm_reason: "Sync now using the signed contract",
              note: "One child job from the signed contract.",
              sync_mode: "incremental",
            },
          }),
        });
        return;
      }
      if (method === "GET" && /\/schedules\/?$/.test(url.split("?")[0])) {
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify([schedule]),
        });
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(schedule),
      });
    });
    await page.route("**/api/v1/copilot/confirm", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          ok: true,
          job_id: "job-child-91ab",
          schedule_id: schedule.id,
          idempotent: false,
        }),
      });
    });
    const childJob = {
      _id: "job-child-91ab",
      name: "Nightly orders sync",
      status: "queued",
      parent_job_id: "job-parent-11",
      source_name: "orders",
      destination_database: "warehouse",
      destination_collection: "warehouse_orders",
      sync_mode: "incremental",
      created_at: "2026-10-14T17:00:00Z",
    };
    await page.route("**/api/v1/connectors/jobs**", async (route) => {
      const url = route.request().url().split("?")[0];
      const body = /\/jobs\/?$/.test(url)
        ? { jobs: [childJob], total: 1, status_counts: { queued: 1 } }
        : childJob;
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(body),
      });
    });
    await page.goto("/#/schedules");
    const preview = page.locator("button", { hasText: "Preview workspace (UI only)" });
    await expect(preview).toBeAttached();
    await preview.evaluate((node: HTMLButtonElement) => node.click());
    await expect(page.getByRole("heading", { name: "Schedules" })).toBeVisible();
    await expect(page.getByText("Nightly orders")).toBeVisible();
  }

  test("sync now stays clear of the row and confirms in the app dialog", async ({ page }) => {
    let nativeDialog = false;
    page.on("dialog", async (dialog) => {
      nativeDialog = true;
      await dialog.dismiss();
    });
    await openSchedules(page);
    for (const width of [1366, 1440, 1920]) {
      await page.setViewportSize({ width, height: width === 1920 ? 1080 : width === 1440 ? 900 : 768 });
      const row = page.locator(".df2-pipeline-row").first();
      const sync = row.getByRole("button", { name: "Sync now" });
      const opener = row.getByRole("button", { name: "Open Nightly orders details" });
      const status = row.locator(".df2-badge").last();
      await expect(sync).toBeVisible();
      await expectDisjoint(page, sync, opener);
      await expectDisjoint(page, sync, status);
      await page.screenshot({
        path: `/opt/cursor/artifacts/schedules-row-${width}.png`,
        fullPage: false,
      });
    }
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.locator(".df2-pipeline-row").first().getByRole("button", { name: "Sync now" }).click();
    const dialog = page.getByRole("dialog", { name: "Sync now" });
    await expect(dialog).toBeVisible();
    await expect(dialog.getByText("Nightly orders", { exact: true })).toBeVisible();
    await expect(dialog.getByText("contract-orders", { exact: true })).toBeVisible();
    await expect(dialog.getByText("orders", { exact: true })).toBeVisible();
    await expect(dialog.getByText("warehouse_orders", { exact: true })).toBeVisible();
    await dialog.getByRole("button", { name: "Sync now" }).click();
    await expect(page.getByText(/Triggered from Nightly orders/)).toBeVisible();
    await expect(page).toHaveURL(/#\/jobs/);
    await expect(page.getByRole("heading", { name: "Nightly orders sync" })).toBeVisible();
    await expect(page.locator("#job-item-job-child-91ab")).toBeVisible();
    await expect(page.getByRole("button", { name: /Triggered from job-pare/ })).toBeVisible();
    expect(nativeDialog).toBe(false);
    await page.screenshot({
      path: "/opt/cursor/artifacts/schedules-sync-confirmed-1440.png",
      fullPage: false,
    });
  });
});

test.describe("studio flag opt-in", () => {
  test("workspace canvas without this browser shows a one-click opt-in", async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.removeItem("df.studioCanvas");
      localStorage.setItem("df2.theme", "light");
    });
    await page.route("**/api/v1/workspace/settings", async (route) => {
      if (route.request().method() !== "GET") {
        await route.continue();
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          org_name: "Datawrap",
          timezone: "UTC",
          retention_days: 90,
          studio_schema_browser: false,
          studio_canvas: true,
        }),
      });
    });
    await openStudio(page);
    await reachTransform(page);
    await expect(page.getByTestId("transform-canvas")).toHaveCount(0);
    await expect(page.getByTestId("studio-canvas-opt-in")).toContainText(/classic editor/);
    await page.getByRole("button", { name: "Use the recipe canvas" }).click();
    await expect(page.getByTestId("transform-canvas")).toBeVisible();
  });

  test("a failed settings read stays on the classic editor and says why", async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.setItem("df.studioCanvas", "1");
      localStorage.setItem("df2.theme", "light");
    });
    await page.route("**/api/v1/workspace/settings", async (route) => {
      if (route.request().method() !== "GET") {
        await route.continue();
        return;
      }
      await route.fulfill({ status: 503, contentType: "application/json", body: JSON.stringify({ detail: "settings down" }) });
    });
    await openStudio(page);
    await reachTransform(page);
    await expect(page.getByTestId("studio-flags-error")).toBeVisible();
    await expect(page.getByTestId("transform-canvas")).toHaveCount(0);
    await expect(page.getByRole("heading", { name: "Steps, in order" })).toBeVisible();
  });
});

test.describe("transfer wizard stepper", () => {
  const widths = [768, 1024, 1280, 1366, 1440, 1920];

  async function showStepper(page: Page, canvas: boolean, theme: "light" | "dark"): Promise<void> {
    await page.unroute("**/api/v1/workspace/settings").catch(() => undefined);
    await page.route("**/api/v1/workspace/settings", async (route) => {
      if (route.request().method() !== "GET") {
        await route.continue();
        return;
      }
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          org_name: "Datawrap",
          timezone: "UTC",
          retention_days: 90,
          studio_schema_browser: canvas,
          studio_canvas: canvas,
        }),
      });
    });
    await page.goto("/#/transfer");
    await page.evaluate(({ canvasOn, themeName }) => {
      if (canvasOn) {
        localStorage.setItem("df.studioCanvas", "1");
        localStorage.setItem("df.studioSchemaBrowser", "1");
      } else {
        localStorage.removeItem("df.studioCanvas");
        localStorage.removeItem("df.studioSchemaBrowser");
      }
      localStorage.setItem("df2.theme", themeName);
      document.documentElement.setAttribute("data-theme", themeName);
      document.documentElement.classList.toggle("df2-theme-dark", themeName === "dark");
    }, { canvasOn: canvas, themeName: theme });
    await page.reload();
    const preview = page.locator("button", { hasText: "Preview workspace (UI only)" });
    const marker = page.locator(".df2-wizard-num").first();
    await expect(preview.or(marker)).toBeAttached();
    if (await preview.count()) {
      await preview.evaluate((node: HTMLButtonElement) => node.click());
    }
    await expect(marker).toBeVisible();
    await expect(page.locator(".df2-wizard-step")).toHaveCount(6);
  }

  test("step icons sit beside their labels and separators stay clear", async ({ page }) => {
    for (const canvas of [false, true]) {
      for (const theme of ["light", "dark"] as const) {
        await showStepper(page, canvas, theme);
        if (!canvas) {
          await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
          const next = page.getByRole("button", { name: /Continue to Destination/ });
          await expect(next).toBeEnabled();
          await next.click();
          await expect(page.locator(".df2-wizard-line.done")).toHaveCount(1);
        }
        // Toasts dock in-flow inside the chrome (D-BP-20) and grow the band on
        // purpose. This test pins the idle chrome, so let them expire first.
        await expect(page.locator(".dt-toast")).toHaveCount(0, { timeout: 30_000 });
        for (const width of widths) {
          await page.setViewportSize({ width, height: width === 1920 ? 1080 : 900 });
          const where = `${canvas ? "flags" : "classic"} ${theme} ${width}`;
          const report = await page.evaluate(() => {
            const visible = (node: Element) => {
              const style = getComputedStyle(node);
              const box = node.getBoundingClientRect();
              return style.display !== "none" && style.visibility !== "hidden" && box.width > 1 && box.height > 1;
            };
            const overlaps = (a: DOMRect, b: DOMRect) =>
              a.left < b.right - 0.5 && a.right > b.left + 0.5 && a.top < b.bottom - 0.5 && a.bottom > b.top + 0.5;
            const steps = [...document.querySelectorAll(".df2-wizard-step")].filter(visible);
            const lines = [...document.querySelectorAll(".df2-wizard-line")].filter(visible);
            const probe = document.createElement("div");
            probe.style.background = "var(--df-border)";
            document.body.appendChild(probe);
            const border = getComputedStyle(probe).backgroundColor;
            probe.remove();
            const parseColor = (color: string) => {
              const match = color.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*([\d.]+))?\)/);
              if (!match) return null;
              return {
                r: Number(match[1]),
                g: Number(match[2]),
                b: Number(match[3]),
                a: match[4] === undefined ? 1 : Number(match[4]),
              };
            };
            const composite = (
              fg: { r: number; g: number; b: number; a: number },
              bg: { r: number; g: number; b: number; a: number },
            ) => ({
              r: Math.round(fg.r * fg.a + bg.r * (1 - fg.a)),
              g: Math.round(fg.g * fg.a + bg.g * (1 - fg.a)),
              b: Math.round(fg.b * fg.a + bg.b * (1 - fg.a)),
              a: 1,
            });
            const backdrop = (node: Element) => {
              const layers: { r: number; g: number; b: number; a: number }[] = [];
              let current: Element | null = node;
              while (current) {
                const parsed = parseColor(getComputedStyle(current).backgroundColor);
                if (parsed && parsed.a > 0) layers.push(parsed);
                if (parsed && parsed.a >= 1) break;
                current = current.parentElement;
              }
              let acc = layers.length ? layers[layers.length - 1] : { r: 255, g: 255, b: 255, a: 1 };
              for (let i = layers.length - 2; i >= 0; i -= 1) acc = composite(layers[i], acc);
              return `rgb(${acc.r}, ${acc.g}, ${acc.b})`;
            };
            const track = document.querySelector(".df2-wizard-track");
            const chromeEl = document.querySelector(".df2-transfer-studio-chrome");
            const chromeBox = chromeEl?.getBoundingClientRect();
            const pairs = steps.map((step, index) => {
              const icon = step.querySelector(".df2-wizard-num");
              const label = [...step.querySelectorAll(".df2-wizard-label, .df2-wizard-label-short")].find(visible);
              if (!icon || !label) return null;
              const iconBox = icon.getBoundingClientRect();
              const labelBox = label.getBoundingClientRect();
              const line = lines[index];
              const lineBox = line ? line.getBoundingClientRect() : null;
              const nextIcon = steps[index + 1]?.querySelector(".df2-wizard-num")?.getBoundingClientRect();
              const style = getComputedStyle(label);
              const range = document.createRange();
              range.selectNodeContents(label);
              const textWidth = range.getBoundingClientRect().width;
              return {
                text: (label.textContent || "").trim(),
                iconHitsLabel: overlaps(iconBox, labelBox),
                gap: labelBox.left - iconBox.right,
                align: (labelBox.top + labelBox.height / 2) - (iconBox.top + iconBox.height / 2),
                lineHits: lineBox ? overlaps(lineBox, labelBox) || overlaps(lineBox, iconBox) : false,
                lineGap: lineBox ? lineBox.left - labelBox.right : null,
                nextGap: lineBox && nextIcon ? nextIcon.left - lineBox.right : null,
                lineMid: lineBox ? lineBox.top + lineBox.height / 2 : null,
                iconMid: iconBox.top + iconBox.height / 2,
                color: line ? getComputedStyle(line).backgroundColor : "",
                done: line ? line.classList.contains("done") : false,
                position: style.position,
                ink: style.color,
                weight: style.fontWeight,
                backdrop: backdrop(label),
                active: step.classList.contains("active"),
                textWidth,
                labelWidth: labelBox.width,
                clipped: textWidth > labelBox.width + 1,
                pastChrome: chromeBox ? labelBox.right > chromeBox.right + 1 : false,
              };
            }).filter((pair) => pair !== null);
            const chrome = chromeBox?.height ?? 0;
            const trackOverflow = track
              ? track.scrollWidth > track.clientWidth + 1
              : false;
            return { steps: steps.length, lines: lines.length, pairs, border, chrome, trackOverflow };
          });
          expect(report.steps, where).toBe(6);
          expect(report.lines, where).toBe(report.steps - 1);
          // Idle classic chrome: the stepper row (54px below 1440, 58px from 1440,
          // where the full names and the larger marker sit) plus the 8px the empty
          // toast dock holds as a row of the chrome grid (D-BP-20; the screenshot
          // baselines are taken with that row). At 1280 and below the route bar
          // stacks under the stepper (D-BP-23): 86px plus the dock row. Flags-on
          // chrome is the 44px studio shell and must not grow.
          expect(report.chrome, where).toBe(canvas ? 44 : width <= 1280 ? 94 : width >= 1440 ? 66 : 62);
          for (const pair of report.pairs) {
            expect(pair.iconHitsLabel, `${where} ${pair.text}`).toBe(false);
            expect(pair.position, `${where} ${pair.text}`).toBe("static");
            expect(pair.gap, `${where} ${pair.text}`).toBeGreaterThanOrEqual(5);
            expect(pair.gap, `${where} ${pair.text}`).toBeLessThanOrEqual(7);
            expect(Math.abs(pair.align), `${where} ${pair.text}`).toBeLessThanOrEqual(1);
            expect(pair.lineHits, `${where} ${pair.text}`).toBe(false);
            if (pair.lineGap != null) expect(pair.lineGap, `${where} ${pair.text}`).toBeGreaterThanOrEqual(8);
            if (pair.nextGap != null) expect(pair.nextGap, `${where} ${pair.text}`).toBeGreaterThanOrEqual(8);
            if (pair.lineMid != null) expect(Math.abs(pair.lineMid - pair.iconMid), `${where} ${pair.text}`).toBeLessThanOrEqual(1);
            // Done lines paint from the --df-logo-ink token (enterprise-ui
            // .df2-wizard-line.done) — assert the resolved token, not a stale
            // hex (UI-11).
            if (pair.done) expect(pair.color, where).toBe(await tokenColor(page, "--df-logo-ink"));
            else if (pair.color) expect(pair.color, where).toBe(report.border);
          }
          const lineGaps = report.pairs.map((pair) => pair.lineGap).filter((gap) => gap != null);
          const spread = Math.max(...lineGaps) - Math.min(...lineGaps);
          expect(spread, where).toBeLessThanOrEqual(1);
          expect(report.trackOverflow, where).toBe(false);
          // Flags-on shows the same six-step flow (P1-3), not a Connect/Review/Run trio.
          const fullNames = ["Source", "Destination", "Transform", "Map", "Validate", "Run"];
          expect(report.pairs.map((pair) => pair.text), where).toEqual(fullNames);
          const active = report.pairs.find((pair) => pair.active);
          const inactive = report.pairs.find((pair) => !pair.active);
          expect(active, where).toBeTruthy();
          expect(inactive, where).toBeTruthy();
          expect(Number(active?.weight), where).toBeGreaterThan(Number(inactive?.weight));
          expect(active?.ink, where).not.toBe(inactive?.ink);
          for (const pair of report.pairs) {
            expect(pair.clipped, `${where} ${pair.text} text ${pair.textWidth} box ${pair.labelWidth}`).toBe(false);
            expect(pair.pastChrome, `${where} ${pair.text}`).toBe(false);
            expect(contrast(pair.ink, pair.backdrop), `${where} ${pair.text} ${pair.ink} on ${pair.backdrop}`).toBeGreaterThanOrEqual(4.5);
          }
        }
      }
    }
  });
});
