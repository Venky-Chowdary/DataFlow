import { execFileSync } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test, type Page } from "@playwright/test";
import { expectDisjoint, expectHitInside, expectStackedAbove, expectWithinViewport } from "./layout";
import { openStudio, reachTransform, shoot } from "./support";

/** A toast shifts the narrow page. Remove it before the shot; the mask alone does not. */
async function setPalette(page: Page, open: boolean): Promise<void> {
  const toggle = page.getByRole("button", { name: "Palette", exact: true });
  const expanded = (await toggle.getAttribute("aria-expanded")) === "true";
  if (expanded !== open) await toggle.click();
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
  await page.locator("#canvas-opt-condition-field").click();
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
    await page.getByRole("button", { name: "Rules and suggestions" }).click();
    await expect(page.getByRole("heading", { name: "Suggested repairs" })).toBeVisible();
    await expectStackedAbove(page, ".df2-xform-actions", ".df2-drawer-overlay");
    await expectWithinViewport(page, ".df2-drawer-overlay .df2-drawer", 8);
    await expectHitInside(page, page.locator(".df2-drawer-close"), ".df2-drawer");
    await page.locator(".df2-drawer-close").click();

    await quietChrome(page);
    await page.getByRole("button", { name: "Templates" }).click();
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
    await page.getByRole("button", { name: "Templates" }).click();
    await expect(page.getByRole("dialog", { name: "Templates" })).toBeVisible();
    await expectStackedAbove(page, ".df2-xform-actions", ".df2-modal-overlay");
    await expectWithinViewport(page, ".df2-modal");
    await page.getByRole("dialog", { name: "Templates" }).getByRole("button", { name: "Close" }).click();
    await page.getByRole("button", { name: "Rules and suggestions" }).click();
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

    await page.getByRole("button", { name: "Rules and suggestions" }).click();
    await expect(page.getByRole("heading", { name: "Suggested repairs" })).toBeVisible();
    await page.getByRole("button", { name: "Close" }).click();
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

    await page.getByRole("button", { name: "Run preview" }).click();
    await expect(page.getByTestId("canvas-preview").locator("tbody tr").first()).toBeVisible({ timeout: 20_000 });
    const continueRecipe = page.getByRole("button", { name: "Continue with this transform" });
    await expect(continueRecipe).toBeEnabled();
    const continueFit = await continueRecipe.evaluate((node) => node.scrollWidth <= node.clientWidth + 1);
    expect(continueFit).toBeTruthy();

    const primary = page.locator(".df2-studio-next .df2-btn-primary").first();
    await expect(primary).toBeVisible();
    const color = await primary.evaluate((node) => getComputedStyle(node).backgroundColor);
    expect(color).toBe("rgb(10, 61, 58)");
  });

  test("sample export runs from the canvas recipe", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    await setPalette(page, true);
    await page.locator(".df2-canvas-op[data-op='trim']").dragTo(page.getByTestId("transform-canvas-stage"), {
      targetPosition: { x: 400, y: 160 },
    });
    await expect(page.locator(".react-flow__node")).toHaveCount(1);
    await page.getByRole("button", { name: "Run preview" }).click();
    await expect(page.getByTestId("canvas-preview").locator("tbody tr").first()).toBeVisible({ timeout: 20_000 });
    const toMap = page.getByRole("button", { name: "Continue with this transform" });
    await expect(toMap).toBeEnabled();
    await toMap.click();
    const toValidate = page.getByRole("button", { name: /Continue to Validate/ });
    await expect(toValidate).toBeEnabled({ timeout: 20_000 });
    await toValidate.click();
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
    const proceed = page.getByRole("button", { name: /Continue to Destination/ });
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
    await expect(node).toContainText("Valid");
    await expect(node).toContainText("Remove leading and trailing whitespace");
    await expect(page.locator(".react-flow__minimap")).toBeVisible();
    await page.getByRole("button", { name: "Fit view", exact: true }).click();
    await page.getByRole("button", { name: "Arrange" }).click();
    await expect(node).toBeVisible();
    await page.getByLabel("Search operations").fill("join");
    const join = page.locator(".df2-canvas-op[data-op='join']").filter({ visible: true });
    await expect(join).toHaveAttribute("data-executable", "false");
    await expect(join).toBeDisabled();
    await expect(join).toHaveAttribute("data-icon", "join");
    await page.getByRole("button", { name: "Preview this step" }).click();
    await expect(page.getByTestId("canvas-node-preview")).toContainText(/sample|Preview|row/i);
    await expect(page.getByTestId("transform-canvas")).toHaveAttribute("data-document", "chain");
    await expect(page.getByTestId("transform-canvas")).toHaveAttribute("data-set-hash", "");

    await node.getByRole("button", { name: "Show fields" }).click();
    await expect(node.locator(".df2-canvas-field")).toHaveCount(2);
    await expect(node.getByRole("button", { name: "Hide fields" })).toHaveAttribute("aria-expanded", "true");
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
    await expect(page.getByRole("button", { name: "Step 3 Invalid" })).toBeVisible();
    await setPalette(page, false);
    await page.locator(".df2-canvas-node[data-index='0']").click();
    await page.getByRole("button", { name: /^Validation/ }).click();
    const validation = page.getByRole("region", { name: "Validation" });
    await expect(validation).toContainText(/order_id/);
    await validation.getByRole("button", { name: /Step 3/ }).click();
    await expect(page.locator(".df2-canvas-node.is-selected[data-index='2']")).toBeVisible();
    await expect(page.locator("#canvas-inspector")).toBeFocused();
    await expect.poll(() => nodeCenterGap(page, 2)).toBeLessThan(80);
    await page.getByRole("button", { name: "Fit view", exact: true }).click();
    await page.locator(".df2-canvas-node[data-index='0'] .df2-canvas-node-title").click();
    const properties = page.getByRole("button", { name: "Properties", exact: true });
    if ((await properties.getAttribute("aria-expanded")) === "true") await properties.click();
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
    await page.evaluate(() => document.documentElement.setAttribute("data-theme", "dark"));
    const trim = page.locator(".df2-canvas-op[data-op='trim']");
    await expect(trim).toBeVisible();
    await trim.click();
    await expect(page.locator(".df2-canvas-node").first()).toContainText("Valid");
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
  await page.getByRole("button", { name: "Arrange" }).click();
  const stage = page.getByTestId("transform-canvas-stage");
  const minimap = page.locator(".react-flow__minimap");
  await expect(minimap).toBeVisible();
  const stageBox = await stage.boundingBox();
  const mapBox = await minimap.boundingBox();
  expect(stageBox && mapBox).toBeTruthy();
  const nodes = page.locator(".react-flow__node");
  const count = await nodes.count();
  const scale = await page.locator(".react-flow__viewport").evaluate((node) => new DOMMatrix(getComputedStyle(node).transform).a);
  expect(scale).toBeGreaterThanOrEqual(0.85);
  for (let index = 0; index < count; index += 1) {
    const box = await nodes.nth(index).boundingBox();
    expect(box).toBeTruthy();
    expect(box!.width).toBeGreaterThanOrEqual(248 * 0.85 - 1);
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
    const title = card.querySelector(".df2-canvas-node-title");
    const bandStyle = band ? getComputedStyle(band) : null;
    const markStyle = mark ? getComputedStyle(mark) : null;
    const titleStyle = title ? getComputedStyle(title) : null;
    return {
      band: bandStyle?.backgroundColor ?? "",
      mark: markStyle?.backgroundColor ?? "",
      title: titleStyle?.color ?? "",
      icon: markStyle?.color ?? "",
    };
  }));
  expect(new Set(paints.map((paint) => paint.band)).size).toBe(3);
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
  expect(inspector!.height).toBeGreaterThanOrEqual(120);

  await page.getByRole("button", { name: "Validation", exact: true }).click();
  await expect(page.getByRole("region", { name: "Validation" })).toContainText("No issues");
  await page.getByRole("button", { name: "Validation", exact: true }).click();
  await page.getByRole("button", { name: "Focus canvas" }).click();
  await expect(page.locator(".df2-sidebar")).toBeHidden();
  await expect(page.locator(".df2-transfer-studio-chrome")).toBeHidden();
  await page.getByRole("button", { name: "Exit focus" }).click();
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
  const zoomToFit = page.getByRole("button", { name: "Zoom to 100%" });
  await expect(zoomToFit).not.toHaveText("100%");
  await zoomToFit.click();
  await expect.poll(async () => zoomed.evaluate((node) => new DOMMatrix(getComputedStyle(node).transform).a)).toBeLessThan(1.02);
  await expect(zoomToFit).toHaveText("100%");
  await page.getByRole("button", { name: "Fit view", exact: true }).click();

  await page.setViewportSize({ width: 390, height: 844 });
  await page.getByRole("button", { name: "Arrange" }).click();
  await expect(minimap).toBeHidden();
  await expect.poll(async () => {
    const fitted = await stage.boundingBox();
    const first = await nodes.first().boundingBox();
    const zoom = await page.locator(".react-flow__viewport").evaluate((node) => new DOMMatrix(getComputedStyle(node).transform).a);
    if (!fitted || !first) return false;
    return zoom >= 0.85
      && first.width >= 248 * 0.85 - 1
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
  expect(phoneInspector!.height).toBeGreaterThanOrEqual(72);
  expect(phoneDock!.height).toBeGreaterThanOrEqual(48);
  expect(phoneRail!.width).toBeGreaterThanOrEqual(112);
  expect(phoneRail!.height).toBeGreaterThanOrEqual(phoneInspector!.height);
  for (let index = 0; index < count; index += 1) {
    const box = await nodes.nth(index).boundingBox();
    expect(box).toBeTruthy();
    expect(box!.width).toBeGreaterThanOrEqual(248 * 0.85 - 1);
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
    const title = card.querySelector(".df2-canvas-node-title");
    return {
      band: band ? getComputedStyle(band).backgroundColor : "",
      mark: mark ? getComputedStyle(mark).backgroundColor : "",
      title: title ? getComputedStyle(title).color : "",
      icon: mark ? getComputedStyle(mark).color : "",
    };
  }));
  expect(new Set(dark.map((paint) => paint.band)).size).toBe(3);
  expect(new Set(dark.map((paint) => paint.mark)).size).toBe(3);
  for (const paint of dark) {
    expect(contrast(paint.title, paint.band)).toBeGreaterThanOrEqual(4.5);
    expect(contrast(paint.icon, paint.mark)).toBeGreaterThanOrEqual(4.5);
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
    const add = page.getByRole("button", { name: "Add a step" });
    await expect(add).toBeVisible();
    await add.click();
    await expect(page.locator("#xform-op")).toBeVisible();
    await shoot(page, "classic-transform-light-1440.png");
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
