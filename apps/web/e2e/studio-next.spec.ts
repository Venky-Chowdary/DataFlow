import { execFileSync } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test, type Page } from "@playwright/test";
import { expectDisjoint, expectHitInside, expectStackedAbove, expectWithinViewport } from "./layout";
import { openStudio, reachTransform, shoot } from "./support";

/** A toast shifts the narrow page. Remove it before the shot; the mask alone does not. */
async function quietChrome(page: Page): Promise<void> {
  await page.locator(".dt-toast").evaluateAll((nodes) => {
    for (const node of nodes) (node as HTMLElement).style.setProperty("display", "none");
  });
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
        [".df2-canvas-rail", "[data-testid='transform-canvas-stage']"],
        ["[data-testid='transform-canvas-stage']", "[data-testid='canvas-preview']"],
        ["[data-testid='canvas-preview']", ".df2-xform-actions"],
        ["[data-testid='transform-canvas']", ".df2-xform-actions"],
        [".df2-canvas-rail", ".df2-canvas-inspector"],
        ["[data-testid='transform-canvas-stage']", ".df2-canvas-inspector"],
        ["[data-testid='canvas-preview']", ".df2-canvas-inspector"],
        ["[aria-label='Transfer steps']", ".df2-xform-actions"],
        [".df2-canvas-head", ".df2-xform-actions"],
      ];
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

    const drop = { x: 220, y: 180 };
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
    expect(moving).toBeTruthy();
    await page.mouse.move(moving!.x + 40, moving!.y + 24);
    await page.mouse.down();
    await page.mouse.move(moving!.x + 40, moving!.y - 200, { steps: 15 });
    await page.mouse.up();
    await expect(page.locator(".df2-canvas-node").nth(0)).toHaveAttribute("data-op", "collapse_whitespace");

    const source = page.locator(".react-flow__node").nth(0).locator(".react-flow__handle-bottom");
    const target = page.locator(".react-flow__node").nth(1).locator(".react-flow__handle-top");
    await source.dragTo(target);
    await expect(page.getByText(/Connected step 1 to step 2/)).toBeVisible();
    await source.dragTo(page.locator(".react-flow__node").nth(0).locator(".react-flow__handle-top"));
    await expect(page.getByRole("alert").filter({ hasText: /itself/ })).toBeVisible();

    const lower = page.locator(".df2-canvas-op[data-op='absolute']");
    const liveStage = await stage.boundingBox();
    const firstBox = await page.locator(".react-flow__node").nth(0).boundingBox();
    const nextBox = await page.locator(".react-flow__node").nth(1).boundingBox();
    expect(firstBox && nextBox && liveStage).toBeTruthy();
    const midX = (firstBox!.x + firstBox!.width / 2) - liveStage!.x;
    const gap = nextBox!.y - (firstBox!.y + firstBox!.height);
    const midY = (firstBox!.y + firstBox!.height + Math.max(gap, 8) / 2) - liveStage!.y;
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
    await page.locator(".df2-canvas-op[data-op='trim']").dragTo(page.getByTestId("transform-canvas-stage"), {
      targetPosition: { x: 220, y: 180 },
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
