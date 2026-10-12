import { execFileSync } from "node:child_process";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test, type Page } from "@playwright/test";
import { openStudio, reachTransform, shoot } from "./support";

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

  test("canvas adds a divert step and undo removes it", async ({ page }) => {
    await openStudio(page);
    await reachTransform(page);
    await expect(page.getByTestId("transform-canvas")).toBeVisible();
    await expect(page.getByRole("heading", { name: "Steps, in order" })).toHaveCount(0);

    await page.getByLabel("Divert condition").fill("[status] = 'void'");
    await page.getByRole("button", { name: "Divert rows" }).click();
    await expect(page.locator(".react-flow__node")).toHaveCount(1);
    await page.getByRole("button", { name: "Undo" }).click();
    await expect(page.locator(".react-flow__node")).toHaveCount(0);

    const primary = page.locator(".df2-studio-next .df2-btn-primary").first();
    await expect(primary).toBeVisible();
    const color = await primary.evaluate((node) => getComputedStyle(node).backgroundColor);
    expect(color).toBe("rgb(10, 61, 58)");

    await shoot(page, "canvas-light-1440.png");
    await page.setViewportSize({ width: 390, height: 844 });
    await shoot(page, "canvas-light-390.png");
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.evaluate(() => {
      localStorage.setItem("df2.theme", "dark");
      document.documentElement.setAttribute("data-theme", "dark");
      document.documentElement.classList.add("df2-theme-dark");
    });
    await shoot(page, "canvas-dark-1440.png");
    await page.setViewportSize({ width: 390, height: 844 });
    await shoot(page, "canvas-dark-390.png");
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
        database: "studio_src",
        schema: "sales",
        connection_string: `sqlite:///${dbPath}`,
      },
    });
    expect(created.ok()).toBeTruthy();

    await openStudio(page);
    await page.getByRole("radio", { name: "Database" }).click();
    await page.getByLabel("Source connector").selectOption({ label: "Studio SQLite" });
    const browser = page.getByTestId("studio-schema-browser");
    await expect(browser).toBeVisible();
    await expect(browser.getByRole("button", { name: "sales" })).toBeVisible();
    await expect(browser.getByText("customers")).toBeVisible();
    await expect(page.getByTestId("transform-canvas")).toHaveCount(0);
  });
});
