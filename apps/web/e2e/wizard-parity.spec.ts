import { expect, test, type Page } from "@playwright/test";
import { expectDisjoint, expectWithinViewport } from "./layout";
import { isolateDrafts, openStudio, settleInput, settleToasts } from "./support";

/**
 * Flags-off pixels against baselines taken from 873bec36.
 * Generate those baselines with PLAYWRIGHT_BASE_URL pointed at that commit.
 * A non-zero diff here means the classic screen moved.
 *
 * The destination overflow count follows GET /transfer/capabilities.
 * A fresh API reports 101 destination engines (+88 in the dropdown).
 * An older process that still had 100 engines painted +87.
 */

const VIEWS = [
  { width: 1440, height: 900, theme: "light", tag: "light-1440" },
  { width: 390, height: 844, theme: "light", tag: "light-390" },
  { width: 1440, height: 900, theme: "dark", tag: "dark-1440" },
  { width: 390, height: 844, theme: "dark", tag: "dark-390" },
] as const;

async function paintTheme(page: Page, theme: "light" | "dark"): Promise<void> {
  await page.evaluate((next) => {
    localStorage.setItem("df2.theme", next);
    document.documentElement.setAttribute("data-theme", next);
    document.documentElement.classList.toggle("df2-theme-dark", next === "dark");
  }, theme);
}

async function still(page: Page): Promise<void> {
  await page.addStyleTag({
    content: "*,*::before,*::after{animation:none!important;transition:none!important;caret-color:transparent!important}",
  });
  await page.evaluate(() => new Promise((resolve) => requestAnimationFrame(() => requestAnimationFrame(() => resolve(undefined)))));
}

async function shootSurface(
  page: Page,
  locator: ReturnType<Page["locator"]>,
  name: string,
  forceElement = false,
): Promise<void> {
  await settleToasts(page);
  await settleInput(page);
  const mask = [
    page.locator(".df2-xform-identity"),
    page.locator(".df2-nav-badge"),
    page.locator(".df2-sidebar-user"),
  ];
  const width = page.viewportSize()?.width ?? 1440;
  const options = { mask, maxDiffPixelRatio: 0 };
  if (!forceElement && width < 500) {
    await expect(page).toHaveScreenshot(name, options);
    return;
  }
  await expect(locator).toHaveScreenshot(name, options);
}

test.describe("flags-off parity", () => {
  test.beforeEach(async ({ page }) => {
    await isolateDrafts(page);
    await page.addInitScript(() => {
      localStorage.removeItem("df.studioSchemaBrowser");
      localStorage.removeItem("df.studioCanvas");
      localStorage.setItem("df2.theme", "light");
    });
    await page.route("**/auth/sso/providers", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ providers: [] }),
      });
    });
  });

  test("login matches the pre-upgrade screen", async ({ page }) => {
    await page.goto("/#/transfer");
    await expect(page.getByRole("heading", { name: "Sign in to governed transfers" })).toBeVisible();
    await still(page);
    for (const view of VIEWS) {
      await page.setViewportSize({ width: view.width, height: view.height });
      await paintTheme(page, view.theme);
      await shootSurface(page, page.locator("body"), `parity-login-${view.tag}.png`);
    }
  });

  test("source, destination, and query match the pre-upgrade screens", async ({ page }) => {
    await page.route("**/api/v1/connectors/saved", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ connectors: [] }),
      });
    });
    await page.route("**/api/v1/connectors/", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ connectors: [] }),
      });
    });
    await openStudio(page);
    await still(page);
    // The source step paints the API's capability probe (OCR line, source types).
    // Until it answers the OCR line reads "Optional", so a capture taken early
    // differs from one taken after. Wait for the probe before any capture.
    await expect(page.locator("#df2-source-ocr-status")).not.toHaveText(/^Optional/, { timeout: 30_000 });
    await expect(page.getByTestId("studio-schema-browser")).toHaveCount(0);
    await expect(page.getByTestId("studio-schema-opt-in")).toHaveCount(0);
    await expect(page.getByTestId("studio-flags-error")).toHaveCount(0);
    await expect(page.locator(".df2-studio-next")).toHaveCount(0);

    for (const view of VIEWS) {
      await page.setViewportSize({ width: view.width, height: view.height });
      await paintTheme(page, view.theme);
      await expect(page.getByTestId("studio-schema-browser")).toHaveCount(0);
      await expectDisjoint(
        page,
        page.getByRole("navigation", { name: "Transfer steps" }),
        page.getByRole("button", { name: /Load sample orders CSV/ }),
      );
      await expectWithinViewport(page, "nav.df2-wizard");
      await shootSurface(page, page.locator(".df2-source-step"), `parity-source-${view.tag}.png`);
    }

    await page.setViewportSize({ width: 1440, height: 900 });
    await paintTheme(page, "light");
    await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
    const cont = page.getByRole("button", { name: /Continue to Destination/ });
    await expect(cont).toBeEnabled();
    await cont.click();
    await expect(page.getByRole("tab", { name: "File export" })).toBeVisible();
    await expect(page.locator(".df2-studio-next")).toHaveCount(0);

    for (const view of VIEWS) {
      await page.setViewportSize({ width: view.width, height: view.height });
      await paintTheme(page, view.theme);
      await expectDisjoint(
        page,
        page.getByRole("navigation", { name: "Transfer steps" }),
        page.getByRole("tab", { name: "File export" }),
      );
      await shootSurface(page, page.locator(".df2-dest-step"), `parity-destination-${view.tag}.png`);
    }

  });

  test("query matches the pre-upgrade screen", async ({ page }) => {
    await page.route("**/api/v1/connectors/saved", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ connectors: [] }),
      });
    });
    await openStudio(page);
    await page.locator(".df2-nav").getByRole("button", { name: "Query", exact: true }).click();
    await expect(page.getByText("Add a connector to run queries")).toBeVisible();
    await still(page);
    for (const view of VIEWS) {
      await page.setViewportSize({ width: view.width, height: view.height });
      await paintTheme(page, view.theme);
      await still(page);
      await shootSurface(page, page.locator(".df2-page-query"), `parity-query-${view.tag}.png`);
    }
  });

  test("settings keeps the pre-upgrade fields and the flag gate does not cover them", async ({ page }) => {
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
    await openStudio(page);
    await page.evaluate(() => {
      window.location.hash = "#/settings";
    });
    await expect(page.getByTestId("settings-appearance")).toBeVisible();
    await expect(page.locator("#org-name")).toHaveValue("Datawrap");
    await expect(page.locator("#retention")).toHaveValue("90");
    await still(page);

    const org = page.locator("#org-name").locator("xpath=ancestor::div[contains(@class,'df2-settings-field')]");
    const zone = page.locator("#timezone").locator("xpath=ancestor::div[contains(@class,'df2-settings-field')]");
    const keep = page.locator("#retention").locator("xpath=ancestor::div[contains(@class,'df2-settings-field')]");

    for (const view of VIEWS) {
      await page.setViewportSize({ width: view.width, height: view.height });
      if (view.theme === "dark") {
        await page.getByRole("tab", { name: "Dark" }).click();
      } else {
        await page.getByRole("tab", { name: "Light" }).click();
      }
      await still(page);
      await shootSurface(page, page.getByTestId("settings-appearance"), `parity-settings-appearance-${view.tag}.png`, true);
      await shootSurface(page, org, `parity-settings-org-${view.tag}.png`, true);
      await shootSurface(page, zone, `parity-settings-timezone-${view.tag}.png`, true);
      await shootSurface(page, keep, `parity-settings-retention-${view.tag}.png`, true);
    }

    const schema = page.locator("#studio-schema-browser");
    const canvas = page.locator("#studio-canvas");
    // 873bec36 has no flag gate. Head adds two unchecked rows under retention.
    if (await schema.count() === 0) return;
    await expect(schema).not.toBeChecked();
    await expect(canvas).not.toBeChecked();
    const schemaField = schema.locator("xpath=ancestor::div[contains(@class,'df2-settings-field')]");
    const canvasField = canvas.locator("xpath=ancestor::div[contains(@class,'df2-settings-field')]");
    const save = page.getByTestId("settings-general-save");
    await schemaField.scrollIntoViewIfNeeded();
    await expectDisjoint(page, keep, schemaField);
    await expectDisjoint(page, schemaField, canvasField);
    await save.scrollIntoViewIfNeeded();
    await expectDisjoint(page, canvasField, save);
    await expectDisjoint(page, schemaField, save);
  });
});
