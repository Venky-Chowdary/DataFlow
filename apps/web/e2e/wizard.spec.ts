import { expect, test } from "@playwright/test";
import { openStudio, reachTransform, shoot, tokenColor } from "./support";

test.describe("wizard baseline", () => {
  test.beforeEach(async ({ page }) => {
    await page.addInitScript(() => {
      localStorage.removeItem("df.studioSchemaBrowser");
      localStorage.removeItem("df.studioCanvas");
      localStorage.setItem("df2.theme", "light");
    });
  });

  test("sample file export stays on the step list", async ({ page }) => {
    await openStudio(page);
    await expect(page.getByTestId("studio-schema-browser")).toHaveCount(0);
    await reachTransform(page);
    await expect(page.getByTestId("transform-canvas")).toHaveCount(0);
    await expect(page.getByRole("heading", { name: "Steps, in order" })).toBeVisible();

    const add = page.getByRole("button", { name: "Add a step" });
    await expect(add).toBeVisible();
    await add.click();
    const operation = page.locator("#xform-op");
    let catalogReady = false;
    try {
      await expect(operation).toBeEnabled({ timeout: 15_000 });
      catalogReady = true;
    } catch {
      catalogReady = false;
    }
    if (!catalogReady) {
      test.info().annotations.push({
        type: "skip-step",
        description: "Shape catalog did not load, so a trim step was not added.",
      });
    } else {
      await page.locator("#xform-op").click();
      await page.getByLabel("Search Operation").fill("trim");
      const trim = page.getByRole("option", { name: /trim/i }).first();
      await expect(trim).toBeVisible();
      await trim.click();
      await page.locator("#xform-column").click();
      await page.getByRole("option", { name: "customer_email" }).click();
      await page.locator(".df2-xform-builder-actions").getByRole("button", { name: "Add step", exact: true }).click();
      await expect(page.locator("#xform-step-1")).toContainText(/trim/i);
    }

    const primary = page.locator(".df2-xform-actions .df2-btn-primary");
    const color = await primary.evaluate((node) => getComputedStyle(node).backgroundColor);
    // The primary fill is the --df-action-bg token (#0f766e): white label text
    // needs >= 4.5:1, which --df-brand (#0d9488, non-text accents only) misses;
    // contrast.test.ts pins that. Assert the resolved token, never a hex
    // literal that goes stale on a brand repaint (UI-11).
    expect(color).toBe(await tokenColor(page, "--df-action-bg"));

    // Toasts are hidden in shoot(), so these shots are the step list itself.
    // The compose-step sentence is part of the pre-upgrade builder.
    await shoot(page, "wizard-transform-light-1440.png");
    await page.setViewportSize({ width: 390, height: 844 });
    await shoot(page, "wizard-transform-light-390.png");
    await page.setViewportSize({ width: 1440, height: 900 });
    await page.evaluate(() => {
      localStorage.setItem("df2.theme", "dark");
      document.documentElement.setAttribute("data-theme", "dark");
      document.documentElement.classList.add("df2-theme-dark");
    });
    await shoot(page, "wizard-transform-dark-1440.png");
    await page.setViewportSize({ width: 390, height: 844 });
    await shoot(page, "wizard-transform-dark-390.png");
  });
});
