import { expect, type Page } from "@playwright/test";

export async function openStudio(page: Page, hash = "/#/transfer"): Promise<void> {
  await page.goto(hash);
  // The dev preview control stays in the DOM and is hidden so Sign in remains
  // above the fold. The test uses that existing control.
  const preview = page.locator("button", { hasText: "Preview workspace (UI only)" });
  await expect(preview).toBeAttached();
  await preview.evaluate((node: HTMLButtonElement) => node.click());
  await expect(page.getByRole("button", { name: /Load sample orders CSV/ })).toBeVisible();
}

export async function reachTransform(page: Page): Promise<void> {
  await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
  const cont = page.getByRole("button", { name: /Continue to Destination/ });
  await expect(cont).toBeEnabled();
  await cont.click();
  await page.getByRole("tab", { name: "File export" }).click();
  const next = page.getByRole("button", { name: /Continue to Transform/ });
  await expect(next).toBeEnabled();
  await next.click();
  await expect(page.getByRole("heading", { name: "Transform", exact: true })).toBeVisible();
}

export async function shoot(page: Page, name: string): Promise<void> {
  const panel = page.locator(".df2-xform");
  await panel.evaluate((node) => {
    node.scrollTop = 0;
  });
  const mask = [page.locator(".dt-toast"), page.locator(".df2-xform-identity")];
  const width = page.viewportSize()?.width ?? 1440;
  // Element screenshots of the scroll host come back blank under 500px.
  // The viewport shot is the one that shows the step.
  if (width < 500) {
    await expect(page).toHaveScreenshot(name, { mask });
    return;
  }
  await expect(panel).toHaveScreenshot(name, { mask });
}
