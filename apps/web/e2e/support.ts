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

export async function shoot(
  page: Page,
  name: string,
  extraMask: ReturnType<Page["locator"]>[] = [],
  maxDiffPixelRatio?: number,
): Promise<void> {
  const panel = page.locator(".df2-xform");
  await panel.evaluate((node) => {
    node.scrollTop = 0;
  });
  // Toasts are fixed overlays. Masking them paints a magenta box into the
  // baseline, so a toast that is present in one run and absent in the next
  // fails the classic screen even when the step list did not move. Hide them
  // before the shot. The transform id stays masked because the hash changes.
  await page.locator(".dt-toast").evaluateAll((nodes) => {
    for (const node of nodes) (node as HTMLElement).style.setProperty("display", "none");
  });
  const mask = [page.locator(".dt-toast"), page.locator(".df2-xform-identity"), ...extraMask];
  const options = maxDiffPixelRatio == null ? { mask } : { mask, maxDiffPixelRatio };
  const width = page.viewportSize()?.width ?? 1440;
  // Element screenshots of the scroll host come back blank under 500px.
  // The viewport shot is the one that shows the step.
  if (width < 500) {
    await expect(page).toHaveScreenshot(name, options);
    return;
  }
  await expect(panel).toHaveScreenshot(name, options);
}
