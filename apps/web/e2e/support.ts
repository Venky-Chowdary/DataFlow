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
  const classic = page.getByRole("button", { name: /Continue to Destination/ });
  const review = page.getByRole("button", { name: /Continue to Review/ });
  await expect.poll(async () => (await review.isVisible()) || (await classic.isVisible())).toBeTruthy();
  if (!(await review.isVisible())) {
    await expect(classic).toBeEnabled();
    await classic.click();
  }
  if (await page.getByRole("button", { name: /Continue to Review/ }).isVisible()) {
    await page.getByRole("tab", { name: "File export" }).click();
    await page.getByRole("button", { name: "Transform (optional)" }).click();
  } else {
    await page.getByRole("tab", { name: "File export" }).click();
    const next = page.getByRole("button", { name: /Continue to Transform/ });
    await expect(next).toBeEnabled();
    await next.click();
  }
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
  // A toast that arrives while the shot settles used to be masked, and the
  // mask paints a magenta rectangle the classic baseline does not have.
  // Hiding the toast keeps the step list itself on that baseline. The
  // transform id stays masked because the hash changes.
  await page.addStyleTag({ content: ".dt-toast { display: none !important; }" });
  await page.evaluate(async () => {
    await document.fonts.ready;
  });
  const mask = [page.locator(".df2-xform-identity"), ...extraMask];
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
