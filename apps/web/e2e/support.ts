import { expect, type Page } from "@playwright/test";

/**
 * Resolve a CSS custom property to its computed colour, the way the browser
 * paints it — so assertions track the token, not a hex literal (UI-11:
 * a brand repaint must never leave a stale `rgb(...)` in the suite).
 * var() chains (--df-brand-tile → --df-logo-tile) resolve on a probe element.
 */
export async function tokenColor(page: Page, token: string): Promise<string> {
  return page.evaluate((name) => {
    const probe = document.createElement("div");
    probe.style.backgroundColor = `var(${name})`;
    probe.style.display = "none";
    document.body.appendChild(probe);
    const color = getComputedStyle(probe).backgroundColor;
    probe.remove();
    return color;
  }, token);
}

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

/**
 * Toasts dock in-flow in the studio header chrome (D-BP-20), so one that is
 * still on screen, or that expires mid-capture, changes the header band and with
 * it the height of everything under it. A visual capture must never depend on
 * how long the sample-load toast lives. Hide toasts for good (a style tag, so a
 * late arrival is hidden too) and wait until every toast has left the DOM; the
 * dock then holds its idle layout, which is what the baselines show.
 */
export async function settleToasts(page: Page): Promise<void> {
  await page.evaluate(() => {
    if (document.getElementById("df-e2e-quiet-toasts")) return;
    const style = document.createElement("style");
    style.id = "df-e2e-quiet-toasts";
    style.textContent = ".dt-toast, .dt-toast-more { display: none !important; }";
    document.head.appendChild(style);
  });
  await expect(page.locator(".dt-toast, .dt-toast-more")).toHaveCount(0, { timeout: 30_000 });
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
  await settleToasts(page);
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
