import { expect, test } from "@playwright/test";

import { installHermeticStubs } from "./hermetic";
import { isolateDrafts, openStudio } from "./support";

/**
 * UX-5: at 390 px the Destination step stacked its two columns inside a
 * fixed-height grid; the connector picker collapsed to nothing and the sync
 * summary painted over the connector cards, so no destination could be
 * chosen on a phone. Every card must be the element a finger lands on, and a
 * tap must select it.
 */
test.describe("phone destination step", () => {
  test.beforeEach(async ({ page }) => {
    await installHermeticStubs(page);
    await isolateDrafts(page);
    await page.addInitScript(() => {
      localStorage.setItem("df2.theme", "light");
    });
  });

  test("every saved destination card can be tapped at 390 px", async ({ page }) => {
    await openStudio(page);
    await page.getByRole("button", { name: /Load sample orders CSV/ }).click();
    await page.getByRole("button", { name: /Continue to Destination/ }).click();

    const cards = page.locator(".df2-dest-connector-card");
    await expect(cards.first()).toBeVisible();
    const count = await cards.count();
    expect(count).toBeGreaterThan(1);

    for (let i = 0; i < count; i += 1) {
      const card = cards.nth(i);
      await card.scrollIntoViewIfNeeded();
      // The card itself, not something painted over it, is under the finger.
      const onTop = await card.evaluate((node) => {
        const box = node.getBoundingClientRect();
        const hit = document.elementFromPoint(box.left + box.width / 2, box.top + box.height / 2);
        return Boolean(hit && node.contains(hit));
      });
      expect(onTop, `destination card ${i + 1} is covered`).toBe(true);
    }

    const last = cards.nth(count - 1);
    await last.scrollIntoViewIfNeeded();
    await last.tap();
    await expect(last).toHaveAttribute("aria-checked", "true");
    await expect(page.locator("#dest-col")).toBeVisible();

    const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
    expect(overflow, "no horizontal page scroll at 390 px").toBeLessThanOrEqual(0);
  });
});
