import { expect, type Locator, type Page } from "@playwright/test";

export type Box = { x: number; y: number; width: number; height: number };

/** Intersection area. A 1px edge touch is contact, not a collision. */
export function overlapArea(a: Box, b: Box): number {
  const width = Math.min(a.x + a.width, b.x + b.width) - Math.max(a.x, b.x);
  const height = Math.min(a.y + a.height, b.y + b.height) - Math.max(a.y, b.y);
  if (width <= 1 || height <= 1) return 0;
  return Math.round(width * height);
}

function asLocator(page: Page, target: string | Locator): Locator {
  return typeof target === "string" ? page.locator(target).first() : target;
}

function labelOf(target: string | Locator): string {
  return typeof target === "string" ? target : target.toString();
}

/** Box after ancestor overflow clips. A scrolled child is not a collision with the footer. */
export async function clippedBox(page: Page, target: string | Locator): Promise<Box | null> {
  return asLocator(page, target).evaluate((el) => {
    const rect = el.getBoundingClientRect();
    let top = rect.top;
    let left = rect.left;
    let right = rect.right;
    let bottom = rect.bottom;
    let node = el.parentElement;
    while (node) {
      const style = getComputedStyle(node);
      const overflow = `${style.overflow} ${style.overflowX} ${style.overflowY}`;
      if (/(auto|hidden|scroll|clip)/.test(overflow)) {
        const clip = node.getBoundingClientRect();
        top = Math.max(top, clip.top);
        left = Math.max(left, clip.left);
        right = Math.min(right, clip.right);
        bottom = Math.min(bottom, clip.bottom);
      }
      node = node.parentElement;
    }
    const width = right - left;
    const height = bottom - top;
    if (width <= 1 || height <= 1) return null;
    return { x: left, y: top, width, height };
  });
}

export async function expectDisjoint(page: Page, left: string | Locator, right: string | Locator): Promise<void> {
  const boxA = await clippedBox(page, left);
  const boxB = await clippedBox(page, right);
  const nameA = labelOf(left);
  const nameB = labelOf(right);
  if (!boxA || !boxB) return;
  const area = overlapArea(boxA, boxB);
  expect(area, `${nameA} overlaps ${nameB} by ${area}px`).toBe(0);
}

export async function expectWithinViewport(page: Page, selector: string, slop = 1): Promise<void> {
  const box = await page.locator(selector).first().boundingBox();
  const viewport = page.viewportSize();
  expect(box && viewport, selector).toBeTruthy();
  expect(box!.y, selector).toBeGreaterThanOrEqual(-slop);
  expect(box!.x, selector).toBeGreaterThanOrEqual(-slop);
  expect(box!.x + box!.width, selector).toBeLessThanOrEqual(viewport!.width + slop);
  expect(box!.y + box!.height, selector).toBeLessThanOrEqual(viewport!.height + slop);
}

/** When two boxes intersect, the topmost element at the intersection must be `over`. */
export async function expectStackedAbove(page: Page, under: string, over: string): Promise<void> {
  const boxA = await page.locator(under).first().boundingBox();
  const boxB = await page.locator(over).first().boundingBox();
  expect(boxA && boxB, `${under} / ${over}`).toBeTruthy();
  if (overlapArea(boxA!, boxB!) === 0) return;
  const left = Math.max(boxA!.x, boxB!.x);
  const right = Math.min(boxA!.x + boxA!.width, boxB!.x + boxB!.width);
  const top = Math.max(boxA!.y, boxB!.y);
  const bottom = Math.min(boxA!.y + boxA!.height, boxB!.y + boxB!.height);
  const x = (left + right) / 2;
  const y = (top + bottom) / 2;
  const hit = await page.evaluate(({ x, y }) => {
    const node = document.elementFromPoint(x, y);
    if (!node) return "nothing";
    const token = node.className && typeof node.className === "string" ? node.className : node.tagName;
    return `${node.tagName}.${token}`.slice(0, 180);
  }, { x, y });
  const stacked = await page.evaluate(({ x, y, over }) => {
    const node = document.elementFromPoint(x, y);
    return Boolean(node && node.closest(over));
  }, { x, y, over });
  expect(stacked, `${over} does not cover ${under} at ${x},${y} (hit ${hit})`).toBeTruthy();
}

export async function expectHitInside(page: Page, target: Locator, ancestor: string): Promise<void> {
  const box = await target.boundingBox();
  expect(box, ancestor).toBeTruthy();
  const x = box!.x + Math.min(box!.width / 2, box!.width - 4);
  const y = box!.y + Math.min(box!.height / 2, 16);
  const inside = await page.evaluate(({ x, y, ancestor }) => {
    const hit = document.elementFromPoint(x, y);
    return Boolean(hit && hit.closest(ancestor));
  }, { x, y, ancestor });
  expect(inside, `hit at ${x},${y} is outside ${ancestor}`).toBeTruthy();
}
