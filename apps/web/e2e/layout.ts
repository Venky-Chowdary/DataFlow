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

/**
 * Flagged transfer shell. Visible text and controls must not intersect an
 * unrelated control, overflow:hidden must not slice them, every positioned
 * z-index must be one of the --df-z-* tokens, and the shell must not scroll
 * sideways. A scrollport (overflow auto) may hold content that is reached by
 * scrolling; a fixed bar is not clipped by the step behind it.
 */
export async function expectStudioShellClear(page: Page, where: string): Promise<void> {
  // A pointer left over a palette tile keeps its tooltip open, and the tooltip overlaps the
  // tile it describes. Park the pointer so the check sees the idle shell.
  await page.mouse.move(0, 0);
  await page.locator(".dt-toast").evaluateAll((nodes) => {
    for (const node of nodes) (node as HTMLElement).style.setProperty("display", "none");
  }).catch(() => {});
  const report = await page.evaluate(() => {
    const shell = document.querySelector(".df2-transfer-studio-shell");
    const pageRoot = document.querySelector(".df2-page-transfer-studio");
    const interactiveSel = 'button, a[href], input, select, textarea, summary, [role="button"], [role="tab"], [role="menuitem"], [role="option"], [role="checkbox"], [role="switch"], [role="link"], [role="radio"], [role="combobox"]';
    const rootStyle = getComputedStyle(document.documentElement);
    const tokenNames = ["--df-z-floor", "--df-z-base", "--df-z-raised", "--df-z-canvas-stage", "--df-z-canvas-rail", "--df-z-canvas-overview", "--df-z-canvas-issues", "--df-z-canvas-toolbar", "--df-z-canvas-menu", "--df-z-sticky", "--df-z-action", "--df-z-topbar", "--df-z-sidebar", "--df-z-dropdown", "--df-z-popover", "--df-z-modal", "--df-z-toast", "--df-z-boot"];
    const tokenValues = new Set(tokenNames.map((name) => String(parseInt(rootStyle.getPropertyValue(name), 10))));

    function label(el: Element): string {
      const text = (el.getAttribute("aria-label") || el.getAttribute("placeholder") || (el as HTMLElement).innerText || "")
        .trim().replace(/\s+/g, " ").slice(0, 70);
      const cls = typeof (el as HTMLElement).className === "string"
        ? (el as HTMLElement).className.trim().split(/\s+/).slice(0, 2).join(".")
        : "";
      return `${el.tagName.toLowerCase()}${cls ? `.${cls}` : ""}${text ? ` "${text}"` : ""}`;
    }

    function visible(el: Element): boolean {
      if (el.closest("[aria-hidden='true']")) return false;
      const style = getComputedStyle(el);
      if (style.display === "none" || style.visibility === "hidden" || Number(style.opacity) === 0) return false;
      const r = el.getBoundingClientRect();
      if (r.width < 2 || r.height < 2) return false;
      if (r.bottom < 1 || r.right < 1 || r.top > innerHeight - 1 || r.left > innerWidth - 1) return false;
      return true;
    }

    const nodes: Element[] = [];
    const seen = new Set<Element>();
    const scope = pageRoot || document.body;
    function add(el: Element | null): void {
      if (!el || seen.has(el) || !visible(el)) return;
      seen.add(el);
      nodes.push(el);
    }
    for (const el of scope.querySelectorAll(interactiveSel)) add(el);
    add(document.querySelector(".df2-copilot-edge-open"));
    const walker = document.createTreeWalker(scope, NodeFilter.SHOW_TEXT);
    let textNode = walker.nextNode();
    while (textNode) {
      if (textNode.textContent && textNode.textContent.trim()) add(textNode.parentElement);
      textNode = walker.nextNode();
    }

    function painted(el: Element): { x: number; y: number; r: number; b: number } {
      const rect = el.getBoundingClientRect();
      let left = rect.left;
      let top = rect.top;
      let right = rect.right;
      let bottom = rect.bottom;
      let node = el.parentElement;
      while (node) {
        const style = getComputedStyle(node);
        if (style.position === "fixed") break;
        const clipsX = /hidden|clip|auto|scroll/.test(style.overflowX);
        const clipsY = /hidden|clip|auto|scroll/.test(style.overflowY);
        if (clipsX || clipsY) {
          const c = node.getBoundingClientRect();
          if (clipsX) {
            left = Math.max(left, c.left);
            right = Math.min(right, c.right);
          }
          if (clipsY) {
            top = Math.max(top, c.top);
            bottom = Math.min(bottom, c.bottom);
          }
        }
        node = node.parentElement;
      }
      return {
        x: Math.max(left, 0),
        y: Math.max(top, 0),
        r: Math.min(right, innerWidth),
        b: Math.min(bottom, innerHeight),
      };
    }

    function area(a: { x: number; y: number; r: number; b: number }, b: { x: number; y: number; r: number; b: number }): number {
      const w = Math.min(a.r, b.r) - Math.max(a.x, b.x);
      const h = Math.min(a.b, b.b) - Math.max(a.y, b.y);
      if (w <= 1 || h <= 1) return 0;
      return Math.round(w * h);
    }

    function related(a: Element, b: Element): boolean {
      if (a.contains(b) || b.contains(a)) return true;
      const host = a.closest("button, a, label, li, th, td, [role='button'], [role='tab'], [role='menuitem'], [role='option'], [role='row']");
      return Boolean(host && host.contains(b));
    }

    const overlaps: string[] = [];
    const boxes = nodes.map(painted);
    for (let i = 0; i < nodes.length; i += 1) {
      if (boxes[i].r - boxes[i].x <= 1 || boxes[i].b - boxes[i].y <= 1) continue;
      for (let j = i + 1; j < nodes.length; j += 1) {
        if (related(nodes[i], nodes[j])) continue;
        const hit = area(boxes[i], boxes[j]);
        if (hit > 0) overlaps.push(`${label(nodes[i])} ∩ ${label(nodes[j])} (${hit}px)`);
      }
    }

    const clips: string[] = [];
    for (const el of nodes) {
      const rect = el.getBoundingClientRect();
      let node = el.parentElement;
      let sawScrollX = false;
      let sawScrollY = false;
      while (node) {
        const style = getComputedStyle(node);
        if (style.position === "fixed") break;
        const scrollX = style.overflowX === "auto" || style.overflowX === "scroll";
        const scrollY = style.overflowY === "auto" || style.overflowY === "scroll";
        const clipX = (style.overflowX === "hidden" || style.overflowX === "clip") && !sawScrollX;
        const clipY = (style.overflowY === "hidden" || style.overflowY === "clip") && !sawScrollY;
        if (clipX || clipY) {
          const c = node.getBoundingClientRect();
          const sides: string[] = [];
          if (clipX && rect.left < c.left - 1) sides.push("L");
          if (clipX && rect.right > c.right + 1) sides.push("R");
          if (clipY && rect.top < c.top - 1) sides.push("T");
          if (clipY && rect.bottom > c.bottom + 1) sides.push("B");
          if (sides.length) {
            clips.push(`${label(el)} clipped ${sides.join("")} by ${label(node)}`);
            break;
          }
        }
        if (scrollX) sawScrollX = true;
        if (scrollY) sawScrollY = true;
        node = node.parentElement;
      }
      const self = getComputedStyle(el);
      if ((self.overflowX === "hidden" || self.overflowX === "clip") && el.scrollWidth > el.clientWidth + 1) {
        clips.push(`${label(el)} self-x ${el.scrollWidth}>${el.clientWidth}`);
      }
      if ((self.overflowY === "hidden" || self.overflowY === "clip") && el.scrollHeight > el.clientHeight + 1) {
        clips.push(`${label(el)} self-y ${el.scrollHeight}>${el.clientHeight}`);
      }
    }

    const off: string[] = [];
    const stacked: Element[] = [];
    for (const el of scope.querySelectorAll("*")) {
      const style = getComputedStyle(el);
      if (style.position === "static" || style.zIndex === "auto" || style.display === "none") continue;
      if (!tokenValues.has(style.zIndex)) off.push(`${label(el)} z=${style.zIndex}`);
      if (visible(el)) stacked.push(el);
    }
    const zHits: string[] = [];
    for (let i = 0; i < stacked.length; i += 1) {
      for (let j = i + 1; j < stacked.length; j += 1) {
        if (getComputedStyle(stacked[i]).zIndex !== getComputedStyle(stacked[j]).zIndex) continue;
        if (related(stacked[i], stacked[j])) continue;
        const hit = area(painted(stacked[i]), painted(stacked[j]));
        if (hit > 0) zHits.push(`${label(stacked[i])} z=${getComputedStyle(stacked[i]).zIndex} ∩ ${label(stacked[j])}`);
      }
    }

    return {
      overlaps,
      clips,
      off,
      zHits,
      shellH: Boolean(shell && shell.scrollWidth > shell.clientWidth + 1),
      docH: document.documentElement.scrollWidth > window.innerWidth + 1,
    };
  });
  expect(report.overlaps, `${where} overlap\n${report.overlaps.slice(0, 12).join("\n")}`).toEqual([]);
  expect(report.clips, `${where} clipped\n${report.clips.slice(0, 12).join("\n")}`).toEqual([]);
  expect(report.off, `${where} z-index off the token scale\n${report.off.slice(0, 12).join("\n")}`).toEqual([]);
  expect(report.zHits, `${where} z-index collision\n${report.zHits.slice(0, 12).join("\n")}`).toEqual([]);
  expect(report.shellH, `${where} shell horizontal scrollbar`).toBe(false);
  expect(report.docH, `${where} page horizontal scrollbar`).toBe(false);
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
