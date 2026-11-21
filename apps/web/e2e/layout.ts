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
  // Measure where the element rests, not mid-entrance (a drawer slides in).
  await page.locator(selector).first().evaluate((el) => Promise.all(
    el.getAnimations()
      .filter((a) => a.effect?.getTiming().iterations !== Infinity)
      .map((a) => a.finished.catch(() => undefined)),
  ));
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

export interface MapStepLayoutReport {
  /** Two visible interactive controls whose painted boxes intersect. */
  overlaps: string[];
  /** A control cut by an overflow:hidden/clip ancestor, or a scrollport that scrolls sideways. */
  clipped: string[];
  /** A control smaller than 24×24 CSS px (WCAG 2.2 SC 2.5.8) with no inline-text exemption. */
  small: string[];
  /** Text cut with an ellipsis or a line clamp and no title to read it in full. */
  truncated: string[];
  /** Neighbouring controls in one table row closer than 8 px. */
  cramped: string[];
  /** A form control that runs past its own table cell's padding box into the next column. */
  spill: string[];
  /**
   * A row whose first-line items (chevron, grip, name, arrow, destination,
   * transform, status action) do not share one vertical centre (±1 px).
   * In a card (the grid's narrow layout) only the items on the name's line
   * are compared; the cells stacked under it are not.
   */
  misaligned: string[];
  /** Where Continue to Validate sits, and whether it is inside the viewport. */
  primary: { left: number; right: number; top: number; bottom: number; inView: boolean } | null;
  /** The page scrolls sideways. */
  pageScrollsX: boolean;
  /** Height of each visible mapping row, in px. */
  rowHeights: number[];
}

/**
 * Map step layout facts (W7). Every visible interactive control in the Map
 * step, its footer, the Pilot edge tab and any toast is compared pairwise. A
 * scrollport may hold rows that are reached by scrolling up or down; it must
 * never hide content sideways.
 */
export async function mapStepLayoutReport(page: Page): Promise<MapStepLayoutReport> {
  await page.mouse.move(0, 0);
  return page.evaluate(() => {
    const step = document.querySelector(".df2-map-step-panel");
    const roots = [step, document.querySelector(".df2-copilot-edge-open"), ...document.querySelectorAll(".dt-toast")]
      .filter((n): n is Element => Boolean(n));
    const sel = 'button, a[href], input, select, textarea, summary, [role="button"], [role="tab"], [role="menuitem"], [role="option"], [role="checkbox"], [role="switch"], [role="combobox"]';
    const label = (el: Element) => {
      const text = (el.getAttribute("aria-label") || (el as HTMLElement).innerText || (el as HTMLInputElement).placeholder || "")
        .trim().replace(/\s+/g, " ").slice(0, 60);
      const cls = String((el as HTMLElement).className || "").trim().split(/\s+/).slice(0, 2).join(".");
      return `${el.tagName.toLowerCase()}${cls ? `.${cls}` : ""}${text ? ` "${text}"` : ""}`;
    };
    const visible = (el: Element) => {
      if (el.closest("[aria-hidden='true'], [inert]")) return false;
      const s = getComputedStyle(el);
      if (s.display === "none" || s.visibility === "hidden" || Number(s.opacity) === 0) return false;
      const r = el.getBoundingClientRect();
      if (r.width < 2 || r.height < 2) return false;
      return r.bottom > 1 && r.right > 1 && r.top < innerHeight - 1 && r.left < innerWidth - 1;
    };
    const painted = (el: Element) => {
      const r = el.getBoundingClientRect();
      let [l, t, rr, b] = [r.left, r.top, r.right, r.bottom];
      for (let n = el.parentElement; n; n = n.parentElement) {
        const s = getComputedStyle(n);
        if (s.position === "fixed") break;
        const c = n.getBoundingClientRect();
        if (/hidden|clip|auto|scroll/.test(s.overflowX)) { l = Math.max(l, c.left); rr = Math.min(rr, c.right); }
        if (/hidden|clip|auto|scroll/.test(s.overflowY)) { t = Math.max(t, c.top); b = Math.min(b, c.bottom); }
      }
      return { l: Math.max(l, 0), t: Math.max(t, 0), r: Math.min(rr, innerWidth), b: Math.min(b, innerHeight) };
    };
    const all = new Set<Element>();
    for (const root of roots) {
      if (root.matches(sel) || root.matches(".df2-copilot-edge-open, .dt-toast")) all.add(root);
      for (const el of root.querySelectorAll(sel)) all.add(el);
    }
    const items = [...all].filter(visible).map((el) => ({ el, p: painted(el) }))
      .filter((x) => x.p.r - x.p.l > 1 && x.p.b - x.p.t > 1);

    const overlaps: string[] = [];
    for (let i = 0; i < items.length; i += 1) {
      for (let j = i + 1; j < items.length; j += 1) {
        const a = items[i];
        const b = items[j];
        if (a.el.contains(b.el) || b.el.contains(a.el)) continue;
        const w = Math.min(a.p.r, b.p.r) - Math.max(a.p.l, b.p.l);
        const h = Math.min(a.p.b, b.p.b) - Math.max(a.p.t, b.p.t);
        if (w > 1 && h > 1) overlaps.push(`${label(a.el)} ∩ ${label(b.el)} (${Math.round(w)}×${Math.round(h)})`);
      }
    }

    const clipped: string[] = [];
    const small: string[] = [];
    for (const { el } of items) {
      const r = el.getBoundingClientRect();
      for (let n = el.parentElement; n; n = n.parentElement) {
        const s = getComputedStyle(n);
        if (s.position === "fixed") break;
        const c = n.getBoundingClientRect();
        const clipX = /hidden|clip/.test(s.overflowX);
        const clipY = /hidden|clip/.test(s.overflowY);
        // A 1 px visually-hidden wrapper hides on purpose; it is not a cut.
        if ((clipX || clipY) && c.width > 2 && c.height > 2) {
          const sides = [
            clipX && r.left < c.left - 1 ? "left" : "",
            clipX && r.right > c.right + 1 ? "right" : "",
            clipY && r.top < c.top - 1 ? "top" : "",
            clipY && r.bottom > c.bottom + 1 ? "bottom" : "",
          ].filter(Boolean);
          if (sides.length) {
            clipped.push(`${label(el)} cut ${sides.join("+")} by ${label(n)}`);
            break;
          }
        }
        if (/auto|scroll/.test(s.overflowX + s.overflowY)) break;
      }
      const inline = el.closest("p, li") && getComputedStyle(el).display === "inline";
      const isBox = el.matches("input[type=checkbox], input[type=radio]");
      if (!inline && !isBox && (r.width < 24 || r.height < 24)) {
        small.push(`${label(el)} ${Math.round(r.width)}×${Math.round(r.height)}`);
      }
    }
    // Sideways scrollports inside the step hide content a reader never sees.
    if (step) {
      for (const n of [step, ...step.querySelectorAll("*")]) {
        const s = getComputedStyle(n);
        if (!/auto|scroll/.test(s.overflowX) || !visible(n)) continue;
        if (n.scrollWidth > n.clientWidth + 1) clipped.push(`${label(n)} scrolls sideways ${n.scrollWidth}>${n.clientWidth}`);
      }
    }

    const truncated: string[] = [];
    if (step) {
      for (const el of step.querySelectorAll("*")) {
        if (!visible(el)) continue;
        const s = getComputedStyle(el);
        const clamps = s.textOverflow === "ellipsis" || (s.webkitLineClamp && s.webkitLineClamp !== "none");
        if (!clamps) continue;
        const over = el.scrollWidth > el.clientWidth + 1 || el.scrollHeight > el.clientHeight + 1;
        if (over && !el.closest("[title]")) truncated.push(`${label(el)} ${el.scrollWidth}>${el.clientWidth}`);
      }
    }

    const cramped: string[] = [];
    const spill: string[] = [];
    const misaligned: string[] = [];
    const rowHeights: number[] = [];
    for (const row of document.querySelectorAll(".df2-map-step-panel tr.df2-column-row")) {
      if (!visible(row)) continue;
      const box = row.getBoundingClientRect();
      if (box.top >= 0 && box.bottom <= innerHeight) rowHeights.push(Math.round(box.height));
      const controls = [...row.querySelectorAll("input, select, button")].filter(visible)
        .map((el) => ({ el, r: el.getBoundingClientRect() }));
      const centreOf = (el: Element | null): number | null => {
        if (!el) return null;
        // The arrow cell is aria-hidden on purpose; it is still painted.
        const shown = el.matches("td.df2-column-arrow")
          ? getComputedStyle(el).display !== "none" && getComputedStyle(el).visibility !== "hidden"
          : visible(el);
        if (!shown) return null;
        let r = el.getBoundingClientRect();
        if (el.matches("td, .df2-column-source")) {
          // Text: measure the first line of glyphs, not the box (a long name wraps).
          const range = document.createRange();
          range.selectNodeContents(el);
          const first = [...range.getClientRects()].find((b) => b.height >= 2);
          if (!first) return null;
          r = first;
        }
        return r.top + r.height / 2;
      };
      const status = row.querySelector(".df2-column-status");
      const firstLine: Array<[string, number | null]> = [
        ["chevron", centreOf(row.querySelector(".df2-column-expand"))],
        ["grip", centreOf(row.querySelector(".df2-ms-grip"))],
        ["name", centreOf(row.querySelector(".df2-column-source"))],
        ["arrow", centreOf(row.querySelector("td.df2-column-arrow"))],
        ["destination", centreOf(row.querySelector(".df2-column-destination-cell input, .df2-column-destination-cell select"))],
        ["transform", centreOf(row.querySelector(".df2-column-transform-cell select"))],
        ["status", centreOf(status?.querySelector("button, .df2-column-status-chip, .df2-badge") ?? null)],
      ];
      const placed = firstLine.filter((x): x is [string, number] => x[1] != null);
      if (placed.length > 1) {
        // The card's first line on a phone is the line the name sits on.
        const anchor = placed.find(([k]) => k === "name")?.[1] ?? placed[0][1];
        const onLine = placed.filter(([, y]) => Math.abs(y - anchor) < 14);
        const ys = onLine.map(([, y]) => y);
        if (Math.max(...ys) - Math.min(...ys) > 1) {
          misaligned.push(`${label(row).slice(0, 40)}: ${onLine.map(([k, y]) => `${k}@${y.toFixed(1)}`).join(" ")}`);
        }
        // A row drawn as a table row has one first line; a card stacks its cells.
        const tableRow = getComputedStyle(row).display === "table-row";
        if (tableRow && onLine.length < placed.length) {
          misaligned.push(`${label(row).slice(0, 40)}: off the first line ${placed.filter((p) => !onLine.includes(p)).map(([k, y]) => `${k}@${y.toFixed(1)}`).join(" ")} (name @${anchor.toFixed(1)})`);
        }
      }
      for (const { el, r } of controls) {
        const td = el.closest("td");
        // A cell drawn with display: contents has no box of its own to spill out of.
        if (!td || getComputedStyle(td).display === "contents") continue;
        const cell = td.getBoundingClientRect();
        const cs = getComputedStyle(td);
        const inner = {
          left: cell.left + parseFloat(cs.paddingLeft) - 1,
          right: cell.right - parseFloat(cs.paddingRight) + 1,
        };
        if (r.right > inner.right || r.left < inner.left) {
          spill.push(`${label(el)} spans ${Math.round(r.left)}–${Math.round(r.right)}, its cell holds ${Math.round(inner.left + 1)}–${Math.round(inner.right - 1)}`);
        }
      }
      for (let i = 0; i < controls.length; i += 1) {
        for (let j = i + 1; j < controls.length; j += 1) {
          const a = controls[i].r;
          const b = controls[j].r;
          const sameLine = Math.min(a.bottom, b.bottom) - Math.max(a.top, b.top) > 4;
          if (!sameLine) continue;
          const gap = a.right <= b.left ? b.left - a.right : b.right <= a.left ? a.left - b.right : -1;
          if (gap < 8) cramped.push(`${label(controls[i].el)} · ${label(controls[j].el)} gap ${Math.round(gap)}px`);
        }
      }
    }

    const primary = document.querySelector(".df2-map-footer .df2-btn-primary");
    const pr = primary?.getBoundingClientRect();
    return {
      overlaps,
      clipped,
      small,
      truncated,
      cramped,
      spill,
      misaligned,
      primary: pr
        ? {
            left: Math.round(pr.left), right: Math.round(pr.right), top: Math.round(pr.top), bottom: Math.round(pr.bottom),
            inView: pr.left >= 0 && pr.right <= innerWidth && pr.top >= 0 && pr.bottom <= innerHeight,
          }
        : null,
      pageScrollsX: document.documentElement.scrollWidth > innerWidth + 1,
      rowHeights,
    };
  });
}
