/**
 * UX-13 / UX-14 (WCAG 2.4.1, 2.4.7, 2.4.11): a skip link and a <main>
 * landmark lead the signed-in shell, and no stylesheet erases a keyboard focus
 * ring without drawing a solid one in its place.
 */
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const stylesDir = dirname(fileURLToPath(import.meta.url));
const srcDir = join(stylesDir, "..");

type Rule = { file: string; selector: string; body: string };

function rules(): Rule[] {
  const out: Rule[] = [];
  for (const file of readdirSync(stylesDir).filter((f) => f.endsWith(".css"))) {
    const css = readFileSync(join(stylesDir, file), "utf8").replace(/\/\*[\s\S]*?\*\//g, "");
    for (const m of css.matchAll(/([^{}]+)\{([^{}]*)\}/g)) {
      out.push({ file, selector: m[1].trim().replace(/\s+/g, " "), body: m[2] });
    }
  }
  return out;
}

const ALL = rules();

/** Inputs whose ring is drawn by their wrapper's :focus-within instead. */
const WRAPPED_INPUTS: Record<string, string> = {
  ".df2-toolbar-search input": ".df2-toolbar-search:focus-within",
  ".df2-command-search input": ".df2-command-search:focus-within",
  ".df2-workspace-search input": ".df2-workspace-search:focus-within",
};

function solidOutline(body: string): boolean {
  return /outline\s*:\s*2px solid var\(--df-focus-ring\)/.test(body);
}

describe("focus rings", () => {
  it("no :focus-visible rule removes the outline without a solid replacement", () => {
    const offenders: string[] = [];
    for (const rule of ALL) {
      for (const part of rule.selector.split(",").map((p) => p.trim())) {
        if (!part.includes(":focus-visible") || part.includes(":not(:focus-visible)")) continue;
        if (!/outline\s*:\s*(none|0)\b/.test(rule.body)) continue;
        const wrapped = Object.keys(WRAPPED_INPUTS).find((input) => part.includes(input));
        if (wrapped) continue;
        // Marketing pages draw a solid --df-focus-ring box-shadow (not an alpha tint).
        if (/box-shadow\s*:\s*0 0 0 3px var\(--df-focus-ring\b/.test(rule.body)) continue;
        offenders.push(`${rule.file}: ${part}`);
      }
    }
    assert.deepEqual(offenders, [], "these focus rules erase the ring");
  });

  it("every wrapper that owns its input's ring draws a solid outline", () => {
    for (const wrapper of Object.values(WRAPPED_INPUTS)) {
      const owners = ALL.filter((r) => r.selector.split(",").some((p) => p.trim().endsWith(wrapper)));
      assert.ok(owners.some((r) => solidOutline(r.body)), `${wrapper} must draw a 2px solid --df-focus-ring outline`);
    }
  });

  it("filter tabs keep a solid ring even after the segmented-control reset clears box-shadow", () => {
    const tabRing = ALL.filter((r) => r.selector.split(",").some((p) => p.trim() === ".df2-app .df2-tab:focus-visible"));
    assert.ok(tabRing.length > 0);
    for (const r of tabRing) assert.ok(solidOutline(r.body), `${r.file}: ${r.selector}`);
  });

  it("the accessibility layer gives every focusable control a ring", () => {
    const layer = ALL.find((r) => r.file === "app-styles.css" && r.selector.startsWith(".df2-app :is(a[href], button"));
    assert.ok(layer, "app-styles.css ends with the shared focus-visible layer");
    assert.ok(solidOutline(layer!.body));
  });

  it("focus ring tokens are opaque colours, not tints", () => {
    const tokens = readFileSync(join(stylesDir, "tokens.css"), "utf8");
    for (const m of tokens.matchAll(/--df-focus-ring:\s*([^;]+);/g)) {
      assert.doesNotMatch(m[1], /rgba\(/, "a focus ring must not be translucent");
    }
  });
});

describe("skip link and main landmark", () => {
  const app = readFileSync(join(srcDir, "DataTransferApp.tsx"), "utf8");

  it("the skip link is the first element of the signed-in shell", () => {
    const shell = app.indexOf('className={`df2-app ');
    const skip = app.indexOf('className="df2-skip-link"', shell);
    const sidebar = app.indexOf("<aside", shell);
    assert.ok(shell > 0 && skip > shell && skip < sidebar, "skip link renders before the sidebar");
    assert.match(app, />\s*Skip to main content\s*</);
  });

  it("the content area is the one <main> landmark and can take focus", () => {
    assert.match(app, /<main id="datawrap-main-content" className={`df2-content \$\{contentModeClass\}`} tabIndex=\{-1\}/);
    assert.equal((app.match(/<main\b/g) ?? []).length, 1);
    const transfer = readFileSync(join(srcDir, "pages", "TransferPage.tsx"), "utf8");
    assert.equal((transfer.match(/<main\b/g) ?? []).length, 0, "Transfer Studio must not nest a second <main>");
  });

  it("the skip link is hidden until focused, then visible", () => {
    const layer = ALL.filter((r) => r.file === "app-styles.css");
    const base = layer.find((r) => r.selector === ".df2-skip-link");
    const focused = layer.find((r) => r.selector.includes(".df2-skip-link:focus"));
    assert.match(base?.body ?? "", /transform:\s*translateY\(-200%\)/);
    assert.match(focused?.body ?? "", /transform:\s*none/);
  });
});
