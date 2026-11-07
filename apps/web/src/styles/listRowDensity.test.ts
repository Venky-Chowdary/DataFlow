/**
 * Workspace list-row density is a contract, not a per-page opinion.
 *
 * Connectors / Contracts / Schedules / Jobs must resolve to the same row
 * geometry at the same viewport. That broke once before in a way no page-level
 * review could catch: the `--df-list-row-*` tokens were spread over four width
 * queries that all match a 1280px viewport, plus a `max-height` query, and each
 * one shrank the row again. The values looked reasonable in isolation and
 * compounded to a 38px row with 4px of padding on the most common enterprise
 * screen.
 *
 * These tests resolve the cascade the way a browser does and hold the floor.
 */
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, it } from 'node:test';

const STYLES_DIR = dirname(fileURLToPath(import.meta.url));
const TOKENS = readFileSync(join(STYLES_DIR, 'tokens.css'), 'utf8');

const ROW_TOKEN_RE = /--df-list-row-([a-z-]+)\s*:\s*([^;]+);/g;

/** Enterprise floor. Below any of these a row stops being comfortably usable. */
const FLOOR = {
  'min-h': 44, // WCAG 2.5.5 target size; Fluent/Carbon compact rows sit here.
  'pad-y': 6,
  'pad-x': 10,
  title: 12.5,
  meta: 11.5,
  gap: 8,
  icon: 15,
  action: 24,
} as const;

type Viewport = { width: number; height: number; label: string };

/** Screens an enterprise operator actually uses, including the split-screen case. */
const VIEWPORTS: Viewport[] = [
  { width: 1024, height: 768, label: 'tablet landscape' },
  { width: 1280, height: 800, label: '1280x800 laptop' },
  { width: 1366, height: 768, label: '1366x768 laptop' },
  { width: 1440, height: 900, label: 'MacBook Air' },
  { width: 1512, height: 982, label: 'MacBook Pro 14"' },
  { width: 1680, height: 1050, label: '16" laptop' },
  { width: 1920, height: 1080, label: 'desktop 1080p' },
  { width: 2560, height: 1440, label: 'desktop 1440p' },
];

type Block = { condition: string; body: string };

/** Top-level `@media` blocks, with their single-level bodies. */
function mediaBlocks(css: string): Block[] {
  const blocks: Block[] = [];
  const re = /@media([^{]+)\{/g;
  let match: RegExpExecArray | null;
  while ((match = re.exec(css))) {
    let depth = 1;
    let i = re.lastIndex;
    while (i < css.length && depth > 0) {
      if (css[i] === '{') depth += 1;
      else if (css[i] === '}') depth -= 1;
      i += 1;
    }
    blocks.push({ condition: match[1].trim(), body: css.slice(re.lastIndex, i - 1) });
    re.lastIndex = i;
  }
  return blocks;
}

function matchesViewport(condition: string, vp: Viewport): boolean {
  if (/prefers-|print|hover|orientation/.test(condition)) return false;
  const features = [...condition.matchAll(/\((min|max)-(width|height):\s*(\d+(?:\.\d+)?)px\)/g)];
  if (!features.length) return false;
  return features.every(([, bound, axis, raw]) => {
    const value = Number(raw);
    const actual = axis === 'width' ? vp.width : vp.height;
    return bound === 'min' ? actual >= value : actual <= value;
  });
}

function declaredRowTokens(body: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [, name, value] of body.matchAll(ROW_TOKEN_RE)) out[name] = value.trim();
  return out;
}

/** Base `.df2-app` declarations, i.e. everything outside an `@media` block. */
function baseRowTokens(css: string): Record<string, string> {
  let stripped = css;
  for (const block of mediaBlocks(css)) stripped = stripped.replace(block.body, '');
  return declaredRowTokens(stripped);
}

function resolveAt(vp: Viewport): Record<string, string> {
  const resolved = baseRowTokens(TOKENS);
  for (const block of mediaBlocks(TOKENS)) {
    if (!matchesViewport(block.condition, vp)) continue;
    Object.assign(resolved, declaredRowTokens(block.body));
  }
  return resolved;
}

describe('list-row density tokens', () => {
  it('are declared by tokens.css alone', () => {
    const offenders = readdirSync(STYLES_DIR)
      .filter((f) => f.endsWith('.css') && f !== 'tokens.css')
      .filter((f) => /--df-list-row-[a-z-]+\s*:/.test(readFileSync(join(STYLES_DIR, f), 'utf8')));
    assert.deepEqual(
      offenders,
      [],
      'list-row geometry must have one owner — move these into tokens.css',
    );
  });

  it('are consumed without a fallback, so a missing token fails loudly', () => {
    const offenders = readdirSync(STYLES_DIR)
      .filter((f) => f.endsWith('.css'))
      .flatMap((f) =>
        [...readFileSync(join(STYLES_DIR, f), 'utf8').matchAll(/var\(--df-list-row-[a-z-]+,[^)]*\)/g)]
          .map((m) => `${f}: ${m[0]}`),
      );
    assert.deepEqual(
      offenders,
      [],
      'a var() fallback silently reinstates a hard-coded density when the token is gone',
    );
  });

  it('never resolve below the enterprise floor', () => {
    for (const vp of VIEWPORTS) {
      const resolved = resolveAt(vp);
      for (const [token, min] of Object.entries(FLOOR)) {
        const raw = resolved[token];
        assert.ok(raw, `--df-list-row-${token} is undefined at ${vp.label}`);
        assert.ok(
          Number.parseFloat(raw) >= min,
          `--df-list-row-${token} is ${raw} at ${vp.label}, below the ${min}px floor`,
        );
      }
    }
  });

  it('are set by exactly one media block per viewport, so they cannot compound', () => {
    for (const vp of VIEWPORTS) {
      const setters = mediaBlocks(TOKENS)
        .filter((b) => matchesViewport(b.condition, vp))
        .filter((b) => Object.keys(declaredRowTokens(b.body)).length > 0)
        .map((b) => b.condition);
      assert.equal(
        setters.length,
        1,
        `${vp.label} matches ${setters.length} list-row blocks (${setters.join(' | ')}); ` +
          'overlapping ranges shrink the row once per block',
      );
    }
  });

  it('do not shrink a row because the viewport is short', () => {
    const tall = resolveAt({ width: 1440, height: 1200, label: 'tall' });
    const short = resolveAt({ width: 1440, height: 700, label: 'short' });
    assert.deepEqual(
      short,
      tall,
      'a short viewport is a reason to scroll, not to shrink targets',
    );
  });

  it('give a two-line row a declared height step, not an accidental one', () => {
    for (const vp of VIEWPORTS) {
      const resolved = resolveAt(vp);
      const single = Number.parseFloat(resolved['min-h']);
      const dual = Number.parseFloat(resolved['min-h-dual']);
      assert.ok(resolved['min-h-dual'], `--df-list-row-min-h-dual is undefined at ${vp.label}`);
      assert.ok(
        dual >= single + 10,
        `two-line rows resolve to ${dual}px against a ${single}px single-line row at ` +
          `${vp.label}; a second content line needs its own declared step`,
      );
    }
  });

  it('own the Jobs row geometry — no hard-coded padding or height beside the tokens', () => {
    const offenders: string[] = [];
    for (const file of readdirSync(STYLES_DIR).filter((f) => f.endsWith('.css'))) {
      const css = readFileSync(join(STYLES_DIR, file), 'utf8');
      for (const match of css.matchAll(/([^{}]*\.df2-job-row(?![-\w])[^{}]*)\{([^}]*)\}/g)) {
        for (const decl of match[2].split(';')) {
          if (!/^\s*(padding|min-height|column-gap|gap)(-[a-z]+)?\s*:/.test(decl)) continue;
          if (decl.includes('var(--df-list-row-')) continue;
          offenders.push(`${file}: ${match[1].trim()} {${decl.trim()}}`);
        }
      }
    }
    assert.deepEqual(
      offenders,
      [],
      'Jobs rows must take their geometry from the shared tokens, or they drift ' +
        'away from Connectors/Contracts/Schedules again',
    );
  });

  it('own list-row typography — no page rule sets a literal font-size on a row name or meta', () => {
    const offenders: string[] = [];
    for (const file of readdirSync(STYLES_DIR).filter((f) => f.endsWith('.css'))) {
      const css = readFileSync(join(STYLES_DIR, file), 'utf8');
      for (const match of css.matchAll(
        /([^{}]*\.df2-(?:job|connector|contract|pipeline)-row-(?:name|meta)(?![-\w])[^{}]*)\{([^}]*)\}/g,
      )) {
        for (const decl of match[2].split(';')) {
          if (!/^\s*font-size\s*:/.test(decl)) continue;
          if (decl.includes('var(--df-list-row-')) continue;
          offenders.push(`${file}: ${match[1].trim()} {${decl.trim()}}`);
        }
      }
    }
    assert.deepEqual(
      offenders,
      [],
      'a page-scoped font-size outranks the density token, so one list reads a ' +
        'step smaller than the rest at the same viewport',
    );
  });

  it('own drawer list rows — a slide-over row is the same row the page lists', () => {
    const offenders: string[] = [];
    for (const file of readdirSync(STYLES_DIR).filter((f) => f.endsWith('.css'))) {
      const css = readFileSync(join(STYLES_DIR, file), 'utf8');
      for (const match of css.matchAll(/([^{}]*\.df2-drawer-related-row(?![-\w])[^{}]*)\{([^}]*)\}/g)) {
        for (const decl of match[2].split(';')) {
          if (!/^\s*(padding|min-height|column-gap|gap)(-[a-z]+)?\s*:/.test(decl)) continue;
          if (decl.includes('var(--df-list-row-')) continue;
          offenders.push(`${file}: ${match[1].trim()} {${decl.trim()}}`);
        }
      }
    }
    assert.deepEqual(
      offenders,
      [],
      'a drawer that hard-codes its row geometry reads taller or shorter than ' +
        'the same records on their own page',
    );
  });

  it('align form-row fields to the top — one label line, one input line', () => {
    const offenders: string[] = [];
    for (const file of readdirSync(STYLES_DIR).filter((f) => f.endsWith('.css'))) {
      const css = readFileSync(join(STYLES_DIR, file), 'utf8');
      for (const match of css.matchAll(/([^{}]*\.df2-form-row(?![-\w])[^{}]*)\{([^}]*)\}/g)) {
        for (const decl of match[2].split(';')) {
          if (!/^\s*align-items\s*:/.test(decl)) continue;
          if (/flex-start/.test(decl)) continue;
          offenders.push(`${file}: ${match[1].trim()} {${decl.trim()}}`);
        }
      }
    }
    assert.deepEqual(
      offenders,
      [],
      'bottom-aligned form rows lift a field that carries an extra control ' +
        '(read-mode segment, hint) above every other label in the row',
    );
  });

  it('give the connector catalog one tile height', () => {
    const grids: string[] = [];
    for (const file of readdirSync(STYLES_DIR).filter((f) => f.endsWith('.css'))) {
      const css = readFileSync(join(STYLES_DIR, file), 'utf8');
      for (const match of css.matchAll(/(^|\})\s*(\.df2-connector-grid)\s*\{([^}]*)\}/g)) {
        if (/grid-auto-rows\s*:\s*1fr/.test(match[3])) grids.push(file);
      }
    }
    assert.ok(
      grids.length > 0,
      'without equal auto rows the catalog renders neighbouring tiles at ' +
        'different heights — a name that wraps or a missing description is enough',
    );
  });

  it('never get denser as the screen gets wider', () => {
    const heights = VIEWPORTS.map((vp) => Number.parseFloat(resolveAt(vp)['min-h']));
    for (let i = 1; i < heights.length; i += 1) {
      assert.ok(
        heights[i] >= heights[i - 1],
        `row height drops from ${heights[i - 1]}px at ${VIEWPORTS[i - 1].label} ` +
          `to ${heights[i]}px at ${VIEWPORTS[i].label}`,
      );
    }
  });
});
