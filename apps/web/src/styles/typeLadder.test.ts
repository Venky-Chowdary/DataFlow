/**
 * Workspace typography is a contract, the same way row density is.
 *
 * The audit that produced these tests measured every rendered font-family,
 * size and weight on the 13 authenticated routes at 1920/1440/1280/1024. What
 * it found was not a design disagreement but drift no page review can catch:
 * native controls fell back to Arial because `font-family` does not inherit
 * into `button`/`input`, pages restated a control's size next to the shared
 * height ladder, weights were requested at 620/650/720/750/780 against faces
 * that ship 400/500/600/700 only, and page titles resolved through four
 * different viewport clamps.
 *
 * These tests hold the parts a browser cannot argue with: which families exist,
 * which weights are loadable, and who owns a control role's size.
 */
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, it } from 'node:test';

const STYLES_DIR = dirname(fileURLToPath(import.meta.url));
const SRC_DIR = dirname(STYLES_DIR);
const TOKENS = readFileSync(join(STYLES_DIR, 'tokens.css'), 'utf8');
const MAIN = readFileSync(join(SRC_DIR, 'main.tsx'), 'utf8');

/** The only weights the bundled faces actually ship. */
const SHIPPED_WEIGHTS = [400, 500, 600, 700];

const cssFiles = (): string[] => readdirSync(STYLES_DIR).filter((f) => f.endsWith('.css'));

/**
 * Sheets that style the authenticated workspace. The public site carries its own
 * display type and is measured on its own routes, so it is out of scope here —
 * it is not exempt from the ladder, only held by a different pass.
 */
const workspaceFiles = (): string[] => cssFiles().filter((f) => !/^(landing|marketing-)/.test(f));

const read = (file: string): string => readFileSync(join(STYLES_DIR, file), 'utf8');

/** Declaration blocks as [selector, body] pairs. Nested at-rules are flattened. */
function blocks(css: string): Array<[string, string]> {
  return [...css.matchAll(/(^|\})([^{}]*)\{([^{}]*)\}/g)].map(([, , selector, body]) => [
    selector.replace(/\/\*[\s\S]*?\*\//g, '').trim(),
    body,
  ]);
}

describe('workspace type ladder', () => {
  it('loads a face for every weight the styles ask for', () => {
    const imported = new Set(
      [...MAIN.matchAll(/@fontsource\/([a-z-]+)\/(\d{3})\.css/g)].map(([, family, weight]) => `${family}/${weight}`),
    );
    const families = ['ibm-plex-sans', 'plus-jakarta-sans'];
    for (const family of families) {
      for (const weight of SHIPPED_WEIGHTS) {
        assert.ok(
          imported.has(`${family}/${weight}`),
          `${family} ${weight} is never imported, so the browser synthesises or ` +
            'substitutes it and the same weight renders differently per element',
        );
      }
    }
  });

  it('requests only weights that ship, so nothing is synthesised', () => {
    const offenders: string[] = [];
    for (const file of workspaceFiles()) {
      for (const [, value] of read(file).matchAll(/font-weight:\s*(\d{3})\b/g)) {
        if (!SHIPPED_WEIGHTS.includes(Number(value))) offenders.push(`${file}: font-weight: ${value}`);
      }
    }
    assert.deepEqual(
      offenders,
      [],
      'a weight between two shipped faces resolves to the nearest one, so 620 and ' +
        '650 render as 600 on one machine and 700 on another',
    );
  });

  it('gives native controls the workspace family, which they do not inherit', () => {
    const inherits = blocks(TOKENS).some(
      ([selector, body]) =>
        /\bbutton\b/.test(selector) &&
        /\binput\b/.test(selector) &&
        /\bselect\b/.test(selector) &&
        /\btextarea\b/.test(selector) &&
        /font-family:\s*inherit/.test(body),
    );
    assert.ok(
      inherits,
      'without this rule every button, input and select renders in the UA font ' +
        '(Arial here) beside IBM Plex Sans body text',
    );
  });

  it('declares the role type tokens in tokens.css alone', () => {
    const offenders = cssFiles()
      .filter((f) => f !== 'tokens.css')
      .filter((f) => /--df-(?:fs|fw)-[a-z-]+\s*:/.test(read(f)));
    assert.deepEqual(offenders, [], 'type tokens must have one owner — move these into tokens.css');
  });

  it('resolves every role token that the sheets consume', () => {
    const declared = new Set([...TOKENS.matchAll(/(--df-(?:fs|fw)-[a-z-]+)\s*:/g)].map(([, name]) => name));
    const missing = new Set<string>();
    for (const file of cssFiles()) {
      for (const [, name] of read(file).matchAll(/var\((--df-(?:fs|fw)-[a-z-]+)\)/g)) {
        if (!declared.has(name)) missing.add(`${file}: ${name}`);
      }
    }
    assert.deepEqual([...missing], [], 'a var() with no declaration silently drops the whole declaration');
  });

  it('keeps a page from outranking a control role with !important', () => {
    const offenders: string[] = [];
    for (const file of workspaceFiles()) {
      for (const [selector, body] of blocks(read(file))) {
        if (!/\.df2-(?:btn|tab|input|select)\b/.test(selector)) continue;
        for (const decl of body.split(';')) {
          if (!/font-size\s*:/.test(decl) || !/!important/.test(decl)) continue;
          if (/var\(--df-fs-/.test(decl)) continue;
          offenders.push(`${file}: ${selector.slice(-60)} {${decl.trim()}}`);
        }
      }
    }
    assert.deepEqual(
      offenders,
      [],
      'an !important literal beats the role rung, which is how one toolbar shipped ' +
        '11.5px buttons beside 13px buttons doing the same job',
    );
  });

  it('sizes a page title from the token, never from the viewport', () => {
    const offenders: string[] = [];
    for (const file of workspaceFiles()) {
      for (const [selector, body] of blocks(read(file))) {
        if (!/\.df2-page-title\b/.test(selector)) continue;
        if (/\.df2-page-hero\b/.test(selector)) continue; // marketing-style hero inside the app shell
        const size = /font-size:\s*([^;]+)/.exec(body);
        if (!size) continue;
        if (/var\(--df-fs-page-title\)/.test(size[1])) continue;
        offenders.push(`${file}: ${selector.slice(-60)} {font-size: ${size[1].trim()}}`);
      }
    }
    assert.deepEqual(
      offenders,
      [],
      'a per-page clamp() makes the same title a different size on Transfer than ' +
        'on Jobs at the same viewport',
    );
  });

  it('sizes controls in px, so a page container cannot rescale them', () => {
    const roleTokens = [...TOKENS.matchAll(/(--df-fs-(?:btn|btn-sm|btn-lg|field|field-sm|tab|chip|label))\s*:\s*([^;]+);/g)];
    assert.ok(roleTokens.length >= 6, 'the control role tokens are missing from tokens.css');
    for (const [, name, value] of roleTokens) {
      assert.match(
        value.trim(),
        /^\d+(\.\d+)?px$/,
        `${name} is ${value.trim()}; an em/rem control size inherits whatever the ` +
          'hosting card decided, which is the drift these tokens exist to stop',
      );
    }
  });
});
