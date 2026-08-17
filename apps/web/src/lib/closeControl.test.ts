/**
 * One close control for the whole workspace.
 *
 * Overlay closers had drifted into three treatments — a generic ghost button in
 * the drawer and dialogs, a bespoke square in the query grid, and a literal `×`
 * glyph in the toolbar search. They differ in size, radius and hit area on
 * screens the user sees side by side, and the glyph version is not an icon at
 * all, so it does not scale with the icon set.
 */
import assert from 'node:assert/strict';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, it } from 'node:test';

const SRC = join(dirname(fileURLToPath(import.meta.url)), '..');

function tsxFiles(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) out.push(...tsxFiles(full));
    else if (entry.endsWith('.tsx')) out.push(full);
  }
  return out;
}

/**
 * Attributes of every `<button …>` opening tag.
 *
 * Brace depth is tracked because a JSX handler contains `>` (`() => close()`),
 * so stopping at the first `>` reads only half the attributes and misses the
 * very controls this test exists to catch.
 */
function buttons(source: string): string[] {
  const out: string[] = [];
  const re = /<button\b/g;
  let match: RegExpExecArray | null;
  while ((match = re.exec(source))) {
    let depth = 0;
    let i = match.index + match[0].length;
    for (; i < source.length; i += 1) {
      const ch = source[i];
      if (ch === '{') depth += 1;
      else if (ch === '}') depth -= 1;
      else if (ch === '>' && depth === 0) break;
    }
    out.push(source.slice(match.index + match[0].length, i));
  }
  return out;
}

describe('overlay close controls', () => {
  it('use the shared icon button, never the generic ghost button', () => {
    const offenders: string[] = [];
    for (const file of tsxFiles(SRC)) {
      for (const attrs of buttons(readFileSync(file, 'utf8'))) {
        if (!/aria-label=["'](Close|Clear search|Close row detail)["']/.test(attrs)) continue;
        if (attrs.includes('df2-close-btn')) continue;
        offenders.push(`${file.slice(SRC.length + 1)}: ${attrs.trim().slice(0, 90)}`);
      }
    }
    assert.deepEqual(
      offenders,
      [],
      'every overlay closer must carry df2-close-btn so size, radius, hover and ' +
        'focus ring are decided in one place',
    );
  });

  it('draw the close glyph with the icon set, not a literal character', () => {
    const offenders: string[] = [];
    for (const file of tsxFiles(SRC)) {
      const source = readFileSync(file, 'utf8');
      for (const match of source.matchAll(/<button\b[^>]*>\s*([×✕✖xX])\s*<\/button>/g)) {
        offenders.push(`${file.slice(SRC.length + 1)}: ${match[1]}`);
      }
    }
    assert.deepEqual(offenders, [], 'a typed × does not match DtIcon stroke, size or colour');
  });
});
