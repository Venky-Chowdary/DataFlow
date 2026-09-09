/**
 * Control-ladder contract — Mode / FilterTabs / dest-write / Advanced
 * must share one height token. Destination looked ragged because Studio
 * restated a 28px tab while dest-mode stayed on --df-btn-height.
 */
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, it } from 'node:test';

const STYLES = dirname(fileURLToPath(import.meta.url));
const TOKENS = readFileSync(join(STYLES, 'tokens.css'), 'utf8');
const STUDIO = readFileSync(join(STYLES, 'transfer-studio.css'), 'utf8');
const CONSISTENCY = readFileSync(join(STYLES, 'ui-consistency.css'), 'utf8');
const PLATFORM = readFileSync(join(STYLES, 'enterprise-platform.css'), 'utf8');
const ENTERPRISE = readFileSync(join(STYLES, 'enterprise-ui.css'), 'utf8');

describe('control density ladder', () => {
  it('aliases --df-tab-height to --df-control-height (one rung)', () => {
    assert.match(TOKENS, /\.df2-app\s*\{[^}]*--df-tab-height:\s*var\(--df-control-height\)/s);
    const firstRoot = TOKENS.slice(TOKENS.indexOf(':root'), TOKENS.indexOf('.df2-app'));
    assert.match(firstRoot, /--df-tab-height:\s*var\(--df-control-height\)/);
    assert.doesNotMatch(firstRoot, /--df-tab-height:\s*\d+px/);
  });

  it('declares segment surface tokens once (theme remaps, no sibling sheet)', () => {
    for (const name of [
      '--df-seg-track',
      '--df-seg-active',
      '--df-seg-ink',
      '--df-seg-ink-active',
      '--df-seg-border',
    ]) {
      assert.match(TOKENS, new RegExp(`${name.replace(/-/g, '\\-')}:`));
    }
    assert.match(TOKENS, /\[data-theme="dark"\]/);
    assert.doesNotMatch(TOKENS, /\.df2-tabs-v2|\.df2-segment-refresh/);
  });

  it('does not shrink Studio tabs below the control ladder', () => {
    assert.doesNotMatch(
      STUDIO,
      /\.df2-page-transfer-studio[^{]*\.df2-tab[^{]*\{[^}]*min-height:\s*28px/,
    );
  });

  it('dest-mode track uses the control-height token', () => {
    assert.match(STUDIO, /\.df2-dest-mode-toggle\s*\{[^}]*height:\s*var\(--df-control-height\)/s);
    assert.match(STUDIO, /\.df2-dest-mode-toggle\s*\{[^}]*background:\s*var\(--df-seg-track\)/s);
  });

  it('dest fields next to Mode share the control-height token (no 34px leftover)', () => {
    assert.match(
      STUDIO,
      /\.df2-dest-connector-search\s*\{[^}]*height:\s*var\(--df-control-height\)/s,
    );
    assert.match(
      STUDIO,
      /\.df2-dest-advanced-btn[\s\S]*?height:\s*var\(--df-control-height\)/,
    );
    assert.match(
      STUDIO,
      /\.df2-dest-type-filter select\s*\{[^}]*height:\s*var\(--df-control-height\)/s,
    );
    assert.match(
      STUDIO,
      /\.df2-dest-engine-select\s*\{[^}]*height:\s*var\(--df-control-height\)/s,
    );
    assert.doesNotMatch(
      STUDIO,
      /\.df2-dest-connector-search[^{]*\{[^}]*height:\s*34px/,
    );
    assert.doesNotMatch(
      STUDIO,
      /\.df2-dest-engine-select[^{]*\{[^}]*height:\s*34px/,
    );
  });

  it('inputs use the control-height token, not a 40px literal', () => {
    assert.match(
      CONSISTENCY,
      /\.df2-app \.df2-input[\s\S]*?min-height:\s*var\(--df-control-height\)/,
    );
    assert.doesNotMatch(
      CONSISTENCY,
      /\.df2-app \.df2-input[\s\S]*?min-height:\s*40px/,
    );
  });

  it('platform tabs and segments read the shared track tokens', () => {
    assert.match(PLATFORM, /\.df2-tabs\s*\{[^}]*background:\s*var\(--df-seg-track\)/s);
    assert.match(PLATFORM, /\.df2-segment\s*\{[^}]*background:\s*var\(--df-seg-track\)/s);
    assert.match(ENTERPRISE, /\.df2-app \.df2-dest-mode-toggle/);
  });
});
