import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, it } from "node:test";

const webRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = join(webRoot, "../../..");

function read(path: string): string {
  return readFileSync(path, "utf8");
}

describe("studio surfaces", () => {
  it("keeps the existing mark and does not restyle the legacy brand fill", () => {
    const mark = read(join(repoRoot, "packages/design-system/src/brand/mark.ts"));
    const tokens = read(join(webRoot, "styles/tokens.css"));
    assert.match(mark, /tile:\s*"#0A3D3A"/);
    assert.match(mark, /strap:\s*"#F59E0B"/);
    assert.match(tokens, /--df-brand:\s*#0d9488/);
    assert.match(tokens, /--df-logo-tile:\s*#0a3d3a/i);
    assert.match(tokens, /--df-brand-tile:\s*var\(--df-logo-tile\)/);
  });

  it("loads studio tokens only inside the new surfaces", () => {
    const styles = read(join(webRoot, "styles/app-styles.css"));
    const studio = read(join(webRoot, "styles/studio-next.css"));
    const tokens = read(join(webRoot, "styles/tokens.css"));
    assert.match(styles, /@import "\.\/studio-next\.css"/);
    assert.doesNotMatch(studio, /#[0-9a-fA-F]{3,8}/);
    assert.doesNotMatch(studio, /!important/);
    assert.doesNotMatch(studio, /z-index:\s*\d/);
    assert.match(studio, /z-index:\s*var\(--df-z-/);
    const tokenValues = [...tokens.matchAll(/--df-z-[a-z0-9-]+:\s*(\d+)/g)].map((match) => match[1]);
    assert.ok(tokenValues.length >= 16);
    assert.equal(new Set(tokenValues).size, tokenValues.length);
    assert.match(studio, /\.df2-studio-next \.df2-btn-primary[\s\S]*--df-accent:\s*var\(--df-brand-tile\)/);
  });

  it("gates the schema browser and canvas behind both flags", () => {
    const page = read(join(webRoot, "pages/TransferPage.tsx"));
    const step = read(join(webRoot, "pages/transfer/TransferTransformStep.tsx"));
    const panel = read(join(webRoot, "components/transfer/TransferSourceSchemaPanel.tsx"));
    assert.match(page, /useStudioFlags\(\)/);
    assert.match(page, /<TransferSourceSchemaPanel/);
    assert.match(page, /schemaBrowser=\{studioFlags\.schemaBrowser\}/);
    assert.match(panel, /!schemaBrowser \|\| sourceKind !== "database" \|\| !connectorId/);
    assert.match(page, /canvasEnabled=\{studioFlags\.canvas\}/);
    assert.match(step, /canvasEnabled \? \([\s\S]*<TransferTransformCanvas/);
    assert.match(step, /<TransformStepBuilder/);
  });

  it("keeps xyflow on the dynamic canvas chunk", () => {
    const vite = read(join(webRoot, "../vite.config.ts"));
    const flow = read(join(webRoot, "components/transfer/TransformFlow.tsx"));
    const canvas = read(join(webRoot, "components/transfer/TransferTransformCanvas.tsx"));
    assert.match(flow, /from "@xyflow\/react"/);
    assert.match(canvas, /lazy\(\(\) => import\("\.\/TransformFlow"\)\)/);
    assert.doesNotMatch(canvas, /from "@xyflow\/react"/);
    assert.match(vite, /@xyflow/);
    assert.match(vite, /return "studio-canvas"/);
  });
});
