import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";
import path from "path";
import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";

const MARKETING_BASE_MARKER = ".df2-css-split-marketing-base";

/**
 * Landing and marketing-layout sit ahead of the workspace sheets. Keeping
 * them in the same file pushed that file past the CSS chunk budget. The
 * marker rule is the cut: the head stays the first stylesheet, and the
 * workspace file still loads after it so its overrides win.
 */
function splitMarketingBaseCss(): Plugin {
  let baseHref = "";
  return {
    name: "split-marketing-base-css",
    generateBundle(_options, bundle) {
      let head = "";
      let digest = "";
      for (const item of Object.values(bundle)) {
        if (item.type !== "asset" || !item.fileName.endsWith(".css")) continue;
        const source = typeof item.source === "string" ? item.source : "";
        const at = source.indexOf(MARKETING_BASE_MARKER);
        if (at < 0) continue;
        const end = source.indexOf("}", at);
        if (end < 0) this.error("marketing base marker is not a complete rule");
        head = source.slice(0, end + 1);
        const tail = source.slice(end + 1);
        if (head.length < 50_000 || tail.length < 50_000) {
          this.error("marketing base split was not at the stylesheet boundary");
        }
        item.source = tail;
        digest = createHash("sha256").update(head).digest("hex").slice(0, 8);
        break;
      }
      if (!digest) this.error("marketing base marker missing from the built CSS");
      const baseName = `assets/marketing-base-${digest}.css`;
      this.emitFile({ type: "asset", fileName: baseName, source: head });
      baseHref = `/${baseName}`;
    },
    writeBundle(options) {
      const dir = options.dir;
      if (!dir || !baseHref) this.error("marketing base stylesheet was not emitted");
      const htmlPath = path.join(dir, "index.html");
      const htmlSource = readFileSync(htmlPath, "utf8");
      const next = htmlSource.replace(
        /^([ \t]*)(<link rel="stylesheet"[^>]*href="\/assets\/index-[^"]+\.css">)/m,
        `$1<link rel="stylesheet" crossorigin href="${baseHref}">\n$1$2`,
      );
      if (next === htmlSource) this.error("could not insert the marketing base stylesheet link");
      writeFileSync(htmlPath, next);
    },
  };
}

export default defineConfig({
  plugins: [react(), splitMarketingBaseCss()],
  resolve: {
    alias: {
      "@dataflow/design-system": path.resolve(__dirname, "../../packages/design-system/src"),
    },
  },
  build: {
    // Phase F9 — route/vendor code-split. Hashed chunk filenames invalidate
    // caches on deploy; prefer a full page reload after release notes rather
    // than inlining everything into a 1.5MB monolith (audit §1.3 D5).
    cssCodeSplit: true,
    chunkSizeWarningLimit: 900,
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (id.includes("node_modules")) {
            if (id.includes("react-dom") || id.includes("/react/")) {
              return "react-vendor";
            }
            return "vendor";
          }
          const norm = id.replace(/\\/g, "/");
          if (
            norm.includes("/pages/TransferPage") ||
            norm.includes("/pages/transfer/") ||
            norm.includes("/components/transfer/")
          ) {
            return "transfer-studio";
          }
          if (norm.includes("/pages/marketing/")) {
            return "marketing";
          }
        },
      },
    },
  },
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: {
      "/api": "http://127.0.0.1:8001",
    },
  },
});
