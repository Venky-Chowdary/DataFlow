import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "path";

export default defineConfig({
  plugins: [react()],
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
