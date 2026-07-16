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
    // Bundle the whole app into a single JS/CSS pair.  This removes runtime
    // dynamic-import chunk fetches that can 404 after a production deploy
    // when the user's browser still holds an older main bundle in memory.
    cssCodeSplit: false,
    rollupOptions: {
      output: {
        inlineDynamicImports: true,
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
