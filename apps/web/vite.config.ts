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
  server: {
    port: 5177,
    proxy: {
      "/api": "http://127.0.0.1:8001",
    },
  },
});
