import { defineConfig, devices } from "@playwright/test";
export default defineConfig({
  testDir: "./e2e",
  timeout: 90_000,
  use: { baseURL: "http://127.0.0.1:5173" },
  projects: [{ name: "capture", testMatch: /dbp-evidence-capture\.spec\.ts/, use: { ...devices["Desktop Chrome"] } }],
});
