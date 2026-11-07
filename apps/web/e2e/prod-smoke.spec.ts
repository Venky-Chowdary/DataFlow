import { expect, test } from "@playwright/test";

/**
 * Loads the production Vite preview (`vite build` + `vite preview`), not the
 * dev server. Dev serves `/src/main.tsx`; the built app requests `/assets/vendor-*.js`.
 * Any uncaught pageerror fails the test, including the react-vendor/vendor
 * cycle that threw `Cannot set properties of undefined (setting 'Activity')`.
 */
test("production build loads with no page errors", async ({ page }) => {
  const pageErrors: string[] = [];
  const assetUrls: string[] = [];
  page.on("pageerror", (error) => {
    pageErrors.push(error instanceof Error ? `${error.name}: ${error.message}` : String(error));
  });
  page.on("response", (response) => {
    const url = response.url();
    if (url.includes("/assets/")) assetUrls.push(url);
  });

  const response = await page.goto("/");
  expect(response?.ok()).toBe(true);
  await expect(page).toHaveTitle(/Datawrap/);
  await expect(page.getByRole("heading", { name: /Move any schema/i })).toBeVisible();

  expect(pageErrors, pageErrors.join("\n")).toEqual([]);
  expect(assetUrls.some((url) => url.includes("react-vendor"))).toBe(false);
  expect(assetUrls.some((url) => /\/assets\/vendor-[^/]+\.js$/.test(url))).toBe(true);
});
