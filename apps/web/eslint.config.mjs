/**
 * no-alert and no-restricted-globals.
 *
 * alert, confirm, and prompt are restricted globals. The web package does not
 * run a TypeScript ESLint parser, so scripts/check-no-browser-dialogs.mjs is
 * the gate (`npm test` and `npm run lint`). A local confirm from useConfirm()
 * is the in-app dialog and stays allowed. ask() collects prompt fields.
 */
export default [
  {
    files: ["src/**/*.{ts,tsx,js,jsx}"],
    rules: {
      "no-alert": "error",
      "no-restricted-globals": [
        "error",
        { name: "alert", message: "Use useToast()." },
        { name: "confirm", message: "Use useConfirm()." },
        { name: "prompt", message: "Use useConfirm().ask()." },
      ],
    },
  },
];
