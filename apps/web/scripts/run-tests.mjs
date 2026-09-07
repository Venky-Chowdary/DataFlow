/**
 * Run the web test suite on any platform.
 *
 * `tsx --test src/**\/*.test.ts` relies on the shell expanding the glob, so on
 * Windows the runner receives the literal pattern, matches nothing, and reports
 * success without running a single test. This collects the files itself.
 */
import { spawn } from "node:child_process";
import { readdirSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const WEB = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const SRC = path.join(WEB, "src");

function testFiles(dir) {
  const out = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) out.push(...testFiles(full));
    else if (/\.test\.tsx?$/.test(entry.name)) out.push(full);
  }
  return out;
}

const files = testFiles(SRC).sort();
if (files.length === 0) {
  console.error(`No test files found under ${SRC}`);
  process.exit(1);
}

const args = ["--test", ...process.argv.slice(2), ...files];
const child = spawn(process.platform === "win32" ? "tsx.cmd" : "tsx", args, {
  cwd: WEB,
  stdio: "inherit",
  shell: process.platform === "win32",
});
child.on("exit", (code, signal) => process.exit(signal ? 1 : (code ?? 1)));
