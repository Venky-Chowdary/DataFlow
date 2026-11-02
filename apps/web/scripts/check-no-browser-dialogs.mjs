/**
 * ESLint no-alert and no-restricted-globals for this package.
 *
 * The web app has no TypeScript ESLint parser, so `npm test` and `npm run lint`
 * run this scanner instead. It bans the same globals those rules ban:
 * alert, confirm, and prompt — including window / globalThis / self.
 * A local `confirm` from useConfirm() is the in-app dialog and is allowed.
 * String and comment text is not a call site.
 */
import { readdirSync, readFileSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const WEB = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const SRC = path.join(WEB, "src");

export function maskNonCode(source) {
  return source
    .replace(/\/\*[\s\S]*?\*\//g, (block) => block.replace(/[^\n]/g, " "))
    .replace(/\/\/[^\n]*/g, (line) => " ".repeat(line.length))
    .replace(/'(?:\\.|[^'\\\n])*'|"(?:\\.|[^"\\\n])*"|`(?:\\.|[^`\\])*`/g, (literal) => (
      literal.replace(/[^\n]/g, " ")
    ));
}

function lineOf(source, index) {
  let line = 1;
  for (let i = 0; i < index; i += 1) {
    if (source[i] === "\n") line += 1;
  }
  return line;
}

function declaresConfirm(masked) {
  return /(?:const|let|var)\s*\{[^}]*\bconfirm\b/.test(masked)
    || /(?:const|let|var|function)\s+confirm\b/.test(masked);
}

export function browserDialogViolations(source, filename = "file.tsx") {
  const masked = maskNonCode(source);
  const rules = [
    {
      id: "no-restricted-globals",
      re: /\b(?:window|globalThis|self)\s*\.\s*(?:alert|confirm|prompt)\b/g,
    },
    {
      id: "no-alert",
      re: /(?<![\w$.])(?:alert|prompt)\s*\(/g,
    },
  ];
  if (!declaresConfirm(masked)) {
    rules.push({
      id: "no-alert",
      re: /(?<![\w$.])confirm\s*\(/g,
    });
  }
  const hits = [];
  for (const rule of rules) {
    for (const match of masked.matchAll(rule.re)) {
      hits.push({
        rule: rule.id,
        file: filename,
        line: lineOf(masked, match.index ?? 0),
        text: match[0].trim(),
      });
    }
  }
  return hits;
}

function walk(dir, out) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name === "node_modules" || entry.name === "dist") continue;
      walk(full, out);
    } else if (/\.(tsx?|jsx?)$/.test(entry.name)) {
      out.push(full);
    }
  }
}

export function scanWebSrc() {
  const files = [];
  walk(SRC, files);
  const hits = [];
  for (const file of files.sort()) {
    hits.push(...browserDialogViolations(readFileSync(file, "utf8"), path.relative(WEB, file)));
  }
  return hits;
}

export function fixtureProblems() {
  const cases = [
    ["window.confirm(\"x\")", 1],
    ["window.alert('no')", 1],
    ["const reason = window.prompt(\"why\")", 1],
    ["globalThis.alert(1)", 1],
    ["alert(\"x\")", 1],
    ["prompt(\"x\")", 1],
    ["confirm(\"native\")", 1],
    ["const { confirm } = useConfirm();\nawait confirm({ title: \"Sync\" });", 0],
    ["const html = '<img src=x onerror=\"alert(1)\">';", 0],
    ["// window.confirm(\"x\")\nconst ok = true;", 0],
    ["foo.alert(1)", 0],
  ];
  const problems = [];
  for (const [source, expected] of cases) {
    const hits = browserDialogViolations(source, "fixture.tsx");
    if (hits.length !== expected) problems.push({ source, expected, actual: hits.length, hits });
  }
  return problems;
}

const invokedDirectly = process.argv[1]
  && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (invokedDirectly) {
  const problems = fixtureProblems();
  const hits = scanWebSrc();
  if (problems.length || hits.length) {
    for (const problem of problems) {
      console.error(`fixture expected ${problem.expected} hit(s), got ${problem.actual}: ${problem.source}`);
    }
    for (const hit of hits) {
      console.error(`${hit.rule}: ${hit.file}:${hit.line} ${hit.text}`);
    }
    console.error("no-alert / no-restricted-globals: use useConfirm() or useToast().");
    process.exit(1);
  }
  console.log("no-alert / no-restricted-globals: clean");
}
