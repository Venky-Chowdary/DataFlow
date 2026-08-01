/**
 * Minimal, XSS-safe markdown for Pilot answers.
 *
 * The engine already emits real markdown — GFM tables for aggregation and
 * sample rows, fenced blocks holding the exact SQL/Mongo it ran, and bullet
 * lists for gates and casts. The previous renderer only understood bold, inline
 * code and newlines, so a grouped count arrived as literal `| status | c_0 |`
 * pipes and the citable query rendered as broken backticks. For a data product
 * the table *is* the answer, so it has to render as a table.
 *
 * Safety model is unchanged and non-negotiable: every piece of model-authored
 * text is HTML-escaped before any markup is produced, and the only tags that
 * can ever reach the DOM are the fixed set this module writes itself.
 */

/** Escape HTML before limited markdown transforms — prevents XSS from LLM/user content */
export function escapeHtml(text: string): string {
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

/** Inline marks, applied only to already-escaped text. */
function inline(escaped: string): string {
  return escaped
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/`([^`]+?)`/g, "<code>$1</code>");
}

const FENCE_RE = /^\s*```(\w*)\s*$/;
const TABLE_DIVIDER_RE = /^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$/;
const BULLET_RE = /^\s*[-*•]\s+(.*)$/;
const INDENTED_BULLET_RE = /^\s{2,}[-–]\s+(.*)$/;

type Align = "left" | "right" | "center";

function splitRow(line: string): string[] {
  let text = line.trim();
  if (text.startsWith("|")) text = text.slice(1);
  if (text.endsWith("|")) text = text.slice(0, -1);
  return text.split("|").map((c) => c.trim());
}

function alignmentsFrom(divider: string): Align[] {
  return splitRow(divider).map((cell) => {
    const left = cell.startsWith(":");
    const right = cell.endsWith(":");
    if (left && right) return "center";
    if (right) return "right";
    return "left";
  });
}

function isTableStart(lines: string[], i: number): boolean {
  return (
    lines[i].includes("|")
    && i + 1 < lines.length
    && TABLE_DIVIDER_RE.test(lines[i + 1])
    && lines[i + 1].includes("-")
  );
}

/**
 * Render a numeric-looking cell right-aligned even when the table did not say
 * so. Counts and sums read wrong ragged-left, and the engine's own tables
 * already mark measure columns this way.
 */
function cellClass(align: Align): string {
  return align === "left" ? "" : ` class="df2-md-${align}"`;
}

function renderTable(lines: string[], start: number): { html: string; next: number } {
  const headers = splitRow(lines[start]);
  const aligns = alignmentsFrom(lines[start + 1]);
  const alignAt = (i: number): Align => aligns[i] || "left";

  let i = start + 2;
  const rows: string[][] = [];
  while (i < lines.length && lines[i].includes("|") && lines[i].trim() !== "") {
    rows.push(splitRow(lines[i]));
    i += 1;
  }

  const head = headers
    .map((h, idx) => `<th${cellClass(alignAt(idx))} scope="col">${inline(escapeHtml(h))}</th>`)
    .join("");
  const body = rows
    .map((row) => {
      const cells = headers
        .map((_, idx) => {
          const raw = row[idx] ?? "";
          // An empty cell is a real signal in data (NULL vs blank string), so
          // mark it rather than rendering an ambiguous gap.
          const content = raw === ""
            ? '<span class="df2-md-empty">—</span>'
            : inline(escapeHtml(raw));
          return `<td${cellClass(alignAt(idx))}>${content}</td>`;
        })
        .join("");
      return `<tr>${cells}</tr>`;
    })
    .join("");

  return {
    html:
      '<div class="df2-md-table-wrap"><table class="df2-md-table">'
      + `<thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`,
    next: i,
  };
}

function renderFence(lines: string[], start: number): { html: string; next: number } {
  const lang = (lines[start].match(FENCE_RE)?.[1] || "").toLowerCase();
  let i = start + 1;
  const body: string[] = [];
  while (i < lines.length && !FENCE_RE.test(lines[i])) {
    body.push(lines[i]);
    i += 1;
  }
  // Code is escaped but never inline-formatted; backticks and asterisks inside
  // a query are literal characters, not markup.
  const code = escapeHtml(body.join("\n"));
  const label = lang ? `<span class="df2-md-code-lang">${escapeHtml(lang)}</span>` : "";
  return {
    html: `<div class="df2-md-code">${label}<pre><code>${code}</code></pre></div>`,
    next: i < lines.length ? i + 1 : i,
  };
}

export function renderSafeMarkdown(text: string): string {
  const lines = String(text ?? "").split("\n");
  const out: string[] = [];
  let paragraph: string[] = [];
  let list: string[] = [];

  const flushParagraph = () => {
    if (paragraph.length === 0) return;
    out.push(`<p>${paragraph.join("<br/>")}</p>`);
    paragraph = [];
  };
  const flushList = () => {
    if (list.length === 0) return;
    out.push(`<ul class="df2-md-list">${list.join("")}</ul>`);
    list = [];
  };
  const flushAll = () => {
    flushParagraph();
    flushList();
  };

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];

    if (FENCE_RE.test(line)) {
      flushAll();
      const fence = renderFence(lines, i);
      out.push(fence.html);
      i = fence.next - 1;
      continue;
    }

    if (isTableStart(lines, i)) {
      flushAll();
      const table = renderTable(lines, i);
      out.push(table.html);
      i = table.next - 1;
      continue;
    }

    const nested = line.match(INDENTED_BULLET_RE);
    const bullet = nested ? null : line.match(BULLET_RE);
    if (bullet || nested) {
      flushParagraph();
      const content = (bullet ? bullet[1] : nested![1]) || "";
      const cls = nested ? ' class="df2-md-sub"' : "";
      list.push(`<li${cls}>${inline(escapeHtml(content))}</li>`);
      continue;
    }

    if (line.trim() === "") {
      flushAll();
      continue;
    }

    flushList();
    paragraph.push(inline(escapeHtml(line)));
  }

  flushAll();
  return out.join("");
}
