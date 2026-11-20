import { useId, useRef, useState, type CSSProperties, type KeyboardEvent, type ReactNode } from "react";
import { Check, Copy } from "lucide-react";

export type CodeSnippet = {
  /** Tab label, e.g. "Trust Report". */
  label: string;
  /** The snippet exactly as the docs give it. */
  code: string;
  /** Where the snippet is written down, shown under the code. */
  source?: ReactNode;
};

type Tok = { t: "kw" | "str" | "com" | "var" | "flag" | "txt"; v: string };

const KEYWORDS = new Set(["curl", "GET", "POST", "PUT", "PATCH", "DELETE"]);

/** Splits one line into a few token kinds. No highlighting library. */
function tokenize(line: string): Tok[] {
  if (/^\s*#/.test(line)) return [{ t: "com", v: line }];
  const out: Tok[] = [];
  const re = /('[^']*'?|"[^"]*"?)|(\$[A-Za-z_][A-Za-z0-9_]*)|(\s-{1,2}[A-Za-z][\w-]*)|([A-Za-z]+)|(\s+|[^\sA-Za-z'"$-]+|-)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(line))) {
    if (m[1]) out.push({ t: "str", v: m[1] });
    else if (m[2]) out.push({ t: "var", v: m[2] });
    else if (m[3]) out.push({ t: "flag", v: m[3] });
    else if (m[4]) out.push({ t: KEYWORDS.has(m[4]) ? "kw" : "txt", v: m[4] });
    else out.push({ t: "txt", v: m[0] });
  }
  return out;
}

function Highlighted({ code }: { code: string }) {
  return (
    <>
      {code.split("\n").map((line, i) => (
        <span key={i} className="mk-code-line">
          {tokenize(line).map((tk, j) =>
            tk.t === "txt" ? tk.v : (
              <span key={j} className={`mk-code-${tk.t}`}>
                {tk.v}
              </span>
            ),
          )}
          {"\n"}
        </span>
      ))}
    </>
  );
}

/**
 * A dark code card: tabs for the documented snippets, a copy button that
 * announces "Copied", and simple token colouring through CSS classes.
 * Only for snippets that exist in the docs; never invent an endpoint.
 */
export function CodeCard({ snippets, label }: { snippets: CodeSnippet[]; label: string }) {
  const [active, setActive] = useState(0);
  const [copied, setCopied] = useState(false);
  const refs = useRef<Array<HTMLButtonElement | null>>([]);
  const base = useId();
  const snip = snippets[active];
  /** The tallest snippet sets the body's height, so tabs never shift the page. */
  const lines = Math.max(...snippets.map((s) => s.code.split("\n").length));

  const onKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const last = snippets.length - 1;
    let next = active;
    if (e.key === "ArrowRight") next = active === last ? 0 : active + 1;
    else if (e.key === "ArrowLeft") next = active === 0 ? last : active - 1;
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = last;
    else return;
    e.preventDefault();
    setActive(next);
    setCopied(false);
    refs.current[next]?.focus();
  };

  const copy = async () => {
    try {
      await navigator.clipboard.writeText(snip.code);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 2000);
    } catch {
      setCopied(false);
    }
  };

  return (
    <figure className="mk-codecard" aria-label={label}>
      <div className="mk-codecard-bar">
        {snippets.length > 1 ? (
          <div className="mk-codecard-tabs" role="tablist" aria-label={label} onKeyDown={onKey}>
            {snippets.map((s, i) => (
              <button
                key={s.label}
                ref={(el) => {
                  refs.current[i] = el;
                }}
                type="button"
                role="tab"
                id={`${base}-t${i}`}
                aria-selected={i === active}
                aria-controls={`${base}-p`}
                tabIndex={i === active ? 0 : -1}
                className={i === active ? "is-active" : undefined}
                onClick={() => {
                  setActive(i);
                  setCopied(false);
                }}
              >
                {s.label}
              </button>
            ))}
          </div>
        ) : (
          <p className="mk-codecard-title">{snip.label}</p>
        )}
        <button type="button" className="mk-codecard-copy" onClick={copy} aria-label={`Copy the ${snip.label} snippet`}>
          {copied ? <Check size={14} aria-hidden /> : <Copy size={14} aria-hidden />}
          <span aria-hidden>{copied ? "Copied" : "Copy"}</span>
        </button>
        <span className="mk-sr-only" aria-live="polite">
          {copied ? "Copied" : ""}
        </span>
      </div>
      <div
        className="mk-codecard-body"
        id={`${base}-p`}
        role={snippets.length > 1 ? "tabpanel" : undefined}
        aria-labelledby={snippets.length > 1 ? `${base}-t${active}` : undefined}
        tabIndex={0}
        style={{ "--mk-code-lines": lines } as CSSProperties}
      >
        <pre>
          <code>
            <Highlighted code={snip.code} />
          </code>
        </pre>
      </div>
      {snip.source ? <figcaption className="mk-codecard-source">{snip.source}</figcaption> : null}
    </figure>
  );
}
