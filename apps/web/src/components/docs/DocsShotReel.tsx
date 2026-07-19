import { useEffect, useState } from "react";

export interface DocsShotFrame {
  src: string;
  alt: string;
  caption?: string;
}

interface DocsShotReelProps {
  frames: DocsShotFrame[];
  intervalMs?: number;
  className?: string;
}

/** Crossfades real workspace screenshots with a slow Ken Burns pan. */
export function DocsShotReel({ frames, intervalMs = 4200, className = "" }: DocsShotReelProps) {
  const [index, setIndex] = useState(0);
  const safe = frames.filter((f) => f.src);

  useEffect(() => {
    if (safe.length < 2) return;
    const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (reduced) return;
    const id = window.setInterval(() => {
      setIndex((i) => (i + 1) % safe.length);
    }, intervalMs);
    return () => window.clearInterval(id);
  }, [safe.length, intervalMs]);

  if (safe.length === 0) return null;
  const active = safe[index] ?? safe[0];

  return (
    <figure className={`docs-shot-reel ${className}`.trim()}>
      <div className="docs-shot-reel-stage">
        {safe.map((frame, i) => (
          <img
            key={frame.src}
            src={frame.src}
            alt={frame.alt}
            className={`docs-shot-reel-frame ${i === index ? "is-active" : ""}`}
            loading={i === 0 ? "eager" : "lazy"}
          />
        ))}
        <div className="docs-shot-reel-vignette" aria-hidden />
      </div>
      {active.caption ? <figcaption>{active.caption}</figcaption> : null}
      {safe.length > 1 ? (
        <div className="docs-shot-reel-dots" role="tablist" aria-label="Screenshot frames">
          {safe.map((frame, i) => (
            <button
              key={frame.src}
              type="button"
              role="tab"
              aria-selected={i === index}
              className={i === index ? "is-active" : ""}
              onClick={() => setIndex(i)}
            />
          ))}
        </div>
      ) : null}
    </figure>
  );
}
