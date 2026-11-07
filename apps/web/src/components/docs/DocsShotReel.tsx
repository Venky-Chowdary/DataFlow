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
  /** Short chrome label — e.g. Transfer Studio. Never a long destination FQDN. */
  surface?: string;
}

/**
 * Crossfades real workspace screenshots inside the same Datawrap chrome
 * as ProductShot. Marketing must not invent a second UI or leave raw
 * Ken Burns crops without a frame.
 */
export function DocsShotReel({
  frames,
  intervalMs = 4200,
  className = "",
  surface = "Workspace",
}: DocsShotReelProps) {
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
    <figure className={`docs-shot-reel lp-product-shot ${className}`.trim()}>
      <div className="lp-product-shot-chrome">
        <div className="lp-product-shot-bar">
          <span className="lp-product-shot-dots" aria-hidden>
            <i />
            <i />
            <i />
          </span>
          <span className="lp-product-shot-surface">{surface}</span>
        </div>
        <div className="docs-shot-reel-stage lp-product-shot-viewport">
          {safe.map((frame, i) => (
            <img
              key={`${frame.src}-${i}`}
              src={frame.src}
              alt={frame.alt}
              className={`docs-shot-reel-frame ${i === index ? "is-active" : ""}`}
              loading={i === 0 ? "eager" : "lazy"}
            />
          ))}
        </div>
      </div>
      {active.caption ? <figcaption className="docs-shot-reel-caption">{active.caption}</figcaption> : null}
      {safe.length > 1 ? (
        <div className="docs-shot-reel-dots" role="tablist" aria-label="Screenshot frames">
          {safe.map((frame, i) => (
            <button
              key={`${frame.src}-dot-${i}`}
              type="button"
              role="tab"
              aria-selected={i === index}
              aria-label={`Show screenshot ${i + 1} of ${safe.length}: ${frame.alt}`}
              className={i === index ? "is-active" : ""}
              onClick={() => setIndex(i)}
            >
              <span className="docs-shot-reel-dot" aria-hidden />
            </button>
          ))}
        </div>
      ) : null}
    </figure>
  );
}
