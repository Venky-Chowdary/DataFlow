import type { ReactNode } from "react";

/**
 * Product photography for public pages — the live workspace inside a
 * Datawrap chrome frame. Marketing must not invent a second UI; it shows
 * the operator surfaces operators already use.
 */
export function ProductShot({
  src,
  alt,
  surface,
  route,
  className = "",
}: {
  src: string;
  alt: string;
  surface: string;
  /** Optional source → dest chip. Tokens ellipsis; they never paint past the frame. */
  route?: { source: string; dest: string };
  className?: string;
}) {
  return (
    <figure className={`lp-product-shot ${className}`.trim()}>
      <div className="lp-product-shot-chrome">
        <div className="lp-product-shot-bar">
          <span className="lp-product-shot-dots" aria-hidden>
            <i />
            <i />
            <i />
          </span>
          <span className="lp-product-shot-surface">{surface}</span>
        </div>
        <div className="lp-product-shot-viewport">
          <img src={src} alt={alt} />
        </div>
      </div>
      {route ? (
        <figcaption className="lp-product-shot-route" title={`${route.source} → ${route.dest}`}>
          <span>{route.source}</span>
          <em aria-hidden>→</em>
          <span>{route.dest}</span>
        </figcaption>
      ) : null}
    </figure>
  );
}

export function ProductShotStack({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return <div className={`lp-product-shot-stack ${className}`.trim()}>{children}</div>;
}
