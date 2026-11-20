import { useRef, type ReactNode } from "react";

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
  const dialog = useRef<HTMLDialogElement>(null);
  return (
    <figure className={`lp-product-shot ${className}`.trim()}>
      <div className="lp-product-shot-chrome">
        <div className="lp-product-shot-bar">
          <span className="lp-product-shot-surface">{surface}</span>
          <button type="button" className="mk-shot-expand" onClick={() => dialog.current?.showModal()}>
            View full size
          </button>
        </div>
        <div className="lp-product-shot-viewport">
          <img src={src} alt={alt} loading="lazy" decoding="async" />
        </div>
      </div>
      <dialog ref={dialog} className="mk-shot-dialog" aria-label={surface} onClick={(e) => e.target === dialog.current && dialog.current?.close()}>
        <div className="mk-shot-dialog-bar">
          <p>{surface}</p>
          <button type="button" className="mk-btn mk-btn--secondary mk-btn--sm" onClick={() => dialog.current?.close()}>
            Close
          </button>
        </div>
        <img src={src} alt={alt} loading="lazy" decoding="async" />
      </dialog>
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
