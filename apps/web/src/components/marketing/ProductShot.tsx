import { useState } from "react";
import { shotSize } from "../../lib/workspaceShots";

/**
 * A crop of the live workspace, shown at half its 2x pixel size so text
 * stays legible. No app chrome: only the region the page talks about, on a
 * painted placeholder, with its intrinsic size reserved before it loads.
 * If the file fails to load, the figure is removed rather than left empty.
 */
export function ProductShot({
  src,
  alt,
  caption,
  className = "",
}: {
  src: string;
  alt: string;
  /** One sentence about what this region shows. */
  caption: string;
  className?: string;
}) {
  const [failed, setFailed] = useState(false);
  const size = shotSize(src);
  if (failed) return null;
  return (
    <figure className={`mk-crop ${className}`.trim()} style={{ maxWidth: size.width / 2 }}>
      <div className="mk-crop-frame" style={{ aspectRatio: `${size.width} / ${size.height}` }}>
        <img
          src={src}
          alt={alt}
          width={size.width}
          height={size.height}
          loading="lazy"
          decoding="async"
          onError={() => setFailed(true)}
        />
      </div>
      <figcaption className="mk-caption">{caption}</figcaption>
    </figure>
  );
}
