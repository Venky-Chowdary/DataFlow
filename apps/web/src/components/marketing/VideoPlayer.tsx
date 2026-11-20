import { useState, type ReactNode } from "react";

/** Privacy-first YouTube player: a poster drawn in code, and the
 * youtube-nocookie embed only after the visitor presses play. */

export function videoThumb(id: string, quality: "maxres" | "hq" = "maxres"): string {
  return quality === "maxres"
    ? `https://i.ytimg.com/vi/${id}/maxresdefault.jpg`
    : `https://i.ytimg.com/vi/${id}/hqdefault.jpg`;
}

export function videoEmbedUrl(id: string): string {
  return `https://www.youtube-nocookie.com/embed/${id}?autoplay=1&rel=0`;
}

export function VideoPlayer({
  videoId,
  title,
  tag,
  glyph,
  showTitle = true,
  poster = true,
  backdrop,
}: {
  videoId: string;
  title: string;
  /** Category shown on the coded poster. */
  tag?: string;
  /** A small coded mark for the topic. */
  glyph?: ReactNode;
  /** The featured player shows its title beside the poster instead. */
  showTitle?: boolean;
  /** Show the poster; false renders the iframe immediately. */
  poster?: boolean;
  /** A picture behind the play button (product UI rendered in code). It is
   * decorative and inert; the button stays the only control. */
  backdrop?: ReactNode;
}) {
  const [playing, setPlaying] = useState(!poster);

  if (playing) {
    return (
      <div className="lp-video-frame">
        <iframe
          src={videoEmbedUrl(videoId)}
          title={title}
          loading="lazy"
          allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
          allowFullScreen
          referrerPolicy="strict-origin-when-cross-origin"
        />
      </div>
    );
  }

  // A poster drawn in code: nothing is requested from YouTube until the
  // visitor presses play.
  const facade = (
    <button type="button" className={`lp-video-facade mk-poster${backdrop ? " has-backdrop" : ""}`} onClick={() => setPlaying(true)}>
      <span className="mk-poster-art" aria-hidden="true">
        {tag ? (
          <span className="mk-poster-tag">
            {glyph}
            {tag}
          </span>
        ) : null}
        {showTitle ? <span className="mk-poster-title">{title}</span> : null}
      </span>
      <span className="mk-poster-play">
        <svg viewBox="0 0 24 24" width="22" height="22" fill="currentColor" aria-hidden="true" focusable="false">
          <path d="M8 5v14l11-7z" />
        </svg>
        <span>Play video</span>
        <span className="mk-sr-only">: {title}</span>
      </span>
    </button>
  );
  if (!backdrop) return facade;
  return (
    <div className="mk-poster-stage">
      <div className="mk-poster-backdrop" aria-hidden="true" inert>
        {backdrop}
      </div>
      {facade}
    </div>
  );
}
