import { useState } from "react";

/** Privacy-first YouTube player: renders a thumbnail facade and only loads the
 * youtube-nocookie embed after the visitor clicks play — no Google tracker on
 * page load, standard practice on enterprise marketing sites. */

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
  poster = true,
}: {
  videoId: string;
  title: string;
  /** Show the thumbnail facade; false renders the iframe immediately. */
  poster?: boolean;
}) {
  const [playing, setPlaying] = useState(!poster);
  const [thumbQuality, setThumbQuality] = useState<"maxres" | "hq">("maxres");

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

  return (
    <button
      type="button"
      className="lp-video-facade"
      onClick={() => setPlaying(true)}
      aria-label={`Play video: ${title}`}
    >
      <img
        src={videoThumb(videoId, thumbQuality)}
        alt=""
        loading="lazy"
        onError={() => {
          // maxresdefault 404s on lower-res uploads — fall back to hqdefault.
          if (thumbQuality === "maxres") setThumbQuality("hq");
        }}
      />
      <span className="lp-video-play" aria-hidden="true">
        <svg viewBox="0 0 24 24" width="26" height="26" fill="currentColor">
          <path d="M8 5v14l11-7z" />
        </svg>
      </span>
    </button>
  );
}
