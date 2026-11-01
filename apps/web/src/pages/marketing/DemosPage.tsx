import { useMemo, useRef, useState } from "react";
import { VideoPlayer, videoThumb } from "../../components/marketing/VideoPlayer";
import {
  FEATURED_VIDEO_ID,
  MARKETING_VIDEOS,
  YOUTUBE_CHANNEL_URL,
  type MarketingVideo,
} from "../../lib/videoCatalog";
import type { PublicRoute } from "../../lib/publicNavigation";

const TAGS = ["All", "Overview", "Sync & CDC", "Verification", "Sources & files", "Transforms & AI"] as const;

export function DemosPage({ onNavigate }: { onNavigate: (route: PublicRoute) => void }) {
  const [current, setCurrent] = useState<MarketingVideo>(
    MARKETING_VIDEOS.find((v) => v.id === FEATURED_VIDEO_ID) ?? MARKETING_VIDEOS[0],
  );
  const [tag, setTag] = useState<(typeof TAGS)[number]>("All");
  const playerRef = useRef<HTMLDivElement>(null);

  const videos = useMemo(
    () => (tag === "All" ? MARKETING_VIDEOS : MARKETING_VIDEOS.filter((v) => v.tag === tag)),
    [tag],
  );

  const play = (video: MarketingVideo) => {
    setCurrent(video);
    playerRef.current?.scrollIntoView({ behavior: "smooth", block: "nearest" });
  };

  return (
    <div className="lp-mkt-page lp-demos">
      <section className="lp-demos-hero" aria-label="Video demos">
        <div className="lp-mkt-wrap">
          <p className="lp-sales-kicker">Video demos</p>
          <h1>Watch the engine prove a transfer.</h1>
          <p className="lp-sales-hero-lead">
            Real runs on the production engine — quarantine, replay, drift review, CDC, and the
            checksum match at the end. No slideware.
          </p>
        </div>
      </section>

      <section className="lp-demos-player" aria-label="Featured demo" ref={playerRef}>
        <div className="lp-mkt-wrap">
          <VideoPlayer key={current.id} videoId={current.id} title={current.title} />
          <div className="lp-demos-current">
            <span className="lp-demos-tag">{current.tag}</span>
            <h2>{current.title}</h2>
            <p>{current.blurb}</p>
          </div>
        </div>
      </section>

      <section className="lp-demos-grid-section" aria-label="All demos">
        <div className="lp-mkt-wrap">
          <div className="lp-demos-filters" role="tablist" aria-label="Filter demos">
            {TAGS.map((t) => (
              <button
                key={t}
                type="button"
                role="tab"
                aria-selected={tag === t}
                className={`lp-demos-filter ${tag === t ? "is-active" : ""}`}
                onClick={() => setTag(t)}
              >
                {t}
              </button>
            ))}
          </div>
          <div className="lp-demos-grid">
            {videos.map((video) => (
              <button
                key={video.id}
                type="button"
                className={`lp-demos-card ${video.id === current.id ? "is-current" : ""}`}
                onClick={() => play(video)}
              >
                <span className="lp-demos-thumb">
                  <img src={videoThumb(video.id, "hq")} alt="" loading="lazy" />
                  <span className="lp-video-play lp-video-play--sm" aria-hidden="true">
                    <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor">
                      <path d="M8 5v14l11-7z" />
                    </svg>
                  </span>
                </span>
                <span className="lp-demos-card-body">
                  <span className="lp-demos-tag">{video.tag}</span>
                  <strong>{video.title}</strong>
                  <span>{video.blurb}</span>
                </span>
              </button>
            ))}
          </div>
          <div className="lp-demos-foot">
            <a
              className="lp-btn lp-btn--outline"
              href={YOUTUBE_CHANNEL_URL}
              target="_blank"
              rel="noopener noreferrer"
            >
              Subscribe on YouTube
            </a>
            <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("help")}>
              Read the docs instead
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}
