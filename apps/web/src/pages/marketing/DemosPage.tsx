import { useMemo, useRef, useState } from "react";
import { MarketingHeroBand } from "../../components/marketing/MarketingHeroBand";
import { VideoPlayer } from "../../components/marketing/VideoPlayer";
import {
  FEATURED_VIDEO_ID,
  MARKETING_VIDEOS,
  YOUTUBE_CHANNEL_URL,
  type MarketingVideo,
} from "../../lib/videoCatalog";
import type { PublicRoute } from "../../lib/publicNavigation";

const TAGS = ["All", "Overview", "Sync & CDC", "Verification", "Sources & files", "Transforms & AI"] as const;

function scrollTo(el: HTMLElement | null, block: ScrollLogicalPosition) {
  let reduce = false;
  try {
    reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  } catch {
    reduce = true;
  }
  el?.scrollIntoView({ behavior: reduce ? "auto" : "smooth", block });
}

export function DemosPage({ onNavigate }: { onNavigate: (route: PublicRoute) => void }) {
  const [current, setCurrent] = useState<MarketingVideo>(
    MARKETING_VIDEOS.find((v) => v.id === FEATURED_VIDEO_ID) ?? MARKETING_VIDEOS[0],
  );
  const [tag, setTag] = useState<(typeof TAGS)[number]>("All");
  const playerRef = useRef<HTMLElement>(null);

  const videos = useMemo(() => (tag === "All" ? MARKETING_VIDEOS : MARKETING_VIDEOS.filter((v) => v.tag === tag)), [tag]);

  const choose = (video: MarketingVideo) => {
    setCurrent(video);
    scrollTo(playerRef.current, "nearest");
  };

  return (
    <div className="mk-page mk-demos">
      <MarketingHeroBand
        kicker="Video demos"
        title="Watch a transfer prove itself."
        lead="Recorded runs on the production engine: mapping, change data capture, quarantine and replay, and the read-back at the end."
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={() => scrollTo(playerRef.current, "start")}>
              Watch the featured demo
            </button>
            <a className="mk-btn mk-btn--secondary mk-btn--lg" href={YOUTUBE_CHANNEL_URL} target="_blank" rel="noopener noreferrer">
              Subscribe on YouTube
            </a>
          </div>
        }
      />

      <section className="mk-section mk-section--tight mk-demos-player" aria-labelledby="mk-demo-current" ref={playerRef}>
        <div className="mk-wrap mk-demos-stage">
          <VideoPlayer key={current.id} videoId={current.id} title={current.title} tag={current.tag} />
          <div className="mk-demos-current">
            <p className="mk-story-label">{current.tag}</p>
            <h2 id="mk-demo-current">{current.title}</h2>
            <p>{current.blurb}</p>
          </div>
        </div>
      </section>

      <section className="mk-section mk-section--band" aria-labelledby="mk-demos-all">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-demos-all">All demos</h2>
          </header>
          <div className="mk-chips" role="group" aria-label="Filter demos by topic">
            {TAGS.map((t) => (
              <button key={t} type="button" className="mk-chip" aria-pressed={tag === t} onClick={() => setTag(t)}>
                {t}
              </button>
            ))}
          </div>
          <ul className="mk-demo-grid">
            {videos.map((video) => (
              <li key={video.id}>
                <button
                  type="button"
                  className="mk-demo-card"
                  aria-current={video.id === current.id ? "true" : undefined}
                  onClick={() => choose(video)}
                >
                  <span className="mk-demo-poster" aria-hidden="true">
                    <span>{video.tag}</span>
                    <svg viewBox="0 0 24 24" width="18" height="18" fill="currentColor" focusable="false">
                      <path d="M8 5v14l11-7z" />
                    </svg>
                  </span>
                  <strong>{video.title}</strong>
                  <span>{video.blurb}</span>
                </button>
              </li>
            ))}
          </ul>
          <p className="mk-demos-foot">
            <button type="button" className="mk-text-btn" onClick={() => onNavigate("help")}>
              Prefer reading? Open the docs.
            </button>
          </p>
        </div>
      </section>
    </div>
  );
}
