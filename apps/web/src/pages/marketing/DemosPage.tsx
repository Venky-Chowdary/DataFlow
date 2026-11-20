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

/** A small coded mark per topic, drawn with the current colour. */
function TopicGlyph({ tag }: { tag: MarketingVideo["tag"] }) {
  const paths: Record<MarketingVideo["tag"], string> = {
    Overview: "M3 8h10M3 4h10M3 12h6",
    "Sync & CDC": "M3 5h8l-2-2M13 11H5l2 2",
    Verification: "M3 8.5l3 3 7-7",
    "Sources & files": "M4 2h5l3 3v9H4zM9 2v3h3",
    "Transforms & AI": "M3 4h4M9 4h4M3 12h7M12 12h1M7 2v4M10 10v4",
  };
  return (
    <svg className="mk-topic-glyph" viewBox="0 0 16 16" width="16" height="16" aria-hidden focusable="false">
      <path d={paths[tag]} fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

/** Three bars whose lengths come from the video id: every poster differs. */
function posterBars(id: string): number[] {
  const n = [...id].reduce((acc, ch) => (acc * 31 + ch.charCodeAt(0)) >>> 0, 7);
  return [0, 1, 2].map((i) => 35 + (((n >>> (i * 8)) & 0xff) % 60));
}

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
          <VideoPlayer
            key={current.id}
            videoId={current.id}
            title={current.title}
            tag={current.tag}
            glyph={<TopicGlyph tag={current.tag} />}
            showTitle={false}
          />
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
                  <span className="mk-demo-poster">
                    <span className="mk-demo-poster-tag">
                      <TopicGlyph tag={video.tag} />
                      {video.tag}
                    </span>
                    <strong>{video.title}</strong>
                    <span className="mk-demo-poster-bars" aria-hidden="true">
                      {posterBars(video.id).map((w, i) => (
                        <i key={i} style={{ width: `${w}%` }} />
                      ))}
                    </span>
                    <svg className="mk-demo-poster-play" viewBox="0 0 24 24" width="18" height="18" fill="currentColor" aria-hidden="true" focusable="false">
                      <path d="M8 5v14l11-7z" />
                    </svg>
                  </span>
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
