import assert from "node:assert/strict";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, it } from "node:test";
import { DemosPage } from "./DemosPage.js";
import { VideoPlayer, videoEmbedUrl, videoThumb } from "../../components/marketing/VideoPlayer.js";
import { FEATURED_VIDEO_ID, MARKETING_VIDEOS, YOUTUBE_CHANNEL_URL } from "../../lib/videoCatalog.js";

describe("videoCatalog", () => {
  it("every video has a YouTube-shaped id, title, blurb and known tag", () => {
    for (const v of MARKETING_VIDEOS) {
      assert.match(v.id, /^[A-Za-z0-9_-]{11}$/);
      assert.ok(v.title.length > 10);
      assert.ok(v.blurb.length > 20);
      assert.ok(["Overview", "Sync & CDC", "Verification", "Sources & files", "Transforms & AI"].includes(v.tag));
    }
    assert.ok(MARKETING_VIDEOS.some((v) => v.id === FEATURED_VIDEO_ID));
  });
});

describe("VideoPlayer", () => {
  it("renders a click-to-play facade, not a tracker iframe", () => {
    const markup = renderToStaticMarkup(
      createElement(VideoPlayer, { videoId: "abc123def45", title: "A demo" }),
    );
    assert.match(markup, /lp-video-facade/);
    assert.doesNotMatch(markup, /iframe/);
    assert.doesNotMatch(markup, /youtube\.com\/embed/);
  });

  it("embeds via youtube-nocookie (privacy-first) when playing", () => {
    const markup = renderToStaticMarkup(
      createElement(VideoPlayer, { videoId: "abc123def45", title: "A demo", poster: false }),
    );
    assert.match(markup, /youtube-nocookie\.com\/embed\/abc123def45/);
    assert.doesNotMatch(markup, /(?<!nocookie\.)youtube\.com\/embed/);
  });

  it("builds correct urls", () => {
    assert.equal(videoThumb("x11", "hq"), "https://i.ytimg.com/vi/x11/hqdefault.jpg");
    assert.match(videoEmbedUrl("x11"), /youtube-nocookie\.com\/embed\/x11\?autoplay=1&rel=0/);
  });
});

describe("DemosPage", () => {
  it("renders the featured player, filters and every video card", () => {
    const markup = renderToStaticMarkup(createElement(DemosPage, { onNavigate: () => {} }));
    assert.match(markup, /Video demos/);
    assert.match(markup, /lp-video-facade/);
    for (const tag of ["Overview", "Sync &amp; CDC", "Verification", "Sources &amp; files", "Transforms &amp; AI"]) {
      assert.ok(markup.includes(tag), `missing filter ${tag}`);
    }
    assert.match(markup, /Subscribe on YouTube/);
  });

  it("points at the real channel, not a placeholder", () => {
    assert.equal(YOUTUBE_CHANNEL_URL, "https://www.youtube.com/@DatawrapCompany");
  });
});
