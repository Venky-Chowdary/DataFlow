/** Datawrap YouTube demos (@DatawrapCompany) — one source of truth for the
 * marketing video player. IDs are the YouTube watch ids; thumbnails and the
 * privacy-first embed derive from them. */

export interface MarketingVideo {
  id: string;
  /** Our own neutral title, shown on the poster; the video itself is unchanged. */
  title: string;
  blurb: string;
  tag: "Overview" | "Sync & CDC" | "Verification" | "Sources & files" | "Transforms & AI";
}

export const YOUTUBE_CHANNEL_URL = "https://www.youtube.com/@DatawrapCompany";

export const MARKETING_VIDEOS: MarketingVideo[] = [
  {
    id: "tSEPnT9WG8k",
    title: "The full proof loop on one run",
    blurb:
      "The full proof loop on one run: every row is accounted for, replay lands the held rows, and the checksums match.",
    tag: "Verification",
  },
  {
    id: "SQnksXWvnvo",
    title: "How Datawrap is different",
    blurb: "The one-minute pitch: every transfer gated, verified, and proven — not just moved.",
    tag: "Overview",
  },
  {
    id: "YvYytRBhTlQ",
    title: "The governed transfer flow, step by step",
    blurb: "Connect, map, preflight, quarantine, verify — the whole governed path end to end.",
    tag: "Overview",
  },
  {
    id: "6F-EUA8CoQI",
    title: "Change data capture from the database log",
    blurb: "Native CDC catching inserts, updates and deletes as they happen.",
    tag: "Sync & CDC",
  },
  {
    id: "Sk7x-f8uvm8",
    title: "Mirror, SCD Type 2, and CDC",
    blurb: "Mirror tables, slowly-changing dimensions, and change data capture working together.",
    tag: "Sync & CDC",
  },
  {
    id: "AgtLA84ZnlQ",
    title: "Schedules that run themselves",
    blurb: "Recurring transfers on a schedule — every run still gated and verified.",
    tag: "Sync & CDC",
  },
  {
    id: "mPds1Oajvj8",
    title: "Schema drift, handled",
    blurb: "When the source shape changes mid-pipeline, drift is flagged for review — never silent.",
    tag: "Sync & CDC",
  },
  {
    id: "gg620aIK_yo",
    title: "Sync modes: append, overwrite, and incremental upsert",
    blurb: "The three sync modes and when to pick each one.",
    tag: "Sync & CDC",
  },
  {
    id: "vYSBd2_JvII",
    title: "Quarantine, replay, and a checksum match",
    blurb: "Every row is accounted for, then replayed and proven with a checksum.",
    tag: "Verification",
  },
  {
    id: "uHQAeF9ipvk",
    title: "Transform data inside the transfer",
    blurb: "Mask PII and cast columns in-flight — no separate transform pipeline needed.",
    tag: "Transforms & AI",
  },
  {
    id: "aydJL3_FSsU",
    title: "Letting an agent run a transfer: stage, then confirm",
    blurb: "An AI agent drives a transfer through MCP — staged, reviewed, confirmed before anything moves.",
    tag: "Transforms & AI",
  },
  {
    id: "9ofHWXFWDDs",
    title: "Read-only SQL, post-load transforms, and agent access",
    blurb: "Query connectors read-only, transform after load, and hand agents governed access.",
    tag: "Transforms & AI",
  },
  {
    id: "vKjyznMvzyM",
    title: "Files in, files out",
    blurb: "File sources and destinations get the same gates as databases.",
    tag: "Sources & files",
  },
  {
    id: "-CpO9uVjfE8",
    title: "A stored procedure as the source",
    blurb: "A stored procedure as a transfer source — parameters in, verified rows out.",
    tag: "Sources & files",
  },
  {
    id: "urI8-RY2kJQ",
    title: "Writing through a staging table",
    blurb: "Stage-first writes for destinations that need them — same verification either way.",
    tag: "Sources & files",
  },
  {
    id: "l0jqCETuwRs",
    title: "MySQL to Postgres: the checklist before you move",
    blurb: "What to check before a MySQL→Postgres migration — types, keys, encoding, volume.",
    tag: "Overview",
  },
];

/** The flagship demo shown first on the demos page and the home band. */
export const FEATURED_VIDEO_ID = "tSEPnT9WG8k";
