/** Datawrap YouTube demos (@DatawrapCompany) — one source of truth for the
 * marketing video player. IDs are the YouTube watch ids; thumbnails and the
 * privacy-first embed derive from them. */

export interface MarketingVideo {
  id: string;
  title: string;
  blurb: string;
  tag: "Overview" | "Sync & CDC" | "Verification" | "Sources & files" | "Transforms & AI";
}

export const YOUTUBE_CHANNEL_URL = "https://www.youtube.com/@DatawrapCompany";

export const MARKETING_VIDEOS: MarketingVideo[] = [
  {
    id: "tSEPnT9WG8k",
    title: "Datawrap Live Demo: 100,000 Rows, 5 Bad Values Quarantined, Replayed and Proven",
    blurb:
      "The full proof loop on one run: quarantine isolates the bad rows, replay lands them, and the checksums match.",
    tag: "Verification",
  },
  {
    id: "SQnksXWvnvo",
    title: "How Datawrap Is Different, in 60 Seconds",
    blurb: "The one-minute pitch: every transfer gated, verified, and proven — not just moved.",
    tag: "Overview",
  },
  {
    id: "YvYytRBhTlQ",
    title: "Datawrap's governed transfer flow in 9 steps",
    blurb: "Connect, map, preflight, quarantine, verify — the whole governed path end to end.",
    tag: "Overview",
  },
  {
    id: "6F-EUA8CoQI",
    title: "Datawrap Real Change Data Capture",
    blurb: "Native CDC catching inserts, updates and deletes as they happen.",
    tag: "Sync & CDC",
  },
  {
    id: "Sk7x-f8uvm8",
    title: "Datawrap Mirror, SCD Type 2 and CDC",
    blurb: "Mirror tables, slowly-changing dimensions, and change data capture working together.",
    tag: "Sync & CDC",
  },
  {
    id: "AgtLA84ZnlQ",
    title: "Datawrap Schedules: Syncs That Run Themselves",
    blurb: "Recurring transfers on a schedule — every run still gated and verified.",
    tag: "Sync & CDC",
  },
  {
    id: "mPds1Oajvj8",
    title: "Datawrap Schema Drift, Handled",
    blurb: "When the source shape changes mid-pipeline, drift is flagged for review — never silent.",
    tag: "Sync & CDC",
  },
  {
    id: "gg620aIK_yo",
    title: "Datawrap Sync Modes: Full Append, Full Overwrite and Incremental Upsert",
    blurb: "The three sync modes and when to pick each one.",
    tag: "Sync & CDC",
  },
  {
    id: "vYSBd2_JvII",
    title: "Bad rows: quarantine, replay and checksum match",
    blurb: "Bad values never reach the destination — they're quarantined, fixed, replayed, proven.",
    tag: "Verification",
  },
  {
    id: "uHQAeF9ipvk",
    title: "Datawrap: Transform Data Inside the Transfer (Mask PII, Cast Columns)",
    blurb: "Mask PII and cast columns in-flight — no separate transform pipeline needed.",
    tag: "Transforms & AI",
  },
  {
    id: "aydJL3_FSsU",
    title: "Letting an AI agent run a data transfer safely: 58 MCP tools, stage then confirm",
    blurb: "An AI agent drives a transfer through MCP — staged, reviewed, confirmed before anything moves.",
    tag: "Transforms & AI",
  },
  {
    id: "9ofHWXFWDDs",
    title: "Datawrap Demo: Read-Only SQL, Post-Load Transforms, and Agent Access over MCP",
    blurb: "Query connectors read-only, transform after load, and hand agents governed access.",
    tag: "Transforms & AI",
  },
  {
    id: "vKjyznMvzyM",
    title: "Datawrap: Files In, Files Out (CSV, Excel and JSON Lines)",
    blurb: "File sources and destinations get the same gates as databases.",
    tag: "Sources & files",
  },
  {
    id: "-CpO9uVjfE8",
    title: "Datawrap Part 13: Stored-Procedure Source",
    blurb: "A stored procedure as a transfer source — parameters in, verified rows out.",
    tag: "Sources & files",
  },
  {
    id: "urI8-RY2kJQ",
    title: "Datawrap Part 12: Write via Staging",
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
