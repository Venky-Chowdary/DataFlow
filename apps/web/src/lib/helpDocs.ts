export type HelpDocId =
  | "help-getting-started"
  | "help-installation"
  | "help-transfer-studio"
  | "help-preflight-gates"
  | "help-semantic-mapping"
  | "help-connectors"
  | "help-pipelines"
  | "help-contracts"
  | "help-transforms"
  | "help-data-pilot"
  | "help-mcp"
  | "help-query"
  | "help-job-theater"
  | "help-enterprise"
  | "help-api"
  | "help-faq";

export interface HelpDocFigure {
  src: string;
  alt: string;
  caption: string;
  /** Pin markers over the screenshot (percent of frame). */
  markers?: HelpDocMarker[];
}

export interface HelpDocMarker {
  n: number | string;
  label: string;
  /** Horizontal position as CSS percent, e.g. "82%" */
  x: string;
  /** Vertical position as CSS percent, e.g. "28%" */
  y: string;
}

/** Confluence-style procedure step with optional annotated workspace shot. */
export interface HelpDocWorkflowStep {
  title: string;
  body: string;
  /** UI control / location callout shown under the title */
  pin?: string;
  figure?: HelpDocFigure;
  tip?: string;
}

export interface HelpDocSection {
  id: string;
  title: string;
  body: string;
  steps?: string[];
  /** Rich pin-to-pin procedure with real workspace images */
  workflow?: HelpDocWorkflowStep[];
  code?: string;
  tip?: string;
  figure?: HelpDocFigure;
  /** When true, section is rendered as a numbered procedure block */
  procedure?: boolean;
}

export interface HelpDocArticle {
  id: HelpDocId;
  slug: string;
  category: string;
  title: string;
  description: string;
  readTime: string;
  icon: string;
  sections: HelpDocSection[];
}

export interface HelpDocCategory {
  id: string;
  title: string;
  description: string;
  docs: HelpDocId[];
}

export const HELP_DOC_IDS: HelpDocId[] = [
  "help-getting-started",
  "help-installation",
  "help-transfer-studio",
  "help-preflight-gates",
  "help-semantic-mapping",
  "help-connectors",
  "help-pipelines",
  "help-contracts",
  "help-transforms",
  "help-data-pilot",
  "help-mcp",
  "help-query",
  "help-job-theater",
  "help-enterprise",
  "help-api",
  "help-faq",
];

export const HELP_DOC_CATEGORIES: HelpDocCategory[] = [
  {
    id: "start",
    title: "Getting started",
    description: "Sign in, learn the workspace, and run your first governed transfer.",
    docs: ["help-getting-started", "help-installation"],
  },
  {
    id: "transfer",
    title: "Transfer Studio",
    description: "Map schemas, run preflight, write with proof.",
    docs: ["help-transfer-studio", "help-preflight-gates", "help-semantic-mapping"],
  },
  {
    id: "connect",
    title: "Connectors & pipelines",
    description: "Drivers, contracts, schedules, and incremental sync.",
    docs: ["help-connectors", "help-pipelines", "help-contracts"],
  },
  {
    id: "ai",
    title: "Pilot, MCP & Query",
    description: "Agent-native and ad-hoc SQL surfaces.",
    docs: ["help-data-pilot", "help-mcp", "help-query"],
  },
  {
    id: "ops",
    title: "Operations",
    description: "Job Theater, transforms, quarantine, and reconciliation.",
    docs: ["help-job-theater", "help-transforms"],
  },
  {
    id: "enterprise",
    title: "Enterprise & API",
    description: "SSO, tenants, security, and REST reference.",
    docs: ["help-enterprise", "help-api", "help-faq"],
  },
];

const ARTICLES: Record<HelpDocId, HelpDocArticle> = {
  "help-getting-started": {
    id: "help-getting-started",
    slug: "getting-started",
    category: "Getting started",
    title: "How Datawrap works",
    description: "Enterprise operator guide: workspace surfaces, the governed transfer path, and your first proven load.",
    readTime: "8 min",
    icon: "book",
    sections: [
      {
        id: "overview",
        title: "What is Datawrap?",
        body: "Datawrap is a universal data transfer platform. Transfer Studio maps schemas across systems, runs nine preflight gates before any production write, and proves every load with checksum reconciliation. Datawrap Pilot and MCP bring the same governed engine to chat and agent workflows.",
      },
      {
        id: "surfaces",
        title: "Product surfaces (sidebar map)",
        body: "The signed-in workspace sidebar matches these guides. Start in Transfer Studio, then expand to Pipelines, Contracts, Pilot, and MCP.",
        steps: [
          "Platform — Overview, Transfer Studio, Connectors, Contracts",
          "Operations — Jobs (Job Theater), Pipelines, Transforms, Query, Pilot",
          "System — Settings (SSO/Team), MCP, Help, Proofs",
          "Governed path — Source → Destination → Map → Validate → Run → Jobs proof",
        ],
        figure: {
          src: "/docs/screenshots/app-overview.png",
          alt: "Workspace Overview with full sidebar navigation",
          caption: "Overview — full Platform / Operations / System sidebar used across every guide.",
        },
      },
      {
        id: "first-transfer",
        title: "Your first transfer (step by step)",
        procedure: true,
        body: "Follow this exact path inside the signed-in workspace. Every screenshot is from the live application — not a marketing mock.",
        workflow: [
          {
            title: "Confirm the workspace is healthy",
            pin: "Sidebar → Overview",
            body: "Open **Overview** and confirm connection health, recent jobs, and throughput for your tenant. If metrics fail to load, fix connectivity with your Owner before creating connectors.\n\nYou should see live **rows moved**, success rate, and connection status cards before you continue.",
            figure: {
              src: "/docs/screenshots/app-overview.png",
              alt: "Workspace Overview with live metrics and connection health",
              caption: "Overview — live rows moved, success rate, and connection health.",
              markers: [
                { n: 1, label: "Overview", x: "8%", y: "24%" },
                { n: 2, label: "Health metrics", x: "48%", y: "35%" },
              ],
            },
          },
          {
            title: "Add or test a connector",
            pin: "Platform → Connectors → New connection",
            body: "Open **Platform → Connectors**. Click **New connection** to create a source or destination, or click **Test** on an existing row until you see **Test passed**.\n\nFor a quick offline walkthrough you can skip connectors and upload a CSV on the Transfer Studio **Source** step instead.",
            figure: {
              src: "/docs/screenshots/app-connectors.png",
              alt: "Connectors page with Test passed rows",
              caption: "Marker 1 = New connection · Marker 2 = Test passed",
              markers: [
                { n: 1, label: "New connection", x: "90%", y: "28%" },
                { n: 2, label: "Test passed", x: "58%", y: "42%" },
              ],
            },
          },
          {
            title: "Run Transfer Studio Source → Destination → Map → Validate → Run",
            pin: "Platform → Transfer  ·  or New transfer",
            body: "Open **Platform → Transfer** (or **New transfer**). On **Source**, upload a sample file such as `sample-orders.csv` or pick a **Database** connector. Review **Detected structure**, then continue through **Destination**, **Map**, and **Validate**.\n\nFix every blocked gate before **Run**. Execute stays locked until Validate passes.",
            figure: {
              src: "/docs/screenshots/app-transfer-source.png",
              alt: "Transfer Studio Source with profiled CSV",
              caption: "Marker 1 = Source step · Marker 2 = Continue to Destination",
              markers: [
                { n: 1, label: "Source", x: "22%", y: "16%" },
                { n: 2, label: "Continue", x: "88%", y: "92%" },
              ],
            },
          },
          {
            title: "Prove the load in Job Theater",
            pin: "Operations → Jobs",
            body: "Open **Operations → Jobs** and select the run. Confirm **Quarantine** is empty (or review each rejected value and reason). Wait for checksum **MATCH** and row fidelity on the reconcile stage — that is the archiveable proof of the load.",
            figure: {
              src: "/docs/screenshots/app-jobs.png",
              alt: "Job Theater with reconcile proof",
              caption: "Marker 1 = selected run · Marker 2 = reconcile proof",
              markers: [
                { n: 1, label: "Job", x: "28%", y: "40%" },
                { n: 2, label: "MATCH", x: "70%", y: "55%" },
              ],
            },
          },
        ],
      },
      {
        id: "workspace",
        title: "Workspace concepts",
        body: "Every transfer runs inside a workspace with connectors, saved routes, synonym dictionaries, and audit history. Team members inherit RBAC from your IdP in enterprise tenants.",
        steps: [
          "Connectors — encrypted credentials scoped to workspace",
          "Saved routes — reuse mapping + write mode presets",
          "Synonym dictionary — accepted semantic pairs for auto-map",
          "Job Theater — unified run history with quarantine and proof",
        ],
      },
    ],
  },
  "help-installation": {
    id: "help-installation",
    slug: "installation",
    category: "Getting started",
    title: "Access your enterprise workspace",
    description: "How your team signs in, joins a tenant, and gets ready to run governed transfers — no local install required.",
    readTime: "10 min",
    icon: "server",
    sections: [
      {
        id: "what-you-get",
        title: "What your organization receives",
        body: "Datawrap is delivered as a **hosted enterprise workspace** at your tenant URL (for example `your-company.datawrap.io`). After sign-in you work entirely in the browser — there is nothing to install on operator laptops.\n\nThe signed-in product includes **Overview**, **Connectors**, **Transfer Studio**, **Pipelines**, **Jobs** (Job Theater), **Query**, **Pilot**, and **Settings**. Your IT team configures SSO and optional BYOK once; day-to-day operators create connections, run governed transfers, and prove loads.",
        figure: {
          src: "/docs/screenshots/app-overview.png",
          alt: "Enterprise workspace Overview after sign-in",
          caption: "After SSO — Overview shows workspace health, throughput, and recent jobs for your tenant.",
          markers: [
            { n: 1, label: "Overview", x: "8%", y: "24%" },
            { n: 2, label: "Workspace health", x: "48%", y: "35%" },
          ],
        },
      },
      {
        id: "access",
        title: "Procedure: get access",
        procedure: true,
        body: "Use this path when onboarding a new operator or confirming a newly provisioned tenant. Complete every step before you create production connectors.",
        workflow: [
          {
            title: "Receive your tenant URL and role",
            pin: "IT / Datawrap admin → invite email",
            body: "A workspace **Owner** (or Datawrap provisioning) invites you by email with one of three roles:\n\n**Viewer** — inspect Jobs, quarantine, and MATCH proof; cannot create connectors or run transfers.\n\n**Editor** — create connectors, run **Transfer Studio** and **Pipelines**, and open **Job Theater**.\n\n**Owner** — everything Editors can do, plus **Settings → SSO**, **Enterprise**, **BYOK**, **Team**, and API keys.\n\nOpen the invite link, or go directly to your tenant URL (for example `https://your-company.datawrap.io`). Sign in with your company identity provider — **Okta**, **Azure AD / Entra ID**, **Google Workspace**, or another SAML 2.0 / OIDC IdP configured for the tenant.",
          },
          {
            title: "Sign in with SSO",
            pin: "Login → Continue with SSO",
            body: "On the login screen choose **Continue with SSO** (or your company's branded SSO button). Authenticate with your work account. Datawrap issues a workspace session that inherits your RBAC role from the invite or from IdP group claims.\n\nA successful sign-in lands on **Overview**. Confirm the top bar shows your tenant name and that the left sidebar lists **Platform** surfaces (Connectors, Transfer, Pipelines) and **Operations → Jobs**. If you only see a blank or permission error, stop and ask an Owner to verify your role under **Settings → Team**.",
            figure: {
              src: "/docs/screenshots/app-overview.png",
              alt: "Workspace after successful enterprise sign-in",
              caption: "Successful SSO lands on Overview — confirm tenant health before creating connectors.",
              markers: [
                { n: 1, label: "Signed-in workspace", x: "50%", y: "40%" },
              ],
            },
          },
          {
            title: "Confirm you can reach Connectors and Transfer",
            pin: "Sidebar → Platform → Connectors  ·  Transfer",
            body: "In the left sidebar open **Platform → Connectors**. You should see health cards and either an empty Connections table or existing rows. Then open **Platform → Transfer** (Transfer Studio) or click **New transfer** if shown.\n\nYou are ready for day-one work when:\n\n**1.** **New connection** is visible on Connectors (Editors and Owners).\n\n**2.** Transfer Studio opens the **Source → Destination → Map → Validate → Run** wizard.\n\n**3.** **Operations → Jobs** opens Job Theater for past and live runs.\n\nIf Connectors or Transfer is missing from the sidebar, your role is likely **Viewer** — ask an Owner to raise it to **Editor** under **Settings → Team**.",
            figure: {
              src: "/docs/screenshots/app-connectors.png",
              alt: "Connectors available in the enterprise sidebar",
              caption: "Marker 1 = Connectors · Marker 2 = New connection (Editor and Owner).",
              markers: [
                { n: 1, label: "Connectors", x: "8%", y: "32%" },
                { n: 2, label: "New connection", x: "90%", y: "28%" },
              ],
            },
            tip: "**Viewer** — inspect Jobs and proof only. **Editor** — run transfers and manage connectors. **Owner** — SSO, BYOK, Team invites, and enterprise controls under **Settings**.",
          },
        ],
      },
      {
        id: "admin-setup",
        title: "Admin checklist (Owners)",
        body: "Tenant **Owners** complete these once per workspace. Operators do not need this checklist to run day-to-day transfers — it is for IT and security readiness.",
        steps: [
          "**Settings → SSO** — choose SAML or OIDC, paste IdP metadata URL (or upload XML), map roles if your IdP sends group claims, then click **Test login** and **Save**.",
          "**Settings → Enterprise** — set tenant display name, data **region**, MFA policy, session timeout, and optional IP allowlist.",
          "**Settings → Enterprise → BYOK** — upload your customer KMS key when procurement requires customer-managed encryption for connector secrets.",
          "**Settings → Team** — invite Editors and Viewers with least privilege; re-check IdP group → role mapping after SSO enforce.",
          "**Settings → Security** — download the **Security posture report** for SOC 2 / GDPR / HIPAA questionnaires.",
        ],
        tip: "If your tenant URL is not live yet, email **sales@datawrap.io**. Provisioning is handled by Datawrap — operators never install or run a local API.",
      },
      {
        id: "browser",
        title: "Supported browsers & network",
        body: "Use a current **Chromium**, **Firefox**, or **Safari** build. Connector **Test** and transfers need outbound network access from the Datawrap worker (in your pinned region) to your sources and destinations — databases, warehouses, and object stores.\n\nCorporate proxies and firewalls must allow those destinations for the workspace region selected under **Settings → Enterprise**. If **Test** fails with a timeout while credentials are correct, open a ticket with network/security before re-running Validate gates in Transfer Studio.",
      },
    ],
  },

  "help-transfer-studio": {
    id: "help-transfer-studio",
    slug: "transfer-studio",
    category: "Transfer Studio",
    title: "Transfer Studio guide",
    description: "Complete 5-step example: sample-orders.csv → File Export CSV through Source, Destination, Map, Validate, and Run — with real workspace screenshots.",
    readTime: "18 min",
    icon: "transfer",
    sections: [
      {
        id: "example",
        title: "Example transfer used in this guide",
        body: "Every screenshot below is from a real Transfer Studio session using the built-in demo file.\n\n**Source:** `sample-orders.csv` (5 rows · 5 columns: `order_id`, `customer_email`, `order_amt`, `order_date`, `status`)\n\n**Destination:** **File Export** → **CSV** → `exports/sample-orders.csv`\n\n**Path:** **Source → Destination → Map → Validate → Run** — the same rail Pipelines, Pilot, and MCP reuse. There is no silent shortcut around gates or checksum proof.\n\nFor production warehouse loads, swap Destination to **Database / Warehouse** and pick a saved connector (for example PostgreSQL `public.orders`). Map, Validate, and Run stay identical.",
      },
      {
        id: "procedure",
        title: "Procedure: run the sample-orders transfer",
        procedure: true,
        body: "Follow all five steps in order. Markers on each screenshot match the live product UI.",
        workflow: [
          {
            title: "Step 1 — Source: load sample-orders.csv",
            pin: "Platform → Transfer  ·  or New transfer  →  Load sample orders CSV",
            body: "Open **Transfer Studio** from the sidebar (**Transfer**) or click **New transfer**. The wizard lands on **Source** with the five-step rail: **Src → Dest → Map → Validate → Run**.\n\nUnder **Where is your data?**, keep **File** selected. Click **Load sample orders CSV** (or upload your own CSV). Wait until the file chip shows **Sample-Orders.csv** with row/column counts and the right panel shows **Detected structure** with typed columns (`INTEGER`, `VARCHAR`, `DECIMAL`, `DATE`).\n\nClick **Continue to Destination →** only after Detected structure is populated — that profile feeds Map and Validate.",
            figure: {
              src: "/docs/screenshots/app-transfer-source.png",
              alt: "Transfer Studio Source with sample-orders.csv loaded and Detected structure",
              caption: "Source — File selected, Sample-Orders.csv profiled, Continue to Destination ready.",
              markers: [
                { n: 1, label: "Src step", x: "18%", y: "14%" },
                { n: 2, label: "File source", x: "22%", y: "38%" },
                { n: 3, label: "Sample-Orders.csv", x: "38%", y: "62%" },
                { n: 4, label: "Detected structure", x: "82%", y: "40%" },
              ],
            },
            tip: "Database and Cloud storage sources work the same way — pick a saved connector, choose table/object, wait for Detected structure, then continue.",
          },
          {
            title: "Step 2 — Destination: File Export CSV",
            pin: "Destination Mode → File Export  ·  format CSV  ·  path exports/sample-orders.csv",
            body: "On **Destination**, choose **Destination Mode**:\n\n**Database / Warehouse** — pick a saved connector (or **Custom connection**) and set schema/table.\n\n**File Export** — used in this example. Select format **CSV** (JSON, Parquet, Excel, and more are available). Optionally set **Output path** to `exports/sample-orders.csv`.\n\nConfirm **Sync defaults** (this example uses **Full append** · Manual approval · Strict validation). Open **Advanced** for overwrite, CDC, SCD2, mirror, cursors, and drift policy.\n\nWhen the route shows ready, click **Continue to Map**.",
            figure: {
              src: "/docs/screenshots/app-transfer-destination.png",
              alt: "Transfer Studio Destination with File Export CSV and output path",
              caption: "Destination — File Export selected, CSV format, path exports/sample-orders.csv.",
              markers: [
                { n: 1, label: "Dest step", x: "32%", y: "14%" },
                { n: 2, label: "File Export", x: "42%", y: "32%" },
                { n: 3, label: "CSV format", x: "22%", y: "48%" },
                { n: 4, label: "Output path", x: "78%", y: "42%" },
              ],
            },
            tip: "Enterprise warehouse path: **Database / Warehouse** → PostgreSQL (or Snowflake / BigQuery) → table such as `public.orders`. The next three steps do not change.",
          },
          {
            title: "Step 3 — Map: align columns and Accept risk",
            pin: "Map columns → review edges → Accept risk / Approve → Continue to Validate",
            body: "Map opens **Map columns** with every source field paired to a destination name and type. For `sample-orders.csv` you should see five edges — for example `order_id` → `order_id`, `order_amt` → `order_amount`, with transforms such as **Cast integer**, **Normalize email**, **Cast decimal**, and **Date → ISO**.\n\nUse the filter tabs (**All**, **Review**, **Critical**, **PII**, **Ready**) to focus issues. False-friends stay named: **qty≠amt**, **user≠customer**, **dest collision**. **Approve eligible** will not clear those — Remap dest, or Confirm this pair on the row. When a type cast is lossy (for example DECIMAL → TEXT on a file export), click **Accept risk** on that row.\n\n**Continue to Validate →** stays locked until every blocking map issue is approved or remapped. Then continue.",
            figure: {
              src: "/docs/screenshots/app-transfer-map.png",
              alt: "Transfer Studio Map columns with sample-orders field mappings",
              caption: "Map — five sample-orders edges with types, transforms, and Accept risk actions.",
              markers: [
                { n: 1, label: "Map step", x: "48%", y: "14%" },
                { n: 2, label: "Column filters", x: "35%", y: "28%" },
                { n: 3, label: "order_id edge", x: "40%", y: "42%" },
                { n: 4, label: "Accept risk", x: "88%", y: "48%" },
              ],
            },
            tip: "Open **Proof** on Map anytime to see confidence and fidelity evidence before Validate.",
          },
          {
            title: "Step 4 — Validate: clear every blocking gate",
            pin: "Validate dashboard → fix suggested remediations → Re-run until Execute unlocks",
            body: "Validate runs the full gate set (source readable, destination write access, schema contract, mappings, sample dry-run, data integrity, DDL, capacity, reconcile simulation, plus sync/policy gates).\n\nIf a gate blocks — as in this example when `order_amt` (DECIMAL) collapses to TEXT — the dashboard shows **Validation blocked**, which checks failed, and **Suggested fixes** (for example **Remap order_amt → VARCHAR**). Apply the fix, click **Re-run**, and wait until the API decision is **approve**.\n\n**Execute** stays locked while any gate is blocked. That is intentional — do not skip Validate.",
            figure: {
              src: "/docs/screenshots/app-transfer-validate.png",
              alt: "Transfer Studio Validate dashboard with pending gates and Run preflight",
              caption: "Validate — gate dashboard before write; click Run preflight, then clear any blocked checks before Execute unlocks.",
              markers: [
                { n: 1, label: "Validate step", x: "58%", y: "14%" },
                { n: 2, label: "Pending summary", x: "28%", y: "30%" },
                { n: 3, label: "Validation rules", x: "45%", y: "62%" },
                { n: 4, label: "Run preflight", x: "88%", y: "92%" },
              ],
            },
            tip: "Use **Explain & fix with AI** on Validate for plain-language why/fix, then **Re-run**. See the Preflight gates article for every gate ID.",
          },
          {
            title: "Step 5 — Run: Execute Transfer and prove in Job Theater",
            pin: "Run → Execute Transfer  ·  then Operations → Jobs",
            body: "When Validate approves, the **Run** step shows readiness (**Preflight approved**) and unlocks **Execute Transfer**.\n\nIf you open Run while Validate is still incomplete, you see **Confirm Validate before write** and Execute stays locked — return to Validate, clear blockers, then come back.\n\nClick **Execute Transfer**. The engine submits the governed job, locks the approved mapping, and opens live telemetry. Then open **Operations → Jobs** (Job Theater): confirm quarantine is empty (or review reasons) and wait for checksum **MATCH** / row fidelity. That MATCH artifact is the archiveable proof of the load.",
            figure: {
              src: "/docs/screenshots/app-transfer-run.png",
              alt: "Transfer Studio Run step requiring Validate approval before Execute",
              caption: "Run — Execute stays locked until Validate approves; then Execute Transfer launches the job.",
              markers: [
                { n: 1, label: "Completed rail", x: "40%", y: "14%" },
                { n: 2, label: "sample-orders route", x: "28%", y: "28%" },
                { n: 3, label: "Preflight incomplete", x: "82%", y: "28%" },
                { n: 4, label: "Confirm Validate", x: "50%", y: "55%" },
              ],
            },
            tip: "After Execute, proof lives on the job — open **Jobs → select the run → Quarantine / Reconcile**. See Job Theater guide for MATCH details.",
          },
        ],
      },
      {
        id: "write-modes",
        title: "Write modes (Advanced)",
        body: "Open **Advanced** on Destination (or Map footer) to set sync mode. Pipelines reuse the same modes on every schedule tick.",
        steps: [
          "**Full append** — keep existing rows; append the full snapshot (default in the example).",
          "**Full overwrite** — replace the target, then load.",
          "**Incremental append** — cursor-based new rows only.",
          "**Incremental deduped** — cursor + primary-key upserts.",
          "**CDC / SCD Type 2 / Mirror** — advanced identity modes (PK required where noted).",
          "**Destination write → Query** — one INSERT/MERGE/UPDATE with `:binds` mapped to source columns. Failed rows quarantine. Not CDC.",
          "**Destination write → Stored procedure** — one CALL/EXEC per row (Informatica connected SQL). SQLite has no dest procedures; dest query INSERT is allowed.",
        ],
      },
      {
        id: "quarantine",
        title: "After Run: quarantine & MATCH proof",
        body: "Rows that fail coerce or policy checks are isolated with **column**, **value**, and **reason** — never silently dropped. After the write, Datawrap reconciles row counts and content hashes. Only then does Job Theater flash **MATCH**.",
        tip: "Open **Jobs → select the run → Quarantine** before replaying failed rows. Export proof when compliance needs an archive pack.",
        figure: {
          src: "/docs/screenshots/app-jobs.png",
          alt: "Job Theater reconcile timeline with row fidelity",
          caption: "Job Theater — after Execute, reconcile and row fidelity prove the sample-orders load.",
          markers: [
            { n: 1, label: "Selected job", x: "28%", y: "40%" },
            { n: 2, label: "Reconcile", x: "70%", y: "55%" },
          ],
        },
      },
    ],
  },
  "help-preflight-gates": {
    id: "help-preflight-gates",
    slug: "preflight-gates",
    category: "Transfer Studio",
    title: "Preflight gates explained",
    description: "What Validate checks before every production write — and how to fix a blocked gate.",
    readTime: "11 min",
    icon: "gate",
    sections: [
      {
        id: "when",
        title: "When Validate runs",
        body: "On Transfer Studio → Validate, and again on every Pipeline tick, Datawrap runs the same gate engine. Execute stays locked until the API decision is approve. Soft or review-grade passes do not unlock write.",
        tip: "Pipelines cannot skip Validate. A failing gate on a schedule blocks the write the same way as a manual run.",
      },
      {
        id: "core-gates",
        title: "Core gates (before write)",
        body: "These checks run in Transfer Studio Validate. Open any failing card for the rule text and remediation.",
        steps: [
          "G1 Source readable — source connects and rows can be read",
          "G2 Destination write access — destination is reachable with write/create privilege",
          "G3 Schema contract — source and target schemas are compatible",
          "G4 Column mappings — every column maps above the confidence threshold",
          "G5 Sample dry-run — sample rows pass the same transforms writers use",
          "G9 Data integrity — encoding, required nulls, identity duplicates, precision on the sample",
          "G6 Target DDL — required CREATE / ALTER statements are valid",
          "G7 Staging capacity — destination has headroom for the volume",
          "G8 Sample reconciliation — pre-write sample identity and uniqueness hold (post-load checksum runs after Execute)",
        ],
        figure: {
          src: "/docs/screenshots/app-transfer-validate.png",
          alt: "Transfer Studio Validate dashboard with gate cards",
          caption: "Validate — core gate cards (Source readable, Destination write access, Schema contract, …) before any write.",
          markers: [
            { n: 1, label: "Validate step", x: "58%", y: "14%" },
            { n: 2, label: "Gate summary", x: "28%", y: "30%" },
            { n: 3, label: "Validation rules", x: "45%", y: "62%" },
            { n: 4, label: "Run preflight", x: "88%", y: "92%" },
          ],
        },
      },
      {
        id: "policy-gates",
        title: "Policy & drift gates",
        body: "Depending on sync mode and Advanced settings, Validate also enforces sync contract (cursor / primary key), schema change policy, validation posture, and live schema drift against your saved mapping.",
        steps: [
          "Sync contract — cursor and primary key satisfy the chosen sync mode",
          "Schema change policy — detected drift is allowed by policy",
          "Validation posture — overall posture matches Strict / Maximum / Balanced",
          "Schema drift — live schemas still match the saved mapping contract",
        ],
      },
      {
        id: "fix",
        title: "Procedure: fix a blocked gate",
        procedure: true,
        body: "Use this path whenever Validate shows a red or blocking gate.",
        workflow: [
          {
            title: "Open the failing gate card",
            pin: "Transfer Studio → Validate → gate card",
            body: "Click **Run preflight** if gates are still PENDING. Open the failing card and read the rule plus sample evidence. Note whether the issue is mapping, credentials, capacity, encoding, or drift.",
            figure: {
              src: "/docs/screenshots/app-transfer-validate.png",
              alt: "Validate dashboard before or after preflight",
              caption: "Start on Validate — run preflight, then open any blocked gate card.",
            },
          },
          {
            title: "Remediate in Map or Advanced",
            pin: "Map · Destination → Advanced · Fix bad data",
            body: "Approve or rematch columns, **Accept risk** for intentional casts, change sync mode / primary key / cursor, or use **Fix bad data** to strip or quarantine encoding issues — then return to Validate.",
            figure: {
              src: "/docs/screenshots/app-transfer-map.png",
              alt: "Map columns with Accept risk actions",
              caption: "Many gate failures start as mapping / fidelity issues — fix on Map, then re-run Validate.",
            },
          },
          {
            title: "Re-run until Execute unlocks",
            pin: "Validate → Re-run preflight",
            body: "Execute unlocks only when the API returns **approve**. Then continue to **Run**. If you are unsure, ask Pilot to explain the failing gate in plain language.",
            tip: "Do not force Execute from the browser alone — governed unlock comes from the control plane.",
          },
        ],
      },
    ],
  },

  "help-semantic-mapping": {
    id: "help-semantic-mapping",
    slug: "semantic-mapping",
    category: "Transfer Studio",
    title: "Semantic column mapping",
    description: "How Datawrap infers roles, synonyms, and confidence scores.",
    readTime: "11 min",
    icon: "sparkle",
    sections: [
      {
        id: "roles",
        title: "Semantic roles",
        body: "Datawrap detects roles like amount, email, identifier, timestamp, and address — not just string name matching.",
        figure: {
          src: "/docs/screenshots/app-transfer-map.png",
          alt: "Map columns with semantic transforms, confidence, and Accept risk",
          caption: "Map — roles and transforms (Cast, Normalize email, Date → ISO) with confidence and Accept risk.",
          markers: [
            { n: 1, label: "Map columns", x: "28%", y: "22%" },
            { n: 2, label: "Confidence", x: "78%", y: "48%" },
            { n: 3, label: "Accept risk", x: "90%", y: "48%" },
          ],
        },
      },
      {
        id: "synonyms",
        title: "Synonym dictionary",
        body: "Accept or reject mapping suggestions. Accepted pairs enter your workspace synonym dictionary for future auto-maps.",
        figure: {
          src: "/docs/screenshots/app-transfer-source.png",
          alt: "Detected structure feeding the mapper",
          caption: "Source profiling types feed synonym and role inference before Map opens.",
        },
      },
      {
        id: "confidence",
        title: "Confidence scores",
        body: "Each Map edge shows a confidence percentage. Review anything below your threshold — use filter tabs **Review**, **Critical**, **PII**, and **Ready**. Click **Accept risk** for intentional lossy casts, or **Approve** when the edge is clean.",
      },
      {
        id: "drift",
        title: "Schema drift detection",
        body: "Datawrap compares current source and destination schemas against your saved route. Drift warnings appear in Transfer Studio and Datawrap Pilot before the next scheduled run.",
        steps: [
          "New columns in source — review suggested maps",
          "Removed columns — confirm destination still valid",
          "Type changes — preflight type-coercion gate validates casts",
        ],
      },
    ],
  },
  "help-connectors": {
    id: "help-connectors",
    slug: "connectors",
    category: "Connectors",
    title: "Add & manage connectors",
    description: "Step-by-step: open Connectors, create a connection, test it, and use it in Transfer Studio — with real workspace pins.",
    readTime: "10 min",
    icon: "connectors",
    sections: [
      {
        id: "overview",
        title: "What you see on Connectors",
        body: "The **Connectors** page is the inventory of every saved source and destination in your workspace. Top **health cards** summarize **Connections**, **Healthy**, and **Needs attention**. The table below shows **Last test** and **Last used** so operators know what is safe to schedule in Pipelines.\n\nUse the **Connections** tab for saved credentials. Use **Catalog** only when browsing available driver types — Catalog does not store secrets.",
        figure: {
          src: "/docs/screenshots/app-connectors.png",
          alt: "Connectors workspace with health cards and connection table",
          caption: "Live workspace — health cards on top, Connections table with Test passed / Test failed.",
          markers: [
            { n: 1, label: "Health cards", x: "45%", y: "18%" },
            { n: 2, label: "Connections table", x: "50%", y: "55%" },
            { n: 3, label: "New connection", x: "90%", y: "28%" },
          ],
        },
      },
      {
        id: "add-connector",
        title: "Procedure: add a connector",
        procedure: true,
        body: "Follow this path whenever you onboard a new system. Numbered markers on the screenshots match the live product UI.",
        workflow: [
          {
            title: "Open Connectors",
            pin: "Sidebar → Platform → Connectors",
            body: "In the signed-in workspace, open **Platform → Connectors**. Confirm the **Connections** tab is selected when you are managing saved credentials (not **Catalog**).\n\nScan the health cards. If **Needs attention** is greater than zero, open those rows and re-run **Test** before scheduling pipelines against them.",
            figure: {
              src: "/docs/screenshots/app-connectors.png",
              alt: "Sidebar with Connectors active and Connections tab selected",
              caption: "Marker 1 = Connectors nav · Marker 2 = Connections tab",
              markers: [
                { n: 1, label: "Connectors", x: "8%", y: "32%" },
                { n: 2, label: "Connections", x: "36%", y: "28%" },
              ],
            },
          },
          {
            title: "Click New connection — Choose a connector dialog",
            pin: "Top-right → New connection",
            body: "Click **New connection** (top-right). A modal dialog opens: **Choose a connector**.\n\nUse category chips (**Databases**, **Data Warehouses**, **Files & Formats**, …), search, and status filters (**Certified**, **Source only**, **Test only**, **Planned**). Transfer-ready cards show badges such as **Full Transfer**.\n\nPick a driver — for example **PostgreSQL**, **MySQL**, **MongoDB**, **Snowflake**, **BigQuery**, **S3**, **CSV / TSV**, or a SQLAlchemy generic. **Planned** catalog entries must not be scheduled in production Pipelines.",
            figure: {
              src: "/docs/screenshots/app-connectors-wizard.png",
              alt: "Choose a connector modal with catalog grid",
              caption: "Modal — Choose a connector. Filter Certified / Planned, then pick a transfer-ready driver.",
              markers: [
                { n: 1, label: "Choose a connector", x: "50%", y: "12%" },
                { n: 2, label: "Category chips", x: "40%", y: "22%" },
                { n: 3, label: "Connector cards", x: "40%", y: "55%" },
              ],
            },
          },
          {
            title: "Enter credentials and Test",
            pin: "Wizard form → Test → Save",
            body: "After you pick a driver, the credential form opens (still in the dialog flow). Fill **host**, **database**, and auth (password, key, or cloud credentials). Click **Test** before **Save**. Do not skip Test.\n\n**Test passed** (green) on the Connections table means the driver reached the system — safe to pick in Transfer Studio and Pipelines.\n\n**Test failed** (red) means fix credentials, firewall, or region network access before using the connector. Failed connections block Validate gates later.",
            figure: {
              src: "/docs/screenshots/app-connectors.png",
              alt: "Connections table with Test passed / Test failed",
              caption: "After Save — Connections table shows Last test. Re-run Test from the row actions anytime.",
              markers: [
                { n: 1, label: "Connections table", x: "50%", y: "55%" },
                { n: 2, label: "New connection", x: "90%", y: "28%" },
              ],
            },
            tip: "Credentials are encrypted at rest with workspace-scoped keys. Enterprise tenants can enable **BYOK** under **Settings → Enterprise**. Raw passwords never appear in Job Theater logs.",
          },
          {
            title: "Use it in Transfer Studio",
            pin: "Transfer → Source or Destination → Database",
            body: "Open **Platform → Transfer**. On **Source** (or later **Destination**), choose **Database** or **Cloud storage**. Your saved healthy connectors appear in the picker.\n\nContinue through **Map → Validate → Run**. A green **Test passed** on Connectors does **not** skip preflight — Validate still runs the full gate set before any production write.",
            figure: {
              src: "/docs/screenshots/app-transfer-source.png",
              alt: "Transfer Studio Source with Database card among source types",
              caption: "Marker 1 = Database source type · Marker 2 = Continue after profiling",
              markers: [
                { n: 1, label: "Database", x: "45%", y: "38%" },
                { n: 2, label: "Continue", x: "88%", y: "92%" },
              ],
            },
          },
        ],
      },
      {
        id: "labels",
        title: "Honest transfer-ready labels",
        body: "Catalog tiles are not the same as production drivers. Each connector shows one of these readiness states. Only routes with transfer-ready evidence get the production badge — everything else stays labelled honestly.",
        steps: [
          "**Live** — governed transfer path proven; safe for production schedules after Test passed.",
          "**Beta** — works with known limits; review before production Pipelines.",
          "**Planned** — catalog presence only; do not schedule yet.",
          "**Connect-only** — auth works; write path is not transfer-ready.",
        ],
      },
      {
        id: "native",
        title: "Native drivers & SQLAlchemy",
        body: "**PostgreSQL**, **MySQL**, **MongoDB**, **Snowflake**, **BigQuery**, **Redshift**, **S3**, **CSV**, **JSON**, and more ship with upsert and incremental where supported. Generic **SQLAlchemy** URLs extend reach with the same **Validate** preflight path used by native drivers.",
      },
    ],
  },
  "help-pipelines": {
    id: "help-pipelines",
    slug: "pipelines",
    category: "Connectors",
    title: "Create pipelines & recurring sync",
    description: "Exact operator path: Operations → Pipelines → Create recurring sync form → Save → detail drawer (Run now) → Jobs proof.",
    readTime: "16 min",
    icon: "activity",
    sections: [
      {
        id: "overview",
        title: "What a pipeline is",
        body: "A **pipeline** is a scheduled Transfer Studio route. Every cadence tick still runs mapping confidence, **Validate** preflight gates, quarantine, and reconciliation proof — recurring does **not** mean less governance.\n\nCreate happens on an **inline form** titled **Create recurring sync** (not a side drawer). After you save, clicking a pipeline opens the **right-side detail drawer** with Overview / Schema / History / Config and actions like **Run now**, **Pause**, and **Edit**.",
        figure: {
          src: "/docs/screenshots/app-pipelines.png",
          alt: "Pipelines page in the live workspace",
          caption: "Operations → Pipelines — create from empty state or New pipeline in the toolbar.",
          markers: [
            { n: 1, label: "Pipelines nav", x: "8%", y: "48%" },
            { n: 2, label: "Create / list", x: "55%", y: "55%" },
            { n: 3, label: "New pipeline", x: "90%", y: "18%" },
          ],
        },
        tip: "You need at least two saved connectors (source + destination) under **Platform → Connectors** before **Save pipeline** enables.",
      },
      {
        id: "create-pipeline",
        title: "Procedure: create a pipeline",
        procedure: true,
        body: "Follow this exact path in the signed-in workspace. Screenshots are from the live **Create recurring sync** form and pipeline detail drawer — not Jobs.",
        workflow: [
          {
            title: "Open Pipelines",
            pin: "Sidebar → Operations → Pipelines",
            body: "Open **Operations → Pipelines**. The page title is **Pipelines** with the subtitle *Schedule recurring syncs with the same governed transfer engine.*\n\nIf none exist yet, the empty state shows **Create pipeline**. The toolbar always offers **Export YAML**, **Import YAML**, **Require signed**, and **New pipeline**.",
            figure: {
              src: "/docs/screenshots/app-pipelines.png",
              alt: "Pipelines home with create actions",
              caption: "Marker 1 = Pipelines in Operations · Marker 2 = Create / New pipeline CTAs",
              markers: [
                { n: 1, label: "Pipelines", x: "8%", y: "48%" },
                { n: 2, label: "Create pipeline", x: "50%", y: "58%" },
              ],
            },
          },
          {
            title: "Open Create recurring sync",
            pin: "Create pipeline  ·  or New pipeline",
            body: "Click **Create pipeline** or **New pipeline**. The page switches to the inline form **Create recurring sync** (*Schedule source → destination with your saved connectors*). A teal status chip shows **Creating your first pipeline** (or **Creating pipeline**).\n\nFill:\n\n1. **Pipeline name** (example: `Nightly orders sync`)\n2. **Source connector** + **Source table|collection**\n3. **Destination connector** + **Destination table|collection**\n\nPrefer connectors you already **Test passed** on Connectors, and a route you have proven once in Transfer Studio.",
            figure: {
              src: "/docs/screenshots/app-pipelines-create.png",
              alt: "Create recurring sync form with name, connectors, cadence, and sync mode",
              caption: "Inline create form — not a drawer. Name, source/dest, Cadence, Sync mode, Validation, Data contract.",
              markers: [
                { n: 1, label: "Create recurring sync", x: "28%", y: "16%" },
                { n: 2, label: "Connectors + tables", x: "45%", y: "28%" },
                { n: 3, label: "Cadence", x: "40%", y: "48%" },
                { n: 4, label: "Sync mode", x: "40%", y: "72%" },
              ],
            },
            tip: "If Source/Destination show **No connectors available**, stop and add connectors under **Platform → Connectors** first — Save stays locked until both sides are selected.",
          },
          {
            title: "Set Cadence (Preset or Cron)",
            pin: "Cadence → Preset | Cron",
            body: "In the **Cadence** panel, choose **Preset** or **Cron**.\n\n**Preset** tiles: **Hourly**, **Daily**, **Weekly**. Presets are rolling intervals from the last run (or create) — Daily means ~24 hours later, not a fixed wall-clock time.\n\n**Cron** uses a 5-field expression plus **Timezone** (example: `10 10 * * *` daily at 10:10). Next run is computed after save.",
            figure: {
              src: "/docs/screenshots/app-pipelines-cadence.png",
              alt: "Cadence panel with Hourly Daily Weekly presets",
              caption: "Cadence — Preset tiles or Cron + timezone for wall-clock schedules.",
              markers: [
                { n: 1, label: "Preset / Cron", x: "82%", y: "38%" },
                { n: 2, label: "Hourly Daily Weekly", x: "45%", y: "52%" },
              ],
            },
          },
          {
            title: "Choose Sync mode + validation policy",
            pin: "Sync mode · Validation mode · Schema change policy",
            body: "In **Sync mode**, pick how rows move. Live tiles include **Full overwrite**, **Full append**, **Incremental append**, **Incremental deduped**, **CDC**, **SCD Type 2**, and **Mirror** (availability depends on connector pair).\n\nFor incremental / CDC / SCD2 / Mirror, fill **Cursor column** and/or **Primary key** when shown.\n\nSet **Validation mode** (**Strict** / **Maximum** / **Balanced**), **Schema change policy**, and optionally **Backfill new fields on schema change**.",
            figure: {
              src: "/docs/screenshots/app-pipelines-sync.png",
              alt: "Sync mode tiles with cursor and primary key fields",
              caption: "Sync mode panel — mode tiles plus cursor / primary key when the mode requires them.",
              markers: [
                { n: 1, label: "Sync mode tiles", x: "40%", y: "40%" },
                { n: 2, label: "Cursor / PK", x: "40%", y: "70%" },
                { n: 3, label: "Validation mode", x: "30%", y: "88%" },
              ],
            },
          },
          {
            title: "Bind Data contract + retries (optional)",
            pin: "Data contract · Retry & notifications",
            body: "In **Data contract**, optionally bind a **SIGNED** contract and enable **Require signed contract before each run (fail-closed)** so unattended ticks cannot write against an unsigned schema.\n\nIn **Retry & notifications**, set **Max retries**, **Retry backoff (seconds)**, and toggles **Notify on failure** / **Notify on success**.",
            figure: {
              src: "/docs/screenshots/app-pipelines-contract.png",
              alt: "Data contract panel on Create recurring sync form",
              caption: "Data contract — bind a signed agreement for fail-closed scheduled runs.",
              markers: [
                { n: 1, label: "Data contract", x: "30%", y: "40%" },
                { n: 2, label: "Require signed", x: "40%", y: "62%" },
              ],
            },
          },
          {
            title: "Save pipeline",
            pin: "Footer → Save pipeline",
            body: "Scroll to the form footer. Click **Save pipeline** (edit mode shows **Save changes**). On success you get a toast **Pipeline created** and return to the pipeline list/cards.\n\n**Cancel** discards the draft without saving.",
            figure: {
              src: "/docs/screenshots/app-pipelines-save.png",
              alt: "Save pipeline footer on Create recurring sync form",
              caption: "Footer — Cancel or Save pipeline when name, connectors, and tables are complete.",
              markers: [
                { n: 1, label: "Save pipeline", x: "88%", y: "90%" },
              ],
            },
          },
          {
            title: "Open the detail drawer — Run now & History",
            pin: "Click pipeline card → right drawer",
            body: "Click the saved pipeline card/row. The **right-side detail drawer** opens with tabs **Overview**, **Schema**, **History**, and **Config**.\n\nFooter actions: **Run now** (immediate governed tick), **Last job**, **Pause** / **Activate**, **Edit** (reopens the inline form), **Export YAML**, **Delete**.\n\nUse **History** to see prior ticks before jumping to Job Theater.",
            figure: {
              src: "/docs/screenshots/app-pipelines-drawer.png",
              alt: "Pipeline detail drawer open on the right with Run now",
              caption: "Detail drawer — Overview facts plus Run now / Pause / Edit. This is not the create form.",
              markers: [
                { n: 1, label: "Detail drawer", x: "78%", y: "45%" },
                { n: 2, label: "Overview tabs", x: "78%", y: "22%" },
                { n: 3, label: "Run now", x: "78%", y: "90%" },
              ],
            },
            tip: "**Edit** closes the drawer and returns you to the same **Create recurring sync** form fields for that pipeline.",
          },
          {
            title: "Prove each tick in Job Theater",
            pin: "Run now / schedule tick → Operations → Jobs",
            body: "Every cadence tick and every **Run now** creates a job with the same phases as Transfer Studio (gates → write → reconcile). Open **Operations → Jobs**, select the run, and confirm quarantine and reconciliation proof.\n\nDo not confuse the Jobs page with Pipelines — Pipelines owns the schedule; Jobs owns the proof for each tick.",
            figure: {
              src: "/docs/screenshots/app-jobs.png",
              alt: "Job Theater listing pipeline tick runs",
              caption: "Jobs — monitor each pipeline tick’s gates, quarantine, and reconcile proof.",
              markers: [
                { n: 1, label: "Runs", x: "28%", y: "40%" },
                { n: 2, label: "Proof", x: "70%", y: "55%" },
              ],
            },
            tip: "Wire **Notify on failure** (or webhooks) before promoting a pipeline to production cadence.",
          },
        ],
      },
      {
        id: "incremental",
        title: "Sync modes (exact product labels)",
        body: "Pipelines reuse Transfer Studio sync modes. Pick the tile that matches your identity strategy:",
        steps: [
          "**Full overwrite** — replace the target, then load the snapshot",
          "**Full append** — keep existing rows; append the full snapshot",
          "**Incremental append** — cursor/watermark new rows only",
          "**Incremental deduped** — cursor + primary-key upserts",
          "**CDC** — log-based changes (when the source supports it)",
          "**SCD Type 2** — versioned history (primary key required)",
          "**Mirror** — soft-delete dest keys missing from source (`_deleted`; physical COUNT(*) stays; primary key required)",
        ],
        tip: "Failed cells quarantine without silently dropping the rest of the batch — same engine as ad-hoc Transfer Studio runs.",
      },
      {
        id: "gitops",
        title: "GitOps YAML (optional)",
        body: "From the Pipelines toolbar you can **Export YAML** / **Import YAML** and optionally **Require signed** contracts for fail-closed unattended runs. Treat exported YAML as the reviewable definition of cadence + route + sync mode for change management.",
      },
      {
        id: "redis-keys",
        title: "Redis destination keys",
        body: "Redis stores each row under `prefix:identity`. Datawrap prefers `id` / `*_id` / `uuid`, then natural keys (`code`, `iso`, `name`, `sku`). Non-unique attributes like `capital` or `city` are never chosen when a stronger key exists. Set **Primary key** on the pipeline Sync mode panel (or on Map for ad-hoc runs) when you need an explicit column.",
      },
    ],
  },
  "help-contracts": {
    id: "help-contracts",
    slug: "contracts",
    category: "Connectors",
    title: "Data contracts",
    description: "Save, sign, and bind schema contracts so Pipelines stay fail-closed on unattended runs.",
    readTime: "9 min",
    icon: "shield",
    sections: [
      {
        id: "overview",
        title: "What a contract is",
        body: "A **data contract** is a signed schema agreement for a route. Contracts live under **Platform → Contracts**. Pipelines can require a **SIGNED** contract before each tick (**Require signed contract before each run**).",
        figure: {
          src: "/docs/screenshots/app-contracts.png",
          alt: "Contracts page in the workspace",
          caption: "Platform → Contracts — inventory of draft and signed schema agreements.",
          markers: [
            { n: 1, label: "Contracts nav", x: "8%", y: "36%" },
            { n: 2, label: "Contract list", x: "50%", y: "50%" },
          ],
        },
      },
      {
        id: "procedure",
        title: "Procedure: bind a contract to a pipeline",
        procedure: true,
        body: "Use contracts when unattended Pipelines must fail closed if the schema drifts or is unsigned.",
        workflow: [
          {
            title: "Create or open a contract",
            pin: "Platform → Contracts",
            body: "Open **Contracts**. Create from a proven Transfer Studio Validate result when available, or open an existing contract card/row. Review schema fields and status (**Draft** vs **Signed**).",
            figure: {
              src: "/docs/screenshots/app-contracts.png",
              alt: "Contracts inventory",
              caption: "Start on Contracts — open a card to inspect schema and signature state.",
            },
          },
          {
            title: "Inspect contract detail",
            pin: "Contract card → detail drawer",
            body: "Click a contract card/row to open its **detail drawer** (right side). Confirm field types, version, and signature state (**Draft** vs **Signed**). Sign when Owners are ready for production schedules.",
            figure: {
              src: "/docs/screenshots/app-contracts.png",
              alt: "Contracts page used to open contract detail",
              caption: "From the Contracts list, open a card to review schema and signature before binding to a pipeline.",
            },
          },
          {
            title: "Bind on Create recurring sync",
            pin: "Pipelines → Data contract panel",
            body: "On **Operations → Pipelines → New pipeline**, open the **Data contract** panel. Select the **SIGNED** contract and enable **Require signed contract before each run (fail-closed)** when unattended ticks must not write without agreement.",
            figure: {
              src: "/docs/screenshots/app-pipelines-contract.png",
              alt: "Pipeline form Data contract panel",
              caption: "Pipelines form — bind a signed contract for fail-closed schedule ticks.",
            },
            tip: "Unsigned or missing contracts with require-signed enabled block the tick the same way a failed Validate gate does.",
          },
        ],
      },
    ],
  },
  "help-transforms": {
    id: "help-transforms",
    slug: "transforms",
    category: "Operations",
    title: "Transforms",
    description: "Reusable transform definitions used by Map and governed writes.",
    readTime: "6 min",
    icon: "sparkle",
    sections: [
      {
        id: "overview",
        title: "Where Transforms live",
        body: "Open **Operations → Transforms**. This page inventories reusable transform definitions operators can apply on **Map** (cast, normalize email, date → ISO, and custom transforms your tenant enables).",
        figure: {
          src: "/docs/screenshots/app-transforms.png",
          alt: "Transforms page in the workspace",
          caption: "Operations → Transforms — reusable transform inventory for Map and pipelines.",
          markers: [
            { n: 1, label: "Transforms nav", x: "8%", y: "55%" },
            { n: 2, label: "Transform list", x: "50%", y: "45%" },
          ],
        },
      },
      {
        id: "with-map",
        title: "How Transforms relate to Map",
        body: "During Transfer Studio **Map**, each edge can show a transform (for example **Cast integer**, **Normalize email**). Accepting risk or approving the edge locks that transform into the governed route Pipelines reuse on every tick.",
        figure: {
          src: "/docs/screenshots/app-transfer-map.png",
          alt: "Map columns showing per-edge transforms",
          caption: "Map — per-column Transform column is where operators see and approve casts.",
        },
        tip: "Changing a production transform usually means editing the route on Map (or the pipeline Config), then re-running Validate before the next schedule tick.",
      },
    ],
  },
  "help-data-pilot": {
    id: "help-data-pilot",
    slug: "data-pilot",
    category: "Pilot & MCP",
    title: "Datawrap Pilot",
    description: "Natural-language triage for transfers, jobs, and schema questions.",
    readTime: "7 min",
    icon: "sparkle",
    sections: [
      {
        id: "use",
        title: "When to use Pilot",
        body: "Open **Operations → Pilot**. Ask about failed preflight gates, job status, connector readiness, or mapping suggestions. Pilot uses the same workspace context as Transfer Studio — it does not bypass Validate.",
        figure: {
          src: "/docs/screenshots/app-pilot.png",
          alt: "Datawrap Pilot chat workspace",
          caption: "Pilot — natural-language triage on the same governed engine.",
          markers: [
            { n: 1, label: "Pilot nav", x: "8%", y: "62%" },
            { n: 2, label: "Chat", x: "55%", y: "45%" },
          ],
        },
      },
      {
        id: "handoff",
        title: "Hand off to Transfer Studio",
        body: "When you need the full wizard, open **Transfer** from Pilot suggestions — maps and gates carry into Source → Destination → Map → Validate → Run.",
      },
      {
        id: "examples",
        title: "Example prompts",
        body: "Pilot understands workspace context — ask about specific jobs, connectors, or mapping decisions.",
        steps: [
          "Why did preflight gate Data integrity fail on my last transfer?",
          "Which connectors are transfer-ready for Snowflake upsert?",
          "Why did order_amt map to total_amount instead of payment_amount?",
          "Show quarantined rows from last night's pipeline",
        ],
      },
    ],
  },
  "help-mcp": {
    id: "help-mcp",
    slug: "mcp",
    category: "Pilot & MCP",
    title: "MCP Server for agents",
    description: "Governed transfer tools for Cursor, Claude, and VS Code — from the live MCP page.",
    readTime: "9 min",
    icon: "zap",
    sections: [
      {
        id: "tools",
        title: "Open the MCP page",
        body: "Open **System → MCP**. The page shows MCP server status, integration snippets for **Cursor**, **Claude Desktop**, **VS Code**, and custom GPT actions, plus recent tool call logs. Agents inherit workspace RBAC — no raw destination passwords.",
        figure: {
          src: "/docs/screenshots/app-mcp.png",
          alt: "MCP Server page with integrations and status",
          caption: "System → MCP — status, client snippets, and tool logs for governed agent runs.",
          markers: [
            { n: 1, label: "MCP nav", x: "8%", y: "72%" },
            { n: 2, label: "Integrations", x: "45%", y: "40%" },
          ],
        },
      },
      {
        id: "setup",
        title: "Procedure: connect Cursor",
        procedure: true,
        body: "Copy the Cursor snippet from the MCP page (or use the JSON below) and authenticate with a workspace API key from **Settings → API Keys**.",
        workflow: [
          {
            title: "Copy the MCP server URL",
            pin: "System → MCP → Cursor",
            body: "Expand the **Cursor** integration card. Copy the `mcpServers.dataflow.url` value shown for your tenant.",
          },
          {
            title: "Paste into Cursor Settings → MCP",
            pin: "Cursor → Settings → MCP",
            body: "Add the server entry, restart MCP if prompted, then call governed tools (connectors, preflight, jobs). Every agent-initiated transfer appears in **Job Theater** with an audit trail.",
          },
        ],
        code: '{\n  "mcpServers": {\n    "dataflow": {\n      "url": "https://api.datawrap.io/api/v1/mcp"\n    }\n  }\n}',
      },
      {
        id: "security",
        title: "Agent security",
        body: "MCP tools inherit workspace RBAC. Agents cannot read raw connector secrets — only trigger governed operations your role allows. Pipeline ticks and agent runs still must pass Validate before write.",
      },
    ],
  },
  "help-query": {
    id: "help-query",
    slug: "query-playground",
    category: "Pilot & MCP",
    title: "Query Playground",
    description: "Multi-dialect SQL editor for ad-hoc checks before transfer.",
    readTime: "6 min",
    icon: "code",
    sections: [
      {
        id: "dialects",
        title: "Open Query Playground",
        body: "Open **Operations → Query**. Pick a saved connector, choose dialect (PostgreSQL, MySQL, Snowflake, BigQuery, or generic SQL), and run read-oriented checks before you map or schedule.",
        figure: {
          src: "/docs/screenshots/app-query.png",
          alt: "Query Playground editor with SQL against saved connectors",
          caption: "Query — connector picker, SQL editor, and results before Transfer Studio.",
          markers: [
            { n: 1, label: "Query nav", x: "8%", y: "58%" },
            { n: 2, label: "Editor", x: "55%", y: "45%" },
          ],
        },
      },
      {
        id: "tips",
        title: "Tips",
        body: "Use Query Playground to validate source data shape before mapping. Format JSON results, clear the editor, and use snippet chips for common patterns. Playground is **read-only** — it refuses CALL/EXEC and DML. Governed writes still go through Transfer Studio / Pipelines.",
      },
      {
        id: "transfer-bridge",
        title: "Bridge to Transfer Studio",
        body: "When the shape looks right, open **Transfer** and reuse the same connector. On **Source**, choose **Query** (read-only SELECT) or **Stored procedure** (one CALL/EXEC, spooled once). On **Destination**, choose **Query** (one INSERT/MERGE/UPDATE with binds) or **Stored procedure** (one CALL per row). Failed dest statements quarantine — they never silently drop. CDC / SCD2 / mirror refuse callable dest writes. Validated playground queries do not skip Validate.",
      },
    ],
  },
  "help-job-theater": {
    id: "help-job-theater",
    slug: "job-theater",
    category: "Operations",
    title: "Job Theater & reconciliation",
    description: "Monitor every run from queue to checksum MATCH — quarantine, proof, and checkpoint resume (not one-click destination undo).",
    readTime: "10 min",
    icon: "jobs",
    sections: [
      {
        id: "open",
        title: "Open Job Theater",
        body: "Go to Operations → Jobs (also linked as Theater). Filter All / Running / Completed / Failed. Select a job to open Detail, Mapping, Quarantine, and Log tabs. Theater auto-opens Quarantine when a run completed with rejected rows.",
        figure: {
          src: "/docs/screenshots/app-jobs.png",
          alt: "Job Theater list and detail with phase timeline",
          caption: "Jobs — select a run to inspect phases, quarantine, and checksum proof.",
          markers: [
            { n: 1, label: "Job list", x: "28%", y: "40%" },
            { n: 2, label: "Phase rail", x: "55%", y: "30%" },
            { n: 3, label: "Proof", x: "70%", y: "55%" },
          ],
        },
      },
      {
        id: "stages",
        title: "Job phases",
        body: "Every ad-hoc transfer and every pipeline tick uses the same phase rail:",
        steps: [
          "Queued — accepted by the control plane",
          "Read — source extract / profile",
          "Gates — Validate decisions applied before write",
          "Write — clean rows land; bad cells quarantine",
          "Reconcile — row counts and content hashes compared",
          "Done — Match, Mismatch, failed, cancelled, or completed with quarantine",
        ],
      },
      {
        id: "quarantine-ui",
        title: "Procedure: inspect quarantine",
        procedure: true,
        body: "Quarantine means bad values were isolated with column, value, and reason — never silently dropped.",
        workflow: [
          {
            title: "Open the job",
            pin: "Operations → Jobs → select run",
            body: "Look for Quarantine status or a non-zero rejected count in the stats strip.",
          },
          {
            title: "Open the Quarantine tab",
            pin: "Job detail → Quarantine",
            body: "Review each bad cell: column, offending value, rule, and timestamp. Export for offline review if needed.",
          },
          {
            title: "Fix and re-run",
            pin: "Open Studio · Retry · Resume from checkpoint",
            body: "Return to Map or Validate to fix mappings / encoding, then Retry or Resume from checkpoint. Pipeline ticks will fail the same way until the underlying route is fixed.",
            tip: "Completed with quarantine still wrote clean rows — proof may Match on the clean set while quarantine holds the rest.",
          },
        ],
      },
      {
        id: "proof",
        title: "Checksum MATCH",
        body: "Gate-8 / post-load proof compares source and destination row counts and content hashes. Theater shows Match or Mismatch with row fidelity. Finance and compliance can treat Match as the archiveable proof of the load — export from the job when required.",
      },
    ],
  },

  "help-enterprise": {
    id: "help-enterprise",
    slug: "enterprise",
    category: "Enterprise",
    title: "Enterprise controls",
    description: "SSO, Team RBAC, BYOK, region pinning, audit — configured in Settings by Owners.",
    readTime: "12 min",
    icon: "shield",
    sections: [
      {
        id: "where",
        title: "Where enterprise settings live",
        body: "Signed-in Owners open **System → Settings**. Tabs typically include General, Security, Enterprise, SSO, Team, Notifications, AI Models, API Keys, and Audit Logs. Day-to-day operators usually stay on Connectors, Transfer, Pipelines, and Jobs.",
        figure: {
          src: "/docs/screenshots/app-settings.png",
          alt: "Settings page in the workspace",
          caption: "System → Settings — Owners configure SSO, Team, Enterprise, and API keys here.",
          markers: [
            { n: 1, label: "Settings nav", x: "8%", y: "68%" },
            { n: 2, label: "Settings tabs", x: "40%", y: "20%" },
          ],
        },
      },
      {
        id: "sso",
        title: "Procedure: configure SSO",
        procedure: true,
        body: "Use SAML 2.0 (Okta / Azure AD / OneLogin) or OIDC (Google / Auth0 / Azure AD).",
        workflow: [
          {
            title: "Open Settings → SSO",
            pin: "System → Settings → SSO",
            body: "Choose SAML or OIDC. Paste IdP metadata URL or upload metadata XML. Map roles if your IdP sends group claims.",
            figure: {
              src: "/docs/screenshots/app-settings-sso.png",
              alt: "Settings SSO tab",
              caption: "SSO tab — configure SAML / OIDC for the tenant.",
            },
          },
          {
            title: "Test, then Save",
            pin: "SSO → Test · Save",
            body: "Run Test login with a pilot user. On success you see SSO ready. Save to enforce SSO for the tenant.",
            tip: "Keep a break-glass Owner account path documented with IT before enforcing SSO-only.",
          },
        ],
      },
      {
        id: "team",
        title: "Team roles",
        body: "Open **Settings → Team** to invite members by email and assign roles.",
        steps: [
          "Viewer — inspect Jobs, proof, and connectors (no execute)",
          "Editor — create connectors, run Transfer Studio and Pipelines",
          "Owner — SSO, BYOK, Enterprise tab, API keys, Team admin",
        ],
        figure: {
          src: "/docs/screenshots/app-settings-team.png",
          alt: "Settings Team tab",
          caption: "Team — invite members and assign Viewer / Editor / Owner.",
        },
      },
      {
        id: "byok-region",
        title: "BYOK, region, and audit",
        body: "Settings → Enterprise sets tenant name, custom domain, data region, MFA, session timeout, and IP allowlist. Optional BYOK wraps connector secrets with your KMS key. Settings → Audit Logs lists mapping decisions, job runs, quarantine events, and MCP calls for SOC 2 / GDPR / HIPAA review. Settings → Security downloads the Security posture report for procurement.",
      },
    ],
  },

  "help-api": {
    id: "help-api",
    slug: "api-reference",
    category: "Enterprise",
    title: "API reference",
    description: "REST endpoints for connectors, transfers, jobs, and MCP.",
    readTime: "12 min",
    icon: "code",
    sections: [
      {
        id: "auth",
        title: "Authentication",
        body: "Bearer tokens scoped to workspace. Enterprise uses SSO-backed service accounts.",
        code: 'curl -H "Authorization: Bearer $TOKEN" https://api.datawrap.io/api/v1/connectors',
      },
      {
        id: "endpoints",
        title: "Core endpoints",
        body: "Catalog, preflight, run, and job status endpoints mirror Transfer Studio behavior.",
        steps: [
          "Canonical prefix is /api/v1 — see docs/API_VERSIONING.md for deprecation policy",
          "GET /api/v1/connectors — list with transfer-ready status",
          "POST /api/v1/preflight — run nine core gates",
          "POST /api/v1/transfer/run — execute governed load",
          "GET /api/v1/connectors/jobs/{id} — status, quarantine, reconciliation",
        ],
        figure: {
          src: "/docs/screenshots/app-jobs.png",
          alt: "Job Theater for API-started transfers",
          caption: "API runs surface in Job Theater with the same stage timeline and checksum proof.",
        },
      },
      {
        id: "webhooks",
        title: "Webhooks",
        body: "Subscribe to job.completed, job.failed, and pipeline.quarantine_threshold events. Payloads include job ID, gate results, and reconciliation summary — no row payloads unless explicitly configured.",
        code: 'POST /api/v1/webhooks\n{ "url": "https://your.app/hooks/dataflow", "events": ["job.completed"] }',
      },
    ],
  },
  "help-faq": {
    id: "help-faq",
    slug: "faq",
    category: "Enterprise",
    title: "Frequently asked questions",
    description: "Common questions from data platform and analytics leaders.",
    readTime: "6 min",
    icon: "book",
    sections: [
      {
        id: "q1",
        title: "What is quarantine?",
        body: "Rows that fail validation during load are isolated with the column, value, and reason — never silently dropped. Open Operations → Jobs → Quarantine on the run to inspect or export them.",
        figure: {
          src: "/docs/screenshots/app-jobs.png",
          alt: "Job Theater showing quarantine and reconcile evidence",
          caption: "Quarantine and proof live on the job — open Theater to inspect failed rows.",
        },
      },
      {
        id: "q2",
        title: "Who can run transfers?",
        body: "Editors and Owners can create connectors and execute Transfer Studio / Pipelines. Viewers can open Jobs and review MATCH / quarantine proof. Roles are assigned under Settings → Team (and can come from IdP group claims when SSO is configured).",
      },
      {
        id: "q3",
        title: "Do pipelines skip preflight?",
        body: "No. Every schedule tick reuses the same Validate gates and post-load checksum path as a manual transfer. A failing gate blocks the write; the tick appears Failed in Jobs until the route is fixed.",
      },
      {
        id: "q3b",
        title: "How is data protected in transit and at rest?",
        body: "Connectors use TLS to sources and destinations. Secrets are encrypted at rest (optional customer BYOK). Quarantine replaces silent drops; post-load checksums prove what landed. Download the Security posture report from Settings → Security for procurement questionnaires.",
      },
      {
        id: "q4",
        title: "How is Datawrap different from ETL scripts?",
        body: "Semantic mapping with confidence, fail-fast Validate gates, quarantine instead of silent coerce, and checksum MATCH after write — with SSO, RBAC, and audit built for enterprise teams.",
      },
    ],
  },

};

export function getHelpDoc(id: HelpDocId): HelpDocArticle {
  return ARTICLES[id];
}

/** Full-text search across help articles for the docs portal. */
export function searchHelpDocs(query: string): HelpDocArticle[] {
  const q = query.trim().toLowerCase();
  if (!q) return [];
  const hit = (text: string | undefined) => Boolean(text && text.toLowerCase().includes(q));
  return Object.values(ARTICLES).filter(
    (a) =>
      hit(a.title) ||
      hit(a.description) ||
      hit(a.category) ||
      a.sections.some(
        (s) =>
          hit(s.title) ||
          hit(s.body) ||
          hit(s.tip) ||
          hit(s.code) ||
          hit(s.figure?.caption) ||
          hit(s.figure?.alt) ||
          s.steps?.some((step) => hit(step)) ||
          s.workflow?.some(
            (step) =>
              hit(step.title) ||
              hit(step.body) ||
              hit(step.pin) ||
              hit(step.tip) ||
              hit(step.figure?.caption) ||
              hit(step.figure?.alt),
          ),
      ),
  );
}

export function listAllHelpDocs(): HelpDocArticle[] {
  return HELP_DOC_IDS.map((id) => ARTICLES[id]);
}

export function helpDocFromSlug(slug: string): HelpDocId | null {
  const entry = Object.values(ARTICLES).find((a) => a.slug === slug);
  return entry?.id ?? null;
}

export function isHelpDocRoute(route: string): route is HelpDocId {
  return (HELP_DOC_IDS as readonly string[]).includes(route);
}

export function hashForHelpDoc(id: HelpDocId): string {
  return `#/help/${ARTICLES[id].slug}`;
}

export function helpDocNeighbors(id: HelpDocId): { prev: HelpDocId | null; next: HelpDocId | null } {
  const idx = HELP_DOC_IDS.indexOf(id);
  return {
    prev: idx > 0 ? HELP_DOC_IDS[idx - 1] : null,
    next: idx < HELP_DOC_IDS.length - 1 ? HELP_DOC_IDS[idx + 1] : null,
  };
}

export const HELP_VIDEO_TUTORIALS = [
  { title: "Transfer Studio in 6 minutes", duration: "6:12", topic: "Getting started" },
  { title: "Preflight gates walkthrough", duration: "4:45", topic: "Transfer Studio" },
  { title: "Semantic mapping deep dive", duration: "8:30", topic: "Mapping" },
  { title: "MCP setup for Cursor", duration: "5:20", topic: "Agents" },
];

export const HELP_PRODUCT_CARDS = [
  {
    title: "Transfer Studio",
    body: "Map, preflight, and prove any→any loads with semantic intelligence.",
    doc: "help-transfer-studio" as HelpDocId,
    icon: "transfer",
  },
  {
    title: "Datawrap Pilot",
    body: "Natural-language triage grounded in your workspace.",
    doc: "help-data-pilot" as HelpDocId,
    icon: "sparkle",
  },
  {
    title: "MCP Server",
    body: "Governed transfers from Cursor, Claude, and VS Code.",
    doc: "help-mcp" as HelpDocId,
    icon: "zap",
  },
];
