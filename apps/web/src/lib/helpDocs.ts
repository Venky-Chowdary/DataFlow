import { WORKSPACE_SHOT } from "./workspaceShots";

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
  | "help-exit-readiness"
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
  "help-exit-readiness",
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
    title: "Connectors & schedules",
    description: "Connections, contracts, schedules, and incremental sync.",
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
    description: "Job space, trust score, reversibility, quarantine, and reconciliation.",
    docs: ["help-job-theater", "help-exit-readiness", "help-transforms"],
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
        body: "Datawrap moves data between systems and shows you whether it arrived intact. Transfer Studio maps columns across systems and runs preflight gates before any write: nine core gates plus the policy gates each route needs. After the write it reads the destination back and compares it with the source. Schedules, Pilot, and MCP use the same engine.",
      },
      {
        id: "surfaces",
        title: "Product surfaces (sidebar map)",
        body: "The sidebar in the signed-in workspace matches these guides. Start in Transfer Studio, then move on to Schedules, Contracts, Pilot, and MCP.",
        steps: [
          "Platform — Overview, Transfer, Connectors, Contracts, Exit readiness",
          "Operations — Jobs (Job space), Schedules, Transforms, Library, Query, Pilot",
          "System — Settings (SSO and Team), MCP, Help, Proofs",
          "The path every load takes — Source → Destination → Transform → Map → Validate → Run → Job space",
        ],
        figure: {
          src: WORKSPACE_SHOT.overview,
          alt: "Workspace Overview with the Platform, Operations, and System sidebar",
          caption: "Overview, with the Platform, Operations, and System sidebar used in every guide.",
          markers: [
            { n: 1, label: "Platform", x: "4%", y: "7.5%" },
            { n: 2, label: "Operations", x: "4%", y: "30%" },
            { n: 3, label: "System", x: "4%", y: "55%" },
          ],
        },
      },
      {
        id: "first-transfer",
        title: "Your first transfer (step by step)",
        procedure: true,
        body: "Follow this path in the signed-in workspace. Every screenshot comes from the hosted product.",
        workflow: [
          {
            title: "Confirm the workspace is healthy",
            pin: "Sidebar → Overview",
            body: "Open **Overview** and check throughput, migration health, and recent jobs. If the panels fail to load, fix connectivity with a workspace admin before you create connections.",
            figure: {
              src: WORKSPACE_SHOT.overview,
              alt: "Workspace Overview with throughput, migration health, and parity trend",
              caption: "Overview: throughput, migration status, migration health, and the parity trend.",
              markers: [
                { n: 1, label: "Overview", x: "5%", y: "11%" },
                { n: 2, label: "Throughput", x: "20%", y: "11%" },
                { n: 3, label: "Migration health", x: "20%", y: "38%" },
              ],
            },
          },
          {
            title: "Add or test a connector",
            pin: "Platform → Connectors → New connection",
            body: "Open **Platform → Connectors**. Click **New connection** to add a source or destination, or click **Test** on an existing row and check that **Last test** shows passed.",
            figure: {
              src: WORKSPACE_SHOT.connectors,
              alt: "Connectors page listing the demo connections with their last test result",
              caption: "Connectors: New connection at the top right, and the last test result on each row.",
              markers: [
                { n: 1, label: "New connection", x: "91%", y: "11%" },
                { n: 2, label: "Last test", x: "71%", y: "21%" },
                { n: 3, label: "Test", x: "91%", y: "21%" },
              ],
            },
          },
          {
            title: "Run Transfer Studio Source → Destination → Transform → Map → Validate → Run",
            pin: "Platform → Transfer  ·  or New transfer",
            body: "Open **Platform → Transfer** (or **New transfer**). On **Source**, pick a **Database** connection and a table, and check the source schema. Then continue through **Destination**, **Transform** (click **Continue without transforming** when there is no recipe), **Map**, and **Validate**.\n\nFix every blocked gate before **Run**. On Validate the footer holds the one primary action: the fix for the blocking gate, or **Execute** when the decision is approve. The Transfer Studio guide walks through a full Postgres to Snowflake load.",
            figure: {
              src: WORKSPACE_SHOT.transferSource,
              alt: "Transfer Studio Source step with Orders Postgres (demo) and sales_orders",
              caption: "Source: pick the connection and table, then Continue to Destination.",
              markers: [
                { n: 1, label: "Source", x: "17%", y: "7%" },
                { n: 2, label: "Continue", x: "90%", y: "95%" },
              ],
            },
          },
          {
            title: "Prove the load in Job space",
            pin: "Operations → Jobs → Trust Report",
            body: "Open **Operations → Jobs** and select the run. The **Trust Report** reads the destination back and compares it with the source: rows in each, rows that differ, and the checksum per table. Open **Reversibility** if the route must be able to give the source values back. Neither is an exactly-once certificate; delivery is at-least-once.",
            figure: {
              src: WORKSPACE_SHOT.jobTrust,
              alt: "Job space Trust Report: 5,100 rows in source and destination, 0 differ, full checksum match",
              caption: "Job space: the Trust Report for a 5,100-row load. 0 rows differ, full checksum match.",
              markers: [
                { n: 1, label: "Trust Report", x: "71%", y: "26%" },
                { n: 2, label: "Rows differ", x: "82%", y: "62%" },
                { n: 3, label: "Match", x: "91%", y: "87%" },
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
          "Job space — run history with the Trust Report, reversibility, quarantine, and sign-off",
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
        body: "Datawrap is delivered as a **hosted enterprise workspace** at your tenant URL (for example `your-company.datawrap.io`). After sign-in you work entirely in the browser — there is nothing to install on operator laptops.\n\nThe signed-in product includes **Overview**, **Connectors**, **Transfer Studio**, **Schedules**, **Jobs** (Job space), **Query**, **Pilot**, and **Settings**. Your IT team configures SSO and optional BYOK once; day-to-day operators create connections, run governed transfers, and prove loads.",
        figure: {
          src: WORKSPACE_SHOT.overview,
          alt: "Workspace Overview after sign-in",
          caption: "After SSO, Overview shows throughput, migration health, and recent jobs for your workspace.",
          markers: [
            { n: 1, label: "Overview", x: "6%", y: "11%" },
            { n: 2, label: "Migration health", x: "20%", y: "38%" },
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
            body: "A workspace **admin** (or Datawrap provisioning) invites you by email with one of four roles:\n\n**Viewer** — read Jobs, quarantine, and Trust Reports; cannot create connectors or run transfers.\n\n**Operator** — run and repair pipelines (run, retry, resume, sign off) but not design them: no planning, connector edits, or schedule changes.\n\n**Editor** — create connectors, run **Transfer Studio** and **Schedules**, and open **Job space**. Only an Admin adds, removes or re-roles members.\n\n**Admin** — everything Editors can do, plus **Settings → SSO**, **Enterprise**, **BYOK**, **Team** roles, and API keys. There is no Owner role in the product.\n\nOpen the invite link, or go directly to your tenant URL (for example `https://your-company.datawrap.io`). Sign in with your company identity provider — **Okta**, **Azure AD / Entra ID**, **Google Workspace**, or another SAML 2.0 / OIDC IdP configured for the tenant.",
          },
          {
            title: "Sign in with SSO",
            pin: "Login → Continue with SSO",
            body: "On the login screen choose **Continue with SSO** (or your company's branded SSO button). Authenticate with your work account. Datawrap issues a workspace session that inherits your RBAC role from the invite or from IdP group claims.\n\nA successful sign-in lands on **Overview**. Confirm the top bar shows your **workspace** name (switcher, when you belong to more than one) and that the left sidebar lists **Platform** (Transfer, Connectors, Contracts) and **Operations** (Jobs, Schedules). If you only see a blank or permission error, stop and ask a workspace admin to verify your role under **Settings → Team**.",
            figure: {
              src: WORKSPACE_SHOT.overview,
              alt: "Workspace Overview after a successful sign-in",
              caption: "A successful sign-in lands on Overview. Check the workspace name and sidebar before you go on.",
              markers: [
                { n: 1, label: "Workspace name", x: "23%", y: "3%" },
                { n: 2, label: "Platform", x: "4%", y: "7.5%" },
                { n: 3, label: "Operations", x: "4%", y: "30%" },
              ],
            },
          },
          {
            title: "Confirm you can reach Connectors and Transfer",
            pin: "Sidebar → Platform → Connectors  ·  Transfer",
            body: "In the left sidebar open **Platform → Connectors**. You should see the connections table, empty or with existing rows. Then open **Platform → Transfer** (Transfer Studio) or click **New transfer** if shown.\n\nYou are ready for day-one work when:\n\n**1.** **New connection** is visible on Connectors (Editors and Admins).\n\n**2.** Transfer Studio opens the **Source → Destination → Transform → Map → Validate → Run** wizard.\n\n**3.** **Operations → Jobs** opens Job space for past and live runs.\n\nIf Connectors or Transfer is missing from the sidebar, your role is likely **Viewer** — ask a workspace admin to raise it to **Editor** under **Settings → Team**.",
            figure: {
              src: WORKSPACE_SHOT.connectors,
              alt: "Connectors page with New connection visible",
              caption: "Connectors in the sidebar, and New connection for Editors and Admins.",
              markers: [
                { n: 1, label: "Connectors", x: "6%", y: "18.5%" },
                { n: 2, label: "New connection", x: "91%", y: "11%" },
              ],
            },
            tip: "**Viewer** — inspect Jobs and proof only. **Editor** — run transfers and manage connectors (members are managed by an Admin). **Admin** — SSO, BYOK, Team roles, and enterprise controls under **Settings**. Switching workspace in the top bar (or on Team) is live for every Settings tab.",
          },
        ],
      },
      {
        id: "admin-setup",
        title: "Admin checklist (workspace admins)",
        body: "Workspace **admins** complete these once per workspace. Operators do not need this checklist to run day-to-day transfers — it is for IT and security readiness.",
        steps: [
          "**Settings → SSO** — choose SAML or OIDC, paste IdP metadata URL (or upload XML), map roles if your IdP sends group claims, then click **Test login** and **Save**.",
          "**Settings → Enterprise** — set tenant display name, data **region**, session timeout, and (for a tenant custom domain) an optional IP allowlist. The workspace picker here is the same live workspace as Team.",
          "**Settings → Enterprise → BYOK** — upload your customer KMS key when procurement requires customer-managed encryption for connector secrets.",
          "**Settings → Team** — invite Viewers, Editors, and Admins with least privilege; re-check IdP group → role mapping after SSO setup. Member and login lists refresh while the tab is open.",
          "**Settings → Security** — download the **Security posture report** for SOC 2 / GDPR / HIPAA questionnaires. Controls are implemented; no third-party certificate is claimed.",
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
    description: "A worked load from Postgres into Snowflake through Source, Destination, Transform, Map, Validate, and Run, using screenshots from the hosted workspace.",
    readTime: "18 min",
    icon: "transfer",
    sections: [
      {
        id: "example",
        title: "Example transfer used in this guide",
        body: "Every screenshot below comes from one real session in the hosted workspace.\n\n**Source:** **Orders Postgres (demo)**, table `public.sales_orders`: 5,100 rows, 11 columns (`order_id`, `region`, `store_code`, `product_sku`, `units`, `unit_price`, `discount_pct`, `order_total`, `status`, `ordered_at`, `attributes`).\n\n**Destination:** **Warehouse Snowflake (demo)**, database `DW_DEMO`, table `SALES_ORDERS_REPORTING`. The table did not exist before the run.\n\n**Sync mode:** **Full append** with manual approval and strict validation.\n\n**Result:** 11 of 11 columns mapped, 17 gates passed with 6 not applicable, 5,100 rows written, and a full checksum match in Job space.\n\nSchedules, Pilot, and MCP use the same six steps. None of them can skip a gate or the read-back.",
      },
      {
        id: "procedure",
        title: "Procedure: load sales_orders into Snowflake",
        procedure: true,
        body: "Follow the six steps in order. The numbered markers on each screenshot point at the control the step talks about.",
        workflow: [
          {
            title: "Step 1 — Source: pick the table",
            pin: "Sidebar → Transfer  ·  or New transfer",
            body: "Open **Transfer** in the sidebar or click **New transfer**. The rail across the top shows the six steps: **Source**, **Destination**, **Transform**, **Map**, **Validate**, **Run**.\n\nUnder **Where is your data?** choose **Database**. Pick **Orders Postgres (demo)** as the source connector, keep **Table** as the extract, and choose `sales_orders`.\n\nThe **Source schema** panel on the right lists the 11 columns with their types and shows sample rows. Check it before you continue: that profile is what Map and Validate work from. Then click **Continue to Destination**.",
            figure: {
              src: WORKSPACE_SHOT.transferSource,
              alt: "Transfer Studio Source step with Orders Postgres (demo) and sales_orders selected, source schema on the right",
              caption: "Source: Database, Orders Postgres (demo), table sales_orders, with the source schema and sample rows.",
              markers: [
                { n: 1, label: "Database", x: "33%", y: "19%" },
                { n: 2, label: "Source connector", x: "15%", y: "34%" },
                { n: 3, label: "Table", x: "15%", y: "46%" },
                { n: 4, label: "Source schema", x: "80%", y: "35%" },
                { n: 5, label: "Continue", x: "90%", y: "95%" },
              ],
            },
            tip: "Choose **SQL query** as the extract to load the result of a SELECT instead of a whole table. It goes through the same Map and Validate steps.",
          },
          {
            title: "Step 2 — Destination: pick the warehouse and table",
            pin: "Destination → Warehouse → saved connection → target location",
            body: "On **Destination** keep **Warehouse** selected and pick **Warehouse Snowflake (demo)** from **Saved connections**. Under **Target location** set the database to `DW_DEMO` and the table to `SALES_ORDERS_REPORTING`.\n\nBecause the table does not exist yet, the page says **Table not found: Datawrap will create it automatically on first write**. Nothing is created now. The DDL is shown for approval in Validate.\n\n**Sync defaults** shows the mode for this run: **Full append**, manual approval, strict validation. Open **Advanced** for overwrite, incremental, CDC, SCD Type 2, mirror, cursor, and drift settings. Click **Continue to Transform**.",
            figure: {
              src: WORKSPACE_SHOT.transferDest,
              alt: "Transfer Studio Destination step with Warehouse Snowflake (demo) and DW_DEMO.SALES_ORDERS_REPORTING",
              caption: "Destination: Warehouse Snowflake (demo), DW_DEMO.SALES_ORDERS_REPORTING, created on first write.",
              markers: [
                { n: 1, label: "Warehouse", x: "20%", y: "24%" },
                { n: 2, label: "Snowflake connection", x: "30%", y: "82%" },
                { n: 3, label: "Target location", x: "60%", y: "33%" },
                { n: 4, label: "Sync defaults", x: "62%", y: "64%" },
              ],
            },
            tip: "File export is not available on the hosted cloud. Use a warehouse, database, or object-store connection as the destination.",
          },
          {
            title: "Step 3 — Transform: add a recipe or skip it",
            pin: "Transform → Continue without transforming",
            body: "Transform is where a pre-load recipe lives: rename, round, filter, or apply rules from the **Transform Library**. If you add steps, Validate checks the transformed rows rather than the raw source.\n\nThis example loads the table as it is, so click **Continue without transforming**.",
            figure: {
              src: WORKSPACE_SHOT.transferTransform,
              alt: "Transfer Studio Transform step with the Transform Library and column list",
              caption: "Transform: no recipe for this load, so Continue without transforming.",
              markers: [
                { n: 1, label: "Transform Library", x: "18%", y: "36%" },
                { n: 2, label: "Continue without transforming", x: "92%", y: "92%" },
              ],
            },
          },
          {
            title: "Step 4 — Map: review every column",
            pin: "Map columns → Approve eligible → Continue to Validate",
            body: "**Map columns** pairs each source column with a destination name and type. Here the header reads **11 of 11 mapped · 11 ready to approve**. `order_id` stays `BIGINT`, the text columns become `VARCHAR`, and the decimals keep their precision as `NUMBER(10,2)`, `NUMBER(5,2)`, and `NUMBER(12,2)`.\n\nUse **Needs attention** to see only the rows that need a decision. Expand a row to see why it was mapped that way. The JSON column `attributes` becomes `VARIANT` in Snowflake, and the expanded row says the conversion keeps every value.\n\nClick **Approve eligible (11)**, then **Continue to Validate**. Pairs that look alike but mean different things, such as quantity and amount, are never approved in bulk. You confirm or remap them one by one.",
            figure: {
              src: WORKSPACE_SHOT.transferMap,
              alt: "Transfer Studio Map: 11 of 11 columns mapped from sales_orders to SALES_ORDERS_REPORTING",
              caption: "Map: 11 of 11 mapped, each row with its destination type and a Safe badge.",
              markers: [
                { n: 1, label: "11 of 11 mapped", x: "22%", y: "24%" },
                { n: 2, label: "Needs attention", x: "27%", y: "29%" },
                { n: 3, label: "Approve eligible", x: "62%", y: "29%" },
                { n: 4, label: "Destination columns", x: "82%", y: "31%" },
                { n: 5, label: "Continue to Validate", x: "88%", y: "94%" },
              ],
            },
            tip: "**More → Open mapping proof** shows the confidence and fidelity evidence for each pair before you validate.",
          },
          {
            title: "Step 5 — Validate: clear every blocking gate",
            pin: "Validate footer → the one primary action",
            body: "Validate runs every gate the route needs. On this route that is 23: the nine core gates plus policy gates for population fit, DDL, staging, sync contract, column coverage, and field reduction.\n\nThe first run blocked on one gate: **Create destination table**. The table does not exist, and strict validation will not create a table nobody approved. The card shows the exact `CREATE TABLE` statement, and the footer's primary button becomes **Approve destination DDL**.\n\nAfter approving and re-running, the summary reads **APPROVE · 17 passed · 0 blocked gates · 6 skipped** in 107.7 seconds. The six skipped gates do not apply to a new table with no code crosswalk. **Type coercions in sample** lists 25 JSON values that will be written as `VARIANT`, none quarantined.\n\n**Execute** unlocks only when no gate is blocked and the decision is approve. The headline still says **not migration proven**: that comes from the read-back after the write.",
            figure: {
              src: WORKSPACE_SHOT.transferValidate,
              alt: "Transfer Studio Validate: APPROVE, 17 passed, 0 blocked, 6 skipped across 23 gates",
              caption: "Validate: 17 passed, 0 blocked, 6 skipped. Execute is unlocked; the read-back is still required.",
              markers: [
                { n: 1, label: "Decision", x: "25%", y: "21%" },
                { n: 2, label: "Gate counts", x: "30%", y: "26%" },
                { n: 3, label: "Type coercions", x: "26%", y: "43%" },
                { n: 4, label: "Execute", x: "83%", y: "96%" },
              ],
            },
            tip: "To see each gate on its own, scroll to **Validation rules**. The Preflight gates guide explains each gate ID.",
          },
          {
            title: "Step 6 — Run: execute and watch the load",
            pin: "Validate → Execute  ·  then Job space",
            body: "Click **Execute**. Run shows the phases (**Extract**, **Transform**, **Load**, **Reconcile**), rows read and written, and a live event log.\n\nWhen it finishes, the append check compares the destination before and after: **0 before, 5,100 after, 5,100 appended**, and the delta balances. Open the job in **Job space** for the Trust Report, which reads Snowflake back and compares it with Postgres.",
            figure: {
              src: WORKSPACE_SHOT.transferRunComplete,
              alt: "Transfer Studio Run complete: 5,100 appended, destination 0 before and 5,100 after, delta balanced",
              caption: "Run: 5,100 appended into an empty table, and the append delta balances.",
              markers: [
                { n: 1, label: "Completed", x: "78%", y: "17%" },
                { n: 2, label: "Append delta", x: "33%", y: "34%" },
                { n: 3, label: "Before / after", x: "70%", y: "47%" },
                { n: 4, label: "Delta balanced", x: "30%", y: "86%" },
              ],
            },
            tip: "Delivery is at-least-once. A retried append can write a batch twice, so use an upsert mode with a business key when duplicates matter.",
          },
        ],
      },
      {
        id: "write-modes",
        title: "Write modes (Advanced)",
        body: "Open **Advanced** on Destination to set the sync mode. Schedules offer the same modes on every run.",
        steps: [
          "**Full append** — keep existing rows and append the full snapshot (used in this example).",
          "**Full overwrite** — replace the target, then load.",
          "**Incremental append** — new rows only, by cursor.",
          "**Incremental deduped** — cursor plus primary-key upserts.",
          "**CDC** — log-based changes when the source supports it. Delivery is at-least-once upsert unless a route proves otherwise.",
          "**SCD Type 2 / Mirror** — history and mirror modes; a primary key is required where noted.",
          "**Destination write → Query** — one INSERT, MERGE, or UPDATE with `:binds` mapped to source columns. Failed rows go to quarantine. Not CDC.",
          "**Destination write → Stored procedure** — one CALL or EXEC per row. SQLite has no destination procedures; a destination query INSERT is allowed.",
        ],
      },
      {
        id: "quarantine",
        title: "After Run: quarantine and the Trust Report",
        body: "Rows that fail a type or policy check go to quarantine with the **column**, **value**, and **reason**. They are never dropped quietly. After the write, Datawrap reads the destination back and compares counts and checksums with the source. The Trust Report shows the result: for this run, 5,100 rows on each side, 0 that differ, and a full checksum match.",
        tip: "Open **Jobs → select the run → Trust Report** before you sign off. **Report (.md)** downloads the evidence for an audit file.",
        figure: {
          src: WORKSPACE_SHOT.jobTrust,
          alt: "Job space Trust Report: 5,100 rows in source and destination, 0 differ, full checksum match",
          caption: "Job space: the Trust Report for the sales_orders load.",
          markers: [
            { n: 1, label: "Trust Report", x: "71%", y: "26%" },
            { n: 2, label: "Rows proven equal", x: "61%", y: "56%" },
            { n: 3, label: "Source / destination", x: "82%", y: "51%" },
            { n: 4, label: "Full checksum", x: "86%", y: "87%" },
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
        body: "Validate runs in Transfer Studio, and the same gates run again before every scheduled run. Execute stays locked until the decision is approve. A pass that still needs review does not unlock the write.",
        tip: "Schedules cannot skip Validate. A failing gate on a schedule blocks the write the same way it blocks a manual run, and the schedule parks with the reason.",
      },
      {
        id: "core-gates",
        title: "Core gates (before write)",
        body: "Every route runs these nine. Open any card under **Validation rules** for the rule text and what to change.",
        steps: [
          "G1 Source readable — source connects and rows can be read",
          "G2 Destination write access — destination is reachable with write/create privilege",
          "G3 Schema contract — source and target schemas are compatible",
          "G4 Column mappings — every column maps above the confidence threshold",
          "G5 Sample dry-run — sample rows pass the same transforms writers use",
          "G6 Target DDL — required CREATE / ALTER statements are valid",
          "G7 Staging capacity — destination has headroom for the volume",
          "G8 Sample reconciliation — pre-write sample identity and uniqueness hold (post-load checksum runs after Execute)",
          "G9 Data integrity — encoding, required nulls, identity duplicates, precision on the sample",
        ],
        figure: {
          src: WORKSPACE_SHOT.transferValidateGates,
          alt: "Validation rules grid: 23 gates for the Postgres to Snowflake route, G1 through G22",
          caption: "Validation rules on the sales_orders route: 23 gates, 17 passed (green) and 6 not applicable (grey).",
          markers: [
            { n: 1, label: "G1 Source readable", x: "18%", y: "23%" },
            { n: 2, label: "G8 Sample reconciliation", x: "61%", y: "33%" },
            { n: 3, label: "Not applicable", x: "73%", y: "42%" },
            { n: 4, label: "Post-write proof pending", x: "40%", y: "77%" },
          ],
        },
      },
      {
        id: "policy-gates",
        title: "Policy & drift gates",
        body: "Validate adds gates for the sync mode, the destination, and your Advanced settings. A gate that does not apply to the route shows as skipped, not passed. On the example route 14 policy gates were added; 8 passed and 6 did not apply.",
        steps: [
          "G3F Population fit — values fit bounded destination types such as DECIMAL(p,s) and VARCHAR(n), checked on the rows this run holds",
          "G6C Create destination table — a table that does not exist is created only after you approve its DDL",
          "G10 Schema change policy — detected drift is allowed by the route's policy",
          "G11 Validation posture — the run matches the posture you chose (strict in the example)",
          "G12 Sync contract — the cursor and primary key satisfy the sync mode",
          "G12S Staging policy — staging tables and their cleanup follow the workspace staging policy",
          "G13 Source column coverage — every source column is mapped or declared an intentional omission; an unaccounted column blocks instead of being dropped",
          "G16 Field reduction governance — every source column not carried over has a recorded reason, and a reason the sample disproves blocks",
          "G14, G15, G19, G20, G21, G22 — required columns, existing-table shape, schema replacement, code crosswalk, control totals, and referential integrity; they apply when the destination or contract calls for them",
          "G24R Reversibility — added when the route must stay reversible; blocks Execute if any column cannot give the source value back",
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
            body: "The summary names the blocking gate. In the example the first run stopped on **Create destination table**: `SALES_ORDERS_REPORTING` did not exist, and the card showed the `CREATE TABLE` statement Datawrap would run. Read the card and note whether the cause is mapping, credentials, capacity, encoding, DDL, or drift.",
            figure: {
              src: WORKSPACE_SHOT.transferValidateBlocked,
              alt: "Validate blocked: 1 blocking gate, Create destination table needs DDL approval",
              caption: "Validation blocked on one gate. The footer's primary action is Approve destination DDL.",
              markers: [
                { n: 1, label: "Validation blocked", x: "25%", y: "21%" },
                { n: 2, label: "Cannot execute yet", x: "40%", y: "34%" },
                { n: 3, label: "Approve destination DDL", x: "83%", y: "96%" },
              ],
            },
          },
          {
            title: "Remediate in Map or Advanced",
            pin: "Map · Destination → Advanced · Fix bad data",
            body: "Use the control the card points to. A DDL gate is cleared by approving the statement. A mapping or fidelity gate sends you to **Map** to remap a column or accept an intentional cast. A sync-contract gate sends you to **Destination → Advanced** to set a primary key or cursor. Encoding problems can be stripped or sent to quarantine.",
            figure: {
              src: WORKSPACE_SHOT.transferMapWhy,
              alt: "Map with the attributes row expanded, explaining the JSON to VARIANT mapping",
              caption: "On Map, expand a row to see why it was mapped that way before changing it.",
              markers: [
                { n: 1, label: "All approved", x: "22%", y: "24%" },
                { n: 2, label: "Why this mapping", x: "28%", y: "86%" },
              ],
            },
          },
          {
            title: "Re-run until Execute unlocks",
            pin: "Validate → Re-run Validate",
            body: "Click **Re-run Validate**. Execute unlocks only when the server returns **approve** with no blocked gate. In the example the second run showed 17 passed, 0 blocked, 6 skipped.",
            tip: "The unlock is decided by the server, not the browser. Editing the page cannot unlock Execute.",
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
          src: WORKSPACE_SHOT.transferMap,
          alt: "Map columns for sales_orders with destination types and Safe badges",
          caption: "Map: each source column with its destination name, type, transform, and status.",
          markers: [
            { n: 1, label: "Map columns", x: "20%", y: "19%" },
            { n: 2, label: "Destination type", x: "37%", y: "40%" },
            { n: 3, label: "Approve", x: "60%", y: "37%" },
          ],
        },
      },
      {
        id: "synonyms",
        title: "Synonym dictionary",
        body: "Accept or reject mapping suggestions. Accepted pairs enter your workspace synonym dictionary for future auto-maps.",
        figure: {
          src: WORKSPACE_SHOT.transferMapWhy,
          alt: "Map row for attributes expanded to show why it maps JSONB to VARIANT",
          caption: "Expand a row to see why it was mapped: here, attributes keeps every JSON value as VARIANT.",
        },
      },
      {
        id: "confidence",
        title: "Confidence scores",
        body: "Each pair has a confidence score, shown when you expand the row. Use the **Needs attention** and **Ready** tabs to focus on what needs a decision. Click **Approve** when a pair is right, remap it when it is not, and accept the risk only for a cast you mean to be lossy.",
      },
      {
        id: "drift",
        title: "Schema drift detection",
        body: "Datawrap compares current source and destination schemas against your saved route. Drift shows up in Validate, and a schedule parks with the change before its next run writes anything.",
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
        body: "The **Connectors** page is the inventory of every saved source and destination in your workspace. The filters at the top count **All**, **Healthy**, and **Errors**. The table shows each connection's capability, **Last test**, and **Last used**, so you know what is safe to schedule.\n\nUse the **Connections** tab for saved credentials. Use **Catalog** only when browsing available driver types — Catalog does not store secrets.",
        figure: {
          src: WORKSPACE_SHOT.connectors,
          alt: "Connectors page with eight demo connections, all Test passed",
          caption: "Connectors: the saved demo connections, each with capability, last test, and last used.",
          markers: [
            { n: 1, label: "Healthy / Errors", x: "71%", y: "11%" },
            { n: 2, label: "Connections", x: "35%", y: "30%" },
            { n: 3, label: "New connection", x: "91%", y: "11%" },
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
            body: "In the signed-in workspace, open **Platform → Connectors**. Confirm the **Connections** tab is selected when you are managing saved credentials (not **Catalog**).\n\nIf **Errors** is greater than zero, open those rows and re-run **Test** before you schedule anything against them.",
            figure: {
              src: WORKSPACE_SHOT.connectors,
              alt: "Sidebar with Connectors active and the Connections tab selected",
              caption: "Connectors in the sidebar, and the Connections tab next to Catalog.",
              markers: [
                { n: 1, label: "Connectors", x: "6%", y: "18.5%" },
                { n: 2, label: "Connections", x: "52%", y: "11%" },
              ],
            },
          },
          {
            title: "Click New connection — Choose a connector dialog",
            pin: "Top-right → New connection",
            body: "Click **New connection** (top-right). A modal dialog opens: **Choose a connector**.\n\nUse category chips (**Databases**, **Data Warehouses**, **Files & Formats**, …), search, and status filters (**Certified**, **Not yet proven**, **Transfer-ready**, **Source only**, **Test only**, **Planned**). Transfer-ready cards show badges such as **Full Transfer**.\n\nPick a driver — for example **PostgreSQL**, **MySQL**, **MongoDB**, **Snowflake**, **BigQuery**, **S3**, **CSV / TSV**, or a SQLAlchemy generic. Do not schedule **Planned** catalog entries in production.",
            figure: {
              src: WORKSPACE_SHOT.connectorsWizard,
              alt: "Choose a connector dialog with category chips, readiness filters, and connector cards",
              caption: "Choose a connector: filter by category and readiness, then pick a card marked Full Transfer.",
              markers: [
                { n: 1, label: "Choose a connector", x: "10%", y: "7%" },
                { n: 2, label: "Category chips", x: "30%", y: "18%" },
                { n: 3, label: "Readiness filters", x: "55%", y: "27%" },
                { n: 4, label: "Connector cards", x: "20%", y: "45%" },
              ],
            },
          },
          {
            title: "Enter credentials and Test",
            pin: "Wizard form → Test → Save",
            body: "After you pick a driver, the credential form opens (still in the dialog flow). Fill **host**, **database**, and auth (password, key, or cloud credentials). Click **Test** before **Save**. Do not skip Test.\n\n**Test passed** (green) on the Connections table means the driver reached the system, so it is safe to pick in Transfer Studio and Schedules.\n\n**Test failed** (red) means fix credentials, firewall, or region network access before using the connector. Failed connections block Validate gates later.",
            figure: {
              src: WORKSPACE_SHOT.connectors,
              alt: "Connections table with Last test results and a Test button on each row",
              caption: "After Save, the row shows Last test. Click Test on the row to run it again.",
              markers: [
                { n: 1, label: "Last test", x: "71%", y: "21%" },
                { n: 2, label: "Test", x: "91%", y: "21%" },
              ],
            },
            tip: "Credentials are encrypted at rest with workspace-scoped keys. Enterprise tenants can enable **BYOK** under **Settings → Enterprise**. Passwords never appear in job logs.",
          },
          {
            title: "Use it in Transfer Studio",
            pin: "Transfer → Source or Destination → Database",
            body: "Open **Platform → Transfer**. On **Source** (or later **Destination**), choose **Database** or **Cloud storage**. Your saved healthy connectors appear in the picker.\n\nContinue through **Map → Validate → Run**. A green **Test passed** on Connectors does **not** skip preflight — Validate still runs the full gate set before any production write.",
            figure: {
              src: WORKSPACE_SHOT.transferSource,
              alt: "Transfer Studio Source with Database selected and the saved Orders Postgres connection",
              caption: "Database selected, a saved connection picked, then Continue to Destination.",
              markers: [
                { n: 1, label: "Database", x: "33%", y: "19%" },
                { n: 2, label: "Saved connection", x: "15%", y: "34%" },
                { n: 3, label: "Continue", x: "90%", y: "95%" },
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
          "**Certified** — the CI jobs that prove this engine ran green with no skipped test; safe for production schedules after Test passed.",
          "**Implemented — not yet proven** — the driver runs transfers, but no green CI run proves its engine yet; not offered for production schedules.",
          "**Source only** — readable; no transfer-ready write path.",
          "**Test only** — auth works; write path is not transfer-ready.",
          "**Planned** — catalog presence only; do not schedule yet.",
        ],
      },
      {
        id: "native",
        title: "Native drivers & SQLAlchemy",
        body: "A connector is **Certified** only when the CI jobs that prove its engine ran green with no skipped test; an engine that runs but has no such run yet is labelled **Implemented — not yet proven** and is not offered for production schedules. The connector catalog shows each tile's tier. **Redshift** stays Planned until a named PRODUCTION_SKU matrix. Generic **SQLAlchemy** URLs extend reach with the same **Validate** preflight path used by native drivers. Catalog tiles are not transfer-live. Proofs → Integrity ledger has **Run desktop lab**: 80 catalog slots exercised as source and destination when the desktop backend is up. **Run unique-engine matrix** is the live src×dst cartesian (Postgres, MySQL, Mongo, SQL Server, Oracle, object stores, warehouses on emulators). Hosted twins (Neon/RDS/CNPG/OpenShift PostgreSQL) share a driver — that option is real, it is not 80 unique engines.",
      },
    ],
  },
  "help-pipelines": {
    id: "help-pipelines",
    slug: "pipelines",
    category: "Connectors",
    title: "Schedules and recurring sync",
    description: "Create a recurring sync, set cadence and sync mode, require reversibility, and read why a schedule parked, with screenshots from the hosted workspace.",
    readTime: "16 min",
    icon: "activity",
    sections: [
      {
        id: "overview",
        title: "What a schedule is",
        body: "A **schedule** runs a route you have already validated in Transfer Studio on a cadence. Each run is a full job: the same gates, quarantine, and read-back as a manual run. Recurring does not mean fewer checks.\n\nWhen a run hits a finding that needs a person, the schedule **parks** instead of retrying. The **Waiting on a decision** panel at the top of the page says what happened and gives one button to fix it. In the screenshot, `orders_feed` parked because upsert needs a business key, and the button is **Open Transfer Studio**.",
        figure: {
          src: WORKSPACE_SHOT.schedules,
          alt: "Schedules list with one schedule parked: upsert needs a business key",
          caption: "Schedules: four schedules, one parked on a finding rather than retrying it.",
          markers: [
            { n: 1, label: "Schedules", x: "6%", y: "37%" },
            { n: 2, label: "Waiting on a decision", x: "22%", y: "17.5%" },
            { n: 3, label: "Open Transfer Studio", x: "23%", y: "40%" },
            { n: 4, label: "New schedule", x: "91%", y: "11%" },
          ],
        },
        tip: "The simplest way to start is from Transfer Studio: after Validate approves a route, click **Schedule** in the footer. A schedule created on this page saves cadence only and stays paused until the route has an approved mapping.",
      },
      {
        id: "create-pipeline",
        title: "Procedure: create a schedule",
        procedure: true,
        body: "This path uses the **Create recurring sync** form for the Postgres to Snowflake route from the Transfer Studio guide.",
        workflow: [
          {
            title: "Open Create recurring sync",
            pin: "Sidebar → Operations → Schedules → New schedule",
            body: "Open **Operations → Schedules** and click **New schedule**. The page switches to the **Create recurring sync** form, with a **Creating schedule** chip at the top.\n\nFill in:\n\n1. **Schedule name** (example: `Nightly sales_orders → Snowflake`)\n2. **Source connector** and **Source table** (`Orders Postgres (demo)`, `sales_orders`)\n3. **Destination connector** and **Destination table** (`Warehouse Snowflake (demo)`, `SALES_ORDERS_REPORTING`)\n\nThe source can also be a **SQL query** or a **Procedure**. **Open Transfer Studio** takes the same route to Map and Validate if it has not been approved yet.",
            figure: {
              src: WORKSPACE_SHOT.schedulesCreate,
              alt: "Create recurring sync form with schedule name, source and destination connectors and tables",
              caption: "Create recurring sync: name, source and destination, and the cadence below.",
              markers: [
                { n: 1, label: "Create recurring sync", x: "21%", y: "17%" },
                { n: 2, label: "Schedule name", x: "25%", y: "25%" },
                { n: 3, label: "Source and destination", x: "40%", y: "42%" },
                { n: 4, label: "Open Transfer Studio", x: "89%", y: "35%" },
              ],
            },
            tip: "If a connector list is empty, add the connection under **Platform → Connectors** first. **Save schedule** stays disabled until both sides are chosen.",
          },
          {
            title: "Set the cadence and sync mode",
            pin: "Cadence → Preset | Cron  ·  Sync mode",
            body: "In **Cadence** choose **Preset** or **Cron**. The presets are **Hourly**, **Daily**, and **Weekly**. They count from the last run, so Daily means about 24 hours after the previous run, not a fixed time of day. For a fixed time use **Cron** with a timezone, for example `10 10 * * *` for 10:10 every day.\n\nIn **Sync mode** pick how rows move: **Full append**, **Full overwrite**, **Incremental append**, **Incremental deduped**, **CDC**, **SCD Type 2**, or **Mirror**. Which ones are offered depends on the two connectors. Upsert, SCD Type 2, and Mirror need a **Primary key**; incremental modes need a **Cursor column**.",
            figure: {
              src: WORKSPACE_SHOT.schedulesCadenceMode,
              alt: "Cadence presets Hourly, Daily, Weekly and the sync mode tiles",
              caption: "Cadence presets, the Preset / Cron switch, and the sync mode tiles.",
              markers: [
                { n: 1, label: "Preset / Cron", x: "91%", y: "22%" },
                { n: 2, label: "Daily", x: "56%", y: "33%" },
                { n: 3, label: "Sync mode", x: "20%", y: "55%" },
                { n: 4, label: "Primary key", x: "22%", y: "87%" },
              ],
            },
          },
          {
            title: "Require reversibility and set retries",
            pin: "Data contract & reversibility  ·  Retry & notifications",
            body: "Under **Data contract & reversibility** you can bind a signed contract and require it before each run, so an unattended run cannot write against a schema nobody signed. **Require reversibility before each run (fail-closed)** blocks any run where a column could not give the source value back, such as a hash, a mask, or a narrowing cast.\n\nUnder **Retry & notifications** set **Max retries**, **Retry backoff (seconds)**, and whether to **Notify on failure** and **Notify on success**. Retries apply to transient failures. A finding that needs a person parks the schedule instead.\n\nClick **Save schedule**. The toast says **Schedule created**, or **Schedule saved paused** if it cannot run yet.",
            figure: {
              src: WORKSPACE_SHOT.schedulesReversibilityRetries,
              alt: "Require reversibility before each run, retry settings, and the Save schedule button",
              caption: "Require reversibility, Max retries 2, backoff 30 seconds, notify on failure, then Save schedule.",
              markers: [
                { n: 1, label: "Require reversibility", x: "18%", y: "16%" },
                { n: 2, label: "Retry & notifications", x: "22%", y: "27%" },
                { n: 3, label: "Notify on failure", x: "93%", y: "47%" },
                { n: 4, label: "Save schedule", x: "90%", y: "65%" },
              ],
            },
          },
          {
            title: "Open the schedule",
            pin: "Click a schedule row → detail drawer",
            body: "Click a schedule to open its drawer. **Overview** shows the cadence, sync mode, contract, reversibility requirement, last and next run, and the numbers from the last run: mapped columns, rows changed, quarantined rows, and trust score.\n\nThe footer has **Run now**, **Re-sync (full)**, **Last job**, **Activate** or **Pause**, **Edit**, **Export YAML**, and **Delete**. **Edit** reopens the same form.",
            figure: {
              src: WORKSPACE_SHOT.schedulesDrawer,
              alt: "Schedule drawer for Orders feed: every 2 minutes, incremental deduped, 0 quarantined",
              caption: "Orders feed, every 2 minutes, incremental deduped. 6 columns mapped, 3 rows changed, 0 quarantined.",
              markers: [
                { n: 1, label: "Cadence", x: "62%", y: "13%" },
                { n: 2, label: "Sync mode", x: "87%", y: "13%" },
                { n: 3, label: "Quarantine", x: "60%", y: "54%" },
                { n: 4, label: "Run now", x: "55%", y: "92%" },
              ],
            },
          },
          {
            title: "Read the run history",
            pin: "Drawer → History",
            body: "**History** lists each run with its status, time, rows read and written, rows rejected, and the job ID. Copy the job ID or click **Last job** to open the run in **Job space**, where the Trust Report and Reversibility tabs show what that run wrote.\n\nSchedules owns the timing. Job space owns the evidence for each run.",
            figure: {
              src: WORKSPACE_SHOT.schedulesHistory,
              alt: "Schedule run history with completed runs, rows written, rows rejected, and job IDs",
              caption: "History: each run with rows written, rows rejected, and the job it created.",
              markers: [
                { n: 1, label: "Run history", x: "57%", y: "31%" },
                { n: 2, label: "Job ID", x: "87%", y: "47%" },
              ],
            },
            tip: "Turn on **Notify on failure** before you move a schedule to a production cadence.",
          },
        ],
      },
      {
        id: "incremental",
        title: "Sync modes",
        body: "Schedules offer the same sync modes as Transfer Studio. Pick the one that matches how the destination should treat repeats and deletes:",
        steps: [
          "**Full overwrite** — replace the target, then load the snapshot",
          "**Full append** — keep existing rows and append the full snapshot",
          "**Incremental append** — new rows only, by cursor",
          "**Incremental deduped** — cursor plus primary-key upserts",
          "**CDC** — log-based changes when the source supports it. Delivery is at-least-once upsert unless a route proves otherwise.",
          "**SCD Type 2** — versioned history (primary key required)",
          "**Mirror** — keys missing from the source are soft-deleted in the destination (`_deleted`), so the physical row count stays the same (primary key required)",
        ],
        tip: "Rows that fail a check go to quarantine. The rest of the batch still lands, and nothing is dropped quietly.",
      },
      {
        id: "gitops",
        title: "Schedules as YAML",
        body: "The Schedules toolbar has **Export YAML** and **Import YAML**, and a **Require signed** switch that refuses imported schedules unless their contract is signed. Keep the exported YAML in version control as the reviewable definition of cadence, route, and sync mode.",
      },
      {
        id: "redis-keys",
        title: "Redis destination keys",
        body: "Redis stores each row under `prefix:identity`. Datawrap prefers `id`, `*_id`, or `uuid`, then natural keys (`code`, `iso`, `name`, `sku`). Columns that are not unique, like `capital` or `city`, are never chosen when a stronger key exists. Set **Primary key** in the schedule's sync mode panel (or on Map for a one-off run) to choose the column yourself.",
      },
    ],
  },
  "help-contracts": {
    id: "help-contracts",
    slug: "contracts",
    category: "Connectors",
    title: "Data contracts",
    description: "Save a contract from Validate, sign it, and require it on a schedule so unattended runs fail closed.",
    readTime: "9 min",
    icon: "shield",
    sections: [
      {
        id: "overview",
        title: "What a contract is",
        body: "A **data contract** records what a route promises: the columns, their types and nullability, and the mappings that produce them. Contracts live under **Platform → Contracts**. A schedule can require a **signed** contract before each run, so an unattended run cannot write against a shape nobody agreed to.\n\nEach contract also has a **breaker**. Repeated failures open it and stop further runs until the cause is fixed; it closes again after a recovery timeout.",
        figure: {
          src: WORKSPACE_SHOT.contracts,
          alt: "Contracts page with one draft contract for Orders Postgres (demo) to DW_DEMO.SALES_ORDERS_REPORTING",
          caption: "Contracts: one draft, v1, 11 columns and 11 mappings, breaker closed.",
          markers: [
            { n: 1, label: "Contracts", x: "6%", y: "22%" },
            { n: 2, label: "Signed / Draft / Broken", x: "78%", y: "16%" },
            { n: 3, label: "Contract", x: "25%", y: "26%" },
            { n: 4, label: "Draft", x: "86%", y: "26%" },
          ],
        },
      },
      {
        id: "procedure",
        title: "Procedure: save, sign, and enforce a contract",
        procedure: true,
        body: "This path uses the contract saved from the Postgres to Snowflake route in the Transfer Studio guide.",
        workflow: [
          {
            title: "Save the contract from Validate",
            pin: "Transfer Studio → Validate → Save contract",
            body: "When Validate approves a route, the footer offers **Save contract**. It saves the approved columns and mappings as a **Draft**, version **v1**, under **Platform → Contracts**.",
          },
          {
            title: "Review it and sign",
            pin: "Contracts → contract row → drawer → Sign",
            body: "Open **Contracts** and click the contract. The drawer shows its status, version, source, and destination, with tabs for **Overview**, **Columns**, **Mappings**, **Quality**, and **History**. **Overview** includes the breaker state and the preflight gates the contract was saved with.\n\nCheck **Columns**: for this route, 11 columns from `order_id BIGINT` to `status VARCHAR`, with `unit_price` as `NUMBER(10,2)`. When the owners agree, click **Sign**. **Deprecate** retires a contract; **Export** downloads it.",
            figure: {
              src: WORKSPACE_SHOT.contractsColumns,
              alt: "Contract drawer Columns tab: 11 columns with types and nullability, and the Sign button",
              caption: "Contract drawer, Columns tab: the 11 promised columns with type and nullability.",
              markers: [
                { n: 1, label: "Status and version", x: "62%", y: "17%" },
                { n: 2, label: "Columns", x: "62%", y: "32%" },
                { n: 3, label: "Sign", x: "54%", y: "96%" },
              ],
            },
          },
          {
            title: "Require it on the schedule",
            pin: "Schedules → New schedule or Edit → Data contract & reversibility",
            body: "On the schedule form, open **Data contract & reversibility**. Pick the signed contract and turn on the requirement to have it signed before each run. If the contract is unsigned, missing, or the live schema has drifted from it, the run is refused and the schedule parks with the reason, the same way a failed gate would stop it.",
            figure: {
              src: WORKSPACE_SHOT.schedulesReversibilityRetries,
              alt: "Schedule form: Data contract & reversibility panel and Retry & notifications",
              caption: "Schedule form: fail-closed requirements for unattended runs, then retries and notifications.",
              markers: [
                { n: 1, label: "Fail-closed requirements", x: "18%", y: "16%" },
              ],
            },
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
    description: "Post-load SQL models that run at the destination after a transfer lands.",
    readTime: "6 min",
    icon: "sparkle",
    sections: [
      {
        id: "overview",
        title: "What Transforms are",
        body: "Open **Operations → Transforms**. A transform here is a set of SQL models that runs **at the destination after a transfer lands**, such as a reporting table rebuilt from the rows just loaded. Each row shows the destination, how many models it has, what triggers it, and whether it is enabled.\n\nThis is different from the **Transform** step in Transfer Studio, which changes rows before they are written.",
        figure: {
          src: WORKSPACE_SHOT.transforms,
          alt: "Transforms page with Revenue by country on Analytics Postgres (demo), enabled",
          caption: "Transforms: Revenue by country, one model on Analytics Postgres (demo), enabled.",
          markers: [
            { n: 1, label: "Transforms", x: "6%", y: "40%" },
            { n: 2, label: "Transform", x: "22%", y: "20%" },
            { n: 3, label: "New transform", x: "90%", y: "11%" },
          ],
        },
      },
      {
        id: "detail",
        title: "Open a transform",
        body: "Click a row to open it. The drawer shows the destination and schema, the trigger (**After transfer** runs it when the named tables land), the bound contract if any, the version, and the models with their build order.\n\nThe footer has **Dry run**, which checks the SQL without writing, **Run now**, **Export dbt**, **Edit**, and **Delete**.",
        figure: {
          src: WORKSPACE_SHOT.transformsDetail,
          alt: "Transform drawer for Revenue by country: trigger After transfer, model revenue_by_country, Dry run and Run now",
          caption: "Revenue by country: runs after transfer and builds revenue_by_country.",
          markers: [
            { n: 1, label: "Trigger", x: "78%", y: "20%" },
            { n: 2, label: "Models", x: "55%", y: "46%" },
            { n: 3, label: "Dry run", x: "57%", y: "96%" },
          ],
        },
        tip: "Use **Dry run** after any change to the source route. A model that refers to a renamed column fails here instead of in the next scheduled run.",
      },
      {
        id: "library",
        title: "Pre-load recipes live in the Library",
        body: "Rules that change rows before the write, such as renames, rounding, and filters, are kept in **Operations → Library** and applied on the Transfer Studio **Transform** step. Validate then checks the transformed rows, and Reversibility flags any step that changes values or drops rows.",
      },
    ],
  },
  "help-data-pilot": {
    id: "help-data-pilot",
    slug: "data-pilot",
    category: "Pilot & MCP",
    title: "Datawrap Pilot",
    description: "Ask about your data and workspace in plain language, and see the SQL behind every answer.",
    readTime: "7 min",
    icon: "sparkle",
    sections: [
      {
        id: "use",
        title: "When to use Pilot",
        body: "Open **Operations → Pilot**, or the **Pilot** tab on the right edge of any page. Ask a question about a table on a saved connection. Pilot builds one SQL statement, runs it on the server over the whole table, and shows the SQL and a result reference under the answer.\n\nIn the screenshot, Pilot counted 5,100 rows in `sales_orders` and then totalled `order_total` by region for 5 regions. Both answers say they are exact server-side aggregates, not samples.",
        figure: {
          src: WORKSPACE_SHOT.pilot,
          alt: "Pilot answering a row count and a total by region, with the SQL it ran",
          caption: "Pilot: a row count and a group-by on sales_orders, each with its SQL and a result reference.",
          markers: [
            { n: 1, label: "Pilot", x: "6%", y: "51%" },
            { n: 2, label: "Answer", x: "60%", y: "42%" },
            { n: 3, label: "SQL", x: "50%", y: "75%" },
            { n: 4, label: "Follow-ups", x: "60%", y: "83%" },
          ],
        },
      },
      {
        id: "handoff",
        title: "Changes need your confirmation",
        body: "Pilot can start work as well as answer questions. Anything that would create a connection, start a transfer, or change a schedule becomes a request you confirm first. After you confirm, it runs through the same Map and Validate gates as Transfer Studio.",
      },
      {
        id: "examples",
        title: "Example prompts",
        body: "Name the table and the connection. Questions that can be answered with one aggregate work best.",
        steps: [
          "How many rows are in sales_orders on Orders Postgres (demo)?",
          "Total order_total by region in sales_orders on Orders Postgres (demo)",
          "Summarize that",
          "What should I do next?",
        ],
        tip: "With the local engine, Pilot handles counts, totals, group-bys, and filters. Open-ended prompts like a full workspace briefing need a configured model under **Settings → AI Models**.",
      },
    ],
  },
  "help-mcp": {
    id: "help-mcp",
    slug: "mcp",
    category: "Pilot & MCP",
    title: "MCP Server for agents",
    description: "Connect Cursor, Claude, VS Code, or any MCP client to the same gates the UI uses.",
    readTime: "9 min",
    icon: "zap",
    sections: [
      {
        id: "tools",
        title: "Open the MCP page",
        body: "Open **System → MCP**. The page shows the **MCP server URL** with its status, setup for **Cursor**, **Claude Desktop**, **VS Code**, a custom GPT, and any other client that speaks MCP over streamable HTTP, and a **Recent activity** list of agent calls. Agents never see connection passwords; they work through the same roles as people.",
        figure: {
          src: WORKSPACE_SHOT.mcp,
          alt: "MCP Server page: server URL online, client setup cards, and recent activity",
          caption: "System → MCP: the server URL, client setup, and recent agent calls.",
          markers: [
            { n: 1, label: "MCP", x: "6%", y: "62%" },
            { n: 2, label: "Server URL", x: "30%", y: "12%" },
            { n: 3, label: "Client setup", x: "30%", y: "40%" },
            { n: 4, label: "Recent activity", x: "62%", y: "31%" },
          ],
        },
      },
      {
        id: "setup",
        title: "Procedure: connect Cursor",
        procedure: true,
        body: "Copy the snippet from the MCP page (or use the JSON below) and authenticate with a workspace API key from **Settings → API Keys**.",
        workflow: [
          {
            title: "Copy the MCP server URL",
            pin: "System → MCP → Cursor",
            body: "Click **Copy MCP URL**, or open the **Cursor** card and copy the `mcpServers.datawrap.url` value shown for your workspace.",
          },
          {
            title: "Paste into Cursor Settings → MCP",
            pin: "Cursor → Settings → MCP",
            body: "Add the server entry from **System → MCP**. The URL is absolute (`https://<the host you are signed into>/api/v1/mcp`) and the workspace API key is the `Authorization: Bearer` header. Restart MCP if prompted. `create_connector`, `start_transfer`, and `create_schedule` return an `ack_id` and do not move data until `confirm_action`. Every confirmed transfer appears in **Job space** with an audit trail. The key's role works like a person's: a Viewer key gets read-only tools, an Operator key can also run and cancel jobs, an Editor key gets everything except deleting connections, and an Admin key gets every tool.",
          },
        ],
        code: '{\n  "mcpServers": {\n    "datawrap": {\n      "url": "https://<tenant-host>/api/v1/mcp",\n      "headers": { "Authorization": "Bearer <workspace-api-key>" }\n    }\n  }\n}',
      },
      {
        id: "security",
        title: "Agent security",
        body: "MCP tools follow workspace roles. Agents cannot read connection secrets; they can only start the operations their key allows. Anything that creates or moves data waits for `confirm_action`, and agent runs pass Validate before they write, like every other run.",
      },
    ],
  },
  "help-query": {
    id: "help-query",
    slug: "query-playground",
    category: "Pilot & MCP",
    title: "Query Playground",
    description: "Read-only SQL on saved connections, for checking data before and after a load.",
    readTime: "6 min",
    icon: "code",
    sections: [
      {
        id: "dialects",
        title: "Open Query Playground",
        body: "Open **Operations → Query** and pick a saved connection. The schema browser lists tables and columns, the editor knows the connection's SQL dialect, and **Row limit** caps the result (1,000 by default). Each tab keeps its own connection, so you can query the source and the destination side by side.\n\nThe screenshot totals paid, shipped, and delivered orders by region on **Warehouse Snowflake (demo)**: 5 rows in 3.1 seconds, with LATAM at 767 orders and 182,648.65 in revenue. The same query on the Postgres source returns the same numbers. Kafka topics and vector stores are not queried here.",
        figure: {
          src: WORKSPACE_SHOT.query,
          alt: "Query on Warehouse Snowflake (demo): revenue by region, 5 rows in 3.1 seconds",
          caption: "Query: revenue by region on the Snowflake copy, with a tab open for the Postgres source.",
          markers: [
            { n: 1, label: "Query", x: "6%", y: "48%" },
            { n: 2, label: "Connector", x: "30%", y: "18%" },
            { n: 3, label: "Row limit", x: "66%", y: "18%" },
            { n: 4, label: "Result", x: "65%", y: "62%" },
          ],
        },
      },
      {
        id: "tips",
        title: "Tips",
        body: "Query is **read-only**. Only SELECT, WITH, EXPLAIN, SHOW, DESCRIBE, and similar statements are accepted; DML, DDL, and CALL or EXEC are refused before they reach the database. On PostgreSQL, MySQL, SQLite, and Oracle the statement also runs in a read-only transaction. Results can be filtered and copied as CSV, JSON, or Markdown. Writes go through Transfer Studio or Schedules.",
        figure: {
          src: WORKSPACE_SHOT.queryReadonly,
          alt: "Query refusing a DELETE statement: only read and metadata queries are allowed",
          caption: "A DELETE is refused with the reason, and nothing is sent to the database.",
          markers: [
            { n: 1, label: "Refused statement", x: "45%", y: "28%" },
            { n: 2, label: "Query failed", x: "45%", y: "55%" },
          ],
        },
      },
      {
        id: "transfer-bridge",
        title: "Bridge to Transfer Studio",
        body: "Query does not move data. To load the result of a query, open **Transfer**, pick the same connection, and choose **SQL query** as the source extract, then paste the SELECT. **Stored procedure** runs one CALL or EXEC, spooled once. On **Destination**, **Query** runs one INSERT, MERGE, or UPDATE with binds and **Stored procedure** runs one CALL per row. Failed destination statements go to quarantine. CDC, SCD Type 2, and Mirror refuse callable destination writes. A query that worked here still goes through Validate.",
      },
    ],
  },
  "help-job-theater": {
    id: "help-job-theater",
    slug: "job-theater",
    category: "Operations",
    title: "Job space, Trust Report, and reversibility",
    description: "Open a run in Job space, read the Trust Report and the Reversibility tab, sign off, and work through quarantine.",
    readTime: "10 min",
    icon: "jobs",
    sections: [
      {
        id: "open",
        title: "Open Job space",
        body: "Go to **Operations → Jobs**. The list shows each run's route, sync mode, rows, verification, and time. Filter by status, source, destination, mode, or verification, or search by table. Click a run to open it. The tabs are **Detail**, **Mapping**, **Trust Report**, **Reversibility**, and **Log**, plus **Quarantine** when rows were held back.",
        figure: {
          src: WORKSPACE_SHOT.jobs,
          alt: "Jobs list filtered to orders, with the sales_orders to Snowflake run at the top: 5,100 rows, Verified",
          caption: "Jobs: the sales_orders → DW_DEMO run at the top, Full append, 5,100 rows, Verified.",
          markers: [
            { n: 1, label: "Jobs", x: "6%", y: "33%" },
            { n: 2, label: "Search", x: "25%", y: "25%" },
            { n: 3, label: "Run", x: "28%", y: "44%" },
            { n: 4, label: "Verified", x: "78%", y: "44%" },
          ],
        },
      },
      {
        id: "trust",
        title: "Trust Report",
        body: "The Trust Report reads both systems after the write. For the Snowflake run: 5,100 rows in the source, 5,100 in the destination, full checksum, Match. Under **How sure is this?** every row was compared.\n\nDelivery is at-least-once, and CDC is at-least-once upsert unless a route proves otherwise.",
        figure: {
          src: WORKSPACE_SHOT.jobTrust,
          alt: "Trust Report: 5,100 in source, 5,100 in destination, full checksum Match",
          caption: "Trust Report for the Snowflake run: 5,100 on each side, full checksum Match.",
          markers: [
            { n: 1, label: "Proven equal", x: "57%", y: "32%" },
            { n: 2, label: "Rows in source", x: "82%", y: "45%" },
            { n: 3, label: "Checksum", x: "82%", y: "62%" },
            { n: 4, label: "Match", x: "91%", y: "87%" },
          ],
        },
      },
      {
        id: "reversibility",
        title: "Reversibility",
        body: "The **Reversibility** tab answers whether the destination can give the source values back. On the Snowflake run all 11 columns are invertible. `order_id` keeps `BIGINT` and the text columns widen to `VARCHAR`, so the rollback is **Restore from destination**.",
        figure: {
          src: WORKSPACE_SHOT.jobReversibilityColumns,
          alt: "Reversibility tab for the Snowflake run: all 11 columns invertible",
          caption: "Snowflake run: every column invertible, rollback by restoring from the destination.",
          markers: [
            { n: 1, label: "Reversibility", x: "79%", y: "26%" },
            { n: 2, label: "11 invertible", x: "57%", y: "33%" },
            { n: 3, label: "Per column", x: "92%", y: "45%" },
          ],
        },
      },
      {
        id: "signoff",
        title: "Procedure: sign off a run",
        procedure: true,
        body: "Sign-off records who accepted the evidence for a run.",
        workflow: [
          {
            title: "Check the evidence",
            pin: "Trust Report → Evidence behind this report",
            body: "Scroll the Trust Report to **Evidence behind this report**. For the Snowflake run it shows reconciliation **Passed**, assurance **Full checksum**, and trust score **100**, plus whether a protection or transform proof applied. **Open Gate-8 proof** shows the read-back itself.",
            figure: {
              src: WORKSPACE_SHOT.jobTrustEvidence,
              alt: "Evidence behind the Trust Report: reconciliation passed, full checksum, trust score 100",
              caption: "Evidence: reconciliation passed, full checksum, trust score 100.",
              markers: [
                { n: 1, label: "Differences", x: "58%", y: "34%" },
                { n: 2, label: "Evidence", x: "60%", y: "61%" },
                { n: 3, label: "Sign-off", x: "60%", y: "86%" },
              ],
            },
          },
          {
            title: "Sign for your role",
            pin: "Go to sign-off → Sign as …",
            body: "Click **Go to sign-off**. **Engineering**, **Data owner**, and **Compliance** each sign the same report version. Choose your role, add a note on what you checked, and click **Sign as** your role. Each approval goes to the audit log. Once every required role has signed, the pack is signed (HMAC-SHA256) with this workspace's evidence signing key, which is separate from the login secret; the pack records the key id. **Download report (.md)** saves it for the audit file. Anyone with access to this workspace can verify the signed pack (Verify, or `POST /api/v1/transfer/proof-pack/verify`). Only this deployment holds the key: there is no public or offline verifier yet.",
            figure: {
              src: WORKSPACE_SHOT.jobSignoff,
              alt: "Sign-off panel with Engineering, Data owner, and Compliance waiting, and Sign as Engineering",
              caption: "Sign-off: three roles waiting, a note, and Sign as Engineering.",
              markers: [
                { n: 1, label: "Roles", x: "92%", y: "35%" },
                { n: 2, label: "Note", x: "70%", y: "58%" },
                { n: 3, label: "Sign as Engineering", x: "60%", y: "65%" },
                { n: 4, label: "Download report", x: "62%", y: "91%" },
              ],
            },
          },
        ],
      },
      {
        id: "quarantine-ui",
        title: "Procedure: work through quarantine",
        procedure: true,
        body: "Quarantined rows were held back with the column, value, and reason. They were not dropped.",
        workflow: [
          {
            title: "Open the job",
            pin: "Operations → Jobs → select run",
            body: "A run with held-back rows shows a non-zero rejected count and opens on the **Quarantine** tab.",
          },
          {
            title: "Read each row",
            pin: "Job detail → Quarantine",
            body: "Each entry shows the column, the value, the rule it failed, and when. Export the list if someone else needs to fix the source.",
          },
          {
            title: "Fix and replay",
            pin: "Fix the route → Replay or Resume",
            body: "Fix the map or the source, then replay the quarantined rows into the same destination, or resume the run from its checkpoint. A schedule fails the same way until the route is fixed.",
            tip: "A run that completed with quarantine still wrote the clean rows. The Trust Report counts the held-back rows, so they are never missing from the totals.",
          },
        ],
      },
    ],
  },

  "help-exit-readiness": {
    id: "help-exit-readiness",
    slug: "exit-readiness",
    category: "Operations",
    title: "Reversibility and exit readiness",
    description: "What the Reversibility analysis checks, what its rollback posture means, and how exit rehearsals prove you can take a system's data out.",
    readTime: "6 min",
    icon: "shield",
    sections: [
      {
        id: "what",
        title: "Two features, no automatic rollback",
        body: "**Reversibility** analyses a transfer's mapping: for each column, could the source value be recovered from the destination? **Exit readiness** registers the systems you would have to take out and **rehearses** their exit with real extracts. Neither moves data back on its own: Datawrap does not undo committed rows, run a reverse transfer, or fail back to the source.",
      },
      {
        id: "reversibility",
        title: "Reversibility",
        body: "Each mapped column is **invertible** (same type, rename, wider column, lossless cast), **lossy** (hash_pii, mask_pii, redact, an omitted or dropped column, a narrower type, rounding, a dropped time zone), or **conditional** (a cast out of text or a normaliser such as trim, which round-trips only for some values). Conditional columns are checked against the rows Validate sampled: a column that fits them is invertible for the data seen, not proven for rows that were not sampled. Turn on **Require reversibility** on the plan and the G24R gate blocks a plan, a run or a schedule that is not reversible, before any row is read.",
      },
      {
        id: "rollback",
        title: "What the rollback posture means",
        body: "A reversible table reads **Restore from destination**: the destination holds what is needed to rebuild the source. Otherwise it reads **Keep source as fallback**: keep the source until sign-off. This is advice. The run's rollback plan is a document-only runbook. The Job Reversibility tab is recomputed from the run's stored request; it does not show whether this run's data actually round-tripped.",
      },
      {
        id: "rehearsals",
        title: "Exit readiness and rehearsals",
        body: "Open **Platform → Exit readiness**. Declare a system: a saved source connection, the tables to take out, the exit window and what it is based on, any regulatory dates, how often to rehearse, and the sign-off roles. **Run rehearsal** runs one real extract per table to a file, then checks each table on both sides: row counts, a digest of the keys and of every row, and the sum, minimum and maximum of every numeric column. A table is **proven** only when all of these match and the extract file's SHA-256 was confirmed on read. The extracts are sealed with a Merkle root, and the duration is measured against the exit window.",
        steps: [
          "Proven: every declared table measured on both sides and equal",
          "Incomplete: something measured differs (a live source that changes during the rehearsal reads incomplete)",
          "Not proven: nothing differs, but something could not be measured (no key, a non-SQL source, a value check that was only sampled)",
          "Failed: an extract failed",
        ],
      },
      {
        id: "signoff",
        title: "Sign-off and the evidence pack",
        body: "Only a proven rehearsal can be signed. Each role signs once, bound to real authority in the workspace. When the last role signs, the Exit Evidence Pack is signed (HMAC-SHA256) with this workspace's evidence signing key, separate from login, and its digest is anchored when the deployment configures S3 Object Lock; otherwise it reads **not anchored**. Anyone with access to the workspace can verify the pack (`POST /api/v1/exit/evidence/verify`). Only this deployment holds the key, so there is no public or offline verifier yet.",
      },
      {
        id: "limits",
        title: "Limits",
        body: "Rehearsal completeness is measured for SQL sources only: a MongoDB, file, object-store or SaaS source cannot be proven. SQL Server, Oracle, Snowflake and BigQuery have no live-engine tests for these features. The full description is in docs/use/exit-readiness.md.",
      },
    ],
  },
  "help-enterprise": {
    id: "help-enterprise",
    slug: "enterprise",
    category: "Enterprise",
    title: "Enterprise controls",
    description: "SSO, Team RBAC, BYOK, region pinning, audit — configured in Settings by workspace admins.",
    readTime: "12 min",
    icon: "shield",
    sections: [
      {
        id: "where",
        title: "Where enterprise settings live",
        body: "Signed-in **admins** open **System → Settings**. Tabs include General, Security, Enterprise, SSO, Team, Notifications, AI Models, API Keys, and Audit Logs. The top-bar workspace switcher (and the picker on Team / Enterprise / Notifications) is one live workspace — switching it reloads each tab. Day-to-day operators usually stay on Connectors, Transfer, Schedules, and Jobs.",
        figure: {
          src: WORKSPACE_SHOT.settings,
          alt: "Settings, General tab, with the list of settings tabs on the left",
          caption: "System → Settings: General, Security, Enterprise, SSO, Team, Notifications, AI Models, API Keys, and Audit Logs.",
          markers: [
            { n: 1, label: "Settings", x: "6%", y: "59%" },
            { n: 2, label: "Settings tabs", x: "13%", y: "35%" },
            { n: 3, label: "Organization profile", x: "35%", y: "35%" },
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
              src: WORKSPACE_SHOT.settingsSso,
              alt: "Settings SSO tab with SAML 2.0, OpenID Connect, and Azure AD, SCIM provisioning, and group mappings",
              caption: "SSO: SAML 2.0, OpenID Connect, or Azure AD, then SCIM provisioning and group mappings.",
              markers: [
                { n: 1, label: "Identity providers", x: "62%", y: "35%" },
                { n: 2, label: "SCIM", x: "45%", y: "57%" },
                { n: 3, label: "Group mappings", x: "45%", y: "72%" },
              ],
            },
          },
          {
            title: "Test, then Save",
            pin: "SSO → Test · Save",
            body: "Run Test login with a pilot user. On success you see SSO ready. Save to enforce SSO for the tenant.",
            tip: "Keep a break-glass admin account path documented with IT before enforcing SSO-only.",
          },
        ],
      },
      {
        id: "team",
        title: "Team roles",
        body: "Open **Settings → Team** to invite members by email and assign one of four roles (Viewer, Operator, Editor, Admin). The workspace picker on this tab is the same live workspace as the top bar — member lists refresh while the tab is open.",
        steps: [
          "Viewer — inspect Jobs, proof, and connectors (no execute)",
          "Editor — create connectors, run Transfer Studio and Schedules (an Admin manages members)",
          "Admin — SSO, BYOK, Enterprise tab, API keys, grant Admin roles",
        ],
      },
      {
        id: "byok-region",
        title: "BYOK, region, and audit",
        body: "Settings → Enterprise sets tenant name, custom domain, data region, session timeout, and an IP allowlist for the custom domain. Optional BYOK wraps connector secrets with your KMS key. Settings → Audit Logs lists mapping decisions, job runs, quarantine events, and MCP calls for SOC 2 / GDPR / HIPAA review. Settings → Security downloads the Security posture report for procurement.",
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
        body: "Bearer tokens scoped to workspace. `$TENANT_ORIGIN` is the host in the browser address bar (the path is `/api/v1`). A workspace API key from **Settings → API Keys** is the same credential MCP uses. Its role decides what it may change. Enterprise uses SSO-backed service accounts.",
        code: 'curl -H "Authorization: Bearer $TOKEN" "$TENANT_ORIGIN/api/v1/connectors"',
      },
      {
        id: "endpoints",
        title: "Core endpoints",
        body: "Catalog, preflight, run, and job status endpoints mirror Transfer Studio behavior.",
        steps: [
          "Canonical prefix is /api/v1 — see docs/API_VERSIONING.md for deprecation policy",
          "GET /api/v1/connectors — list with transfer-ready status",
          "POST /api/v1/preflight — run the nine core gates plus the policy gates the route needs",
          "POST /api/v1/transfer/run — execute governed load",
          "GET /api/v1/connectors/jobs/{id} — status, quarantine, reconciliation",
        ],
        figure: {
          src: WORKSPACE_SHOT.jobs,
          alt: "Jobs list where API-started runs appear next to Studio runs",
          caption: "Runs started from the API appear in Jobs next to Studio runs, with the same Trust Report.",
        },
      },
      {
        id: "webhooks",
        title: "Webhooks",
        body: "Subscribe to job.completed, job.failed, and pipeline.quarantine_threshold (schedule) events. Payloads include job ID, gate results, and reconciliation summary — no row payloads unless explicitly configured.",
        code: 'POST /api/v1/webhooks\n{ "url": "https://your.app/hooks/datawrap", "events": ["job.completed"] }',
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
          src: WORKSPACE_SHOT.jobs,
          alt: "Jobs list with runs marked Verified, Verified (partial), and Failed",
          caption: "Quarantine and proof live on the job. Open the run in Job space to see held-back rows.",
        },
      },
      {
        id: "q2",
        title: "Who can run transfers?",
        body: "Editors and Admins can create connectors, plan transfers, and manage Schedules. Operators run and repair pipelines but do not design them. Viewers can open Jobs and read Trust Reports and quarantine. Roles are assigned under Settings → Team (Viewer, Operator, Editor, Admin — there is no Owner role) and can come from IdP group claims when SSO is configured.",
      },
      {
        id: "q3",
        title: "Do schedules skip preflight?",
        body: "No. Every scheduled run uses the same Validate gates and post-load read-back as a manual transfer. A failing gate blocks the write, and a finding that needs a person parks the schedule with the reason until the route is fixed.",
      },
      {
        id: "q3b",
        title: "How is data protected in transit and at rest?",
        body: "Connectors use TLS to sources and destinations. Secrets are encrypted at rest (optional customer BYOK). Quarantine replaces silent drops; post-load checksums prove what landed. Download the Security posture report from Settings → Security for procurement questionnaires.",
      },
      {
        id: "q4",
        title: "How is Datawrap different from ETL scripts?",
        body: "Columns are mapped by meaning with a confidence score, Validate stops a bad load before it writes, bad rows go to quarantine instead of being coerced quietly, and the destination is read back and compared after every load. SSO, roles, and audit logs come built in.",
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
