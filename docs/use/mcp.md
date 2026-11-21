# MCP server

Datawrap exposes its tools over the Model Context Protocol (MCP), so an MCP
client such as an AI coding assistant can read your workspace and stage
actions in it. The tools are the same ones Pilot uses, with the same
permissions.

## Connecting

| Transport | Path | Use it for |
|---|---|---|
| MCP Streamable HTTP (JSON-RPC) | `/api/v1/mcp` | MCP clients |
| REST bridge | `GET /api/v1/mcp/manifest`, `GET /api/v1/mcp/tools`, `POST /api/v1/mcp/tools/call` | Custom integrations |

A client is configured with the server URL, for example:

```json
{"mcpServers": {"datawrap": {"url": "https://<your-api-host>/api/v1/mcp"}}}
```

`GET /api/v1/mcp/status` reports whether the server is available.

## Authentication

- Send `Authorization: Bearer <token>`, where the token is a workspace API key
  or a session token.
- Discovery needs no token: the handshake, `/mcp/tools`, `/mcp/manifest` and
  `/mcp/status` answer anonymous callers and list every tool's name and
  schema. This is by design so clients can discover the server.
- Running a tool needs a token. Without one the server answers 401 with a
  `WWW-Authenticate` challenge.
- OAuth resource metadata is published at
  `/.well-known/oauth-protected-resource`. The issuer and audience come from
  `DATAWRAP_MCP_OAUTH_ISSUER` and `DATAWRAP_MCP_OAUTH_AUDIENCE`.
- When sign-in is turned off for the whole deployment (local development),
  tools can be called without a token.

## Permissions

Each tool needs one permission, the same permission as the API route that does
the same thing. The caller's role decides which tools it may run (see the role
table in [../operate/security.md](../operate/security.md)).

- A tool with no entry in the permission table is admin-only, so a new tool
  cannot ship ungated by omission.
- An API key acts with the role stored on the key, and never with more than
  the person who created it holds today.
- An administrator can turn MCP off for the organisation, or restrict it to a
  list of tools, with `GET` and `PUT /api/v1/mcp/policy` (needs
  `workspace.manage`).
- Calls are rate limited (`DATAFLOW_MCP_RATE_*` settings).
- Recent calls are listed at `GET /api/v1/mcp/logs`. The log is a file on the
  API host, so each replica has its own.

## The confirmation model

Tools fall into three groups:

| Effect | Meaning |
|---|---|
| read | Reads workspace state or product knowledge. Changes nothing. |
| plan | Reads the source and produces a proposal. Changes nothing. |
| mutate | Would change something. **It does not act when called.** |

A mutating tool **stages** the action and returns an `ack_id` with a safe
preview. Nothing has happened yet. The action runs only when `confirm_action`
is called with that `ack_id`.

1. Call the staging tool, for example `start_transfer`. You get an `ack_id`.
2. A person reviews the preview and confirms it in the product (Pilot's
   Confirm button), signed in. Under the default confirm policy the MCP
   client cannot confirm it itself (see below).
3. Only in a workspace whose confirm policy is `agent` may the client call
   `confirm_action` with the `ack_id` (and an optional `reason`, stored with
   the approval).

Properties of an approval:

- It expires 15 minutes after staging.
- It is used once. Confirming a used `ack_id` again returns the original
  result and does not run the action twice.
- At confirm time the permission for that specific action is checked again
  against the confirming person.
- A refused confirmation leaves the approval pending and is recorded in the
  audit log (`pilot.confirm.refused_channel`).
- Secrets in a staged connector stay on the server. The client receives only
  the `ack_id` and the preview.

### Who may confirm (confirm policy)

Before this policy existed, any caller who could read the workspace and do
the staged kind of change could confirm, on any channel. An MCP client that
approves its own tool calls automatically therefore removed the human
approval. Confirmation now has to come from the channel the workspace's
confirm policy names. It is set per workspace, with a default, in the MCP
policy (`PUT /api/v1/mcp/policy`, field `confirm`:
`{"default": "<mode>", "workspaces": {"<workspace id>": "<mode>"}}`):

| Mode | Who may confirm |
|---|---|
| `interactive` (default) | A person signed in interactively (browser session or sign-in token): the person who staged it, or a workspace admin. Never through the MCP endpoint, never with a workspace API key or MCP OAuth token. |
| `four_eyes` | As `interactive`, and the confirmer must be a different person from the one who staged it and hold workspace admin (`workspace.manage`). |
| `agent` | Any caller with the staged action's permission, on any channel, including `confirm_action` over MCP. The previous behaviour: use only for unattended automation, where there is no human approval. |

- An unknown value, or a policy store that cannot be read, means
  `interactive`: the check never falls open.
- An API key stages as the person who created it, so that person confirms
  from their own signed-in session.
- The `confirm` part of the policy can only be changed from an interactive
  session; an API key or MCP token (the agent it governs) gets 403.
- A change merges into the stored policy. `default` and other workspaces'
  entries need a platform admin; a workspace admin may set (or clear, with
  `null`) only their own workspace's entry. The audit event
  `mcp.policy.updated` records the old and new policy.
- With platform authentication off (`DATAFLOW_REQUIRE_AUTH=0`) there is no
  identity to bind: only the MCP channel is refused.
- Not covered: a person who copies their own sign-in token into an agent has
  delegated their session. Sign-in tokens expire and the workspace idle
  policy revokes them; use `four_eyes` where a second person is required.

Give MCP clients an API key with the lowest role that does the job.

## Tools

The table is generated from the tool registry. Regenerate with
`python docs/tools/gen_mcp_tools.py`.

<!-- BEGIN GENERATED: mcp-tools (docs/tools/gen_mcp_tools.py) -->

71 tools: 45 read, 3 plan, 23 change something.

| Tool | Permission | Effect | Staged for Confirm | What it does |
|---|---|---|---|---|
| `aggregate_data` | `query.use` | read | no | Answer an analytics question with an exact server-side aggregate: row counts, SUM/AVG/MIN/MAX, COUNT(DISTINCT), and GROUP BY / top-N breakdowns. |
| `analyze_dataset` | `workspace.read` | read | no | Deep analysis of a dataset: columns, types, PII, quality, samples. |
| `analyze_result` | `query.use` | read | no | Profile a previously sampled/queried result by result_id (or the latest result in this Pilot session). |
| `brief_workspace` | `workspace.read` | read | no | Spoken sitrep of this workspace: connectors (tested/failed), recent jobs, pipelines due or parked on approval, contracts. |
| `check_reversibility` | `job.plan` | read | no | Whether a transfer plan's (or a job's) destination can be turned back into the source: the verdict per table (reversible / partly / not reversible)... |
| `compare_connectors` | `connector.read` | read | no | Compare two saved connectors side by side on engine, endpoint, connection health, transfer-readiness and live object count. |
| `compare_datasets` | `workspace.read` | read | no | Compare schemas of two datasets side by side. |
| `confirm_action` | `workspace.read` | read | no | Consume a pending approval (ack_id) and perform the mutation the operator already staged: create_connector, start_transfer, start_dataset_transfer,... |
| `describe_connector_transforms` | `workspace.read` | read | no | List every column transform the engine applies — exact semantics, deterministic flag, and lossy flag per op, plus accepted aliases and parameterize... |
| `describe_pilot` | `workspace.read` | read | no | Explain what Datawrap Pilot knows and can do locally — capabilities, not raw RAG dumps. |
| `diff_schemas` | `connector.read` | read | no | Diff two live connector schemas (source table vs dest table) using classify_schema_change — additive vs breaking. |
| `explain_exit_rehearsal` | `job.read` | read | no | Explain one exit rehearsal: its verdict, every table's completeness (row counts, key-set and row digests, control totals, value parity method), the... |
| `explain_mapping_assurance` | `workspace.read` | read | no | Explain the schema mapping algorithms, confidence scoring, review rules, and guarantees. |
| `explain_parity` | `job.read` | read | no | Explain whether a finished job's destination matches its source: the parity verdict, how sure it is, every difference with its deterministic cause,... |
| `explain_product` | `workspace.read` | read | no | Answer product how-to questions about Datawrap (transfer, mapping, preflight, connectors, PII, troubleshooting) from curated local knowledge — not... |
| `filter_result` | `query.use` | read | no | Filter rows in a stored Pilot result (eq/ne/contains/gt/lt/is_null/…). |
| `get_exit_readiness` | `job.read` | read | no | Exit readiness of this workspace: the declared systems (what would have to be taken out), which have a proven exit rehearsal, which rehearsals are... |
| `get_job` | `job.read` | read | no | Fetch a transfer job by exact ID (24-char hex ObjectId or job_id string). |
| `get_preflight_run` | `job.read` | read | no | Look up a validation/preflight run by ID (pf_…) — blockers, gates, remediations. |
| `get_schedule` | `schedule.read` | read | no | Fetch one pipeline schedule by id or name, including route, sync mode, and bound contract / breaker when one is set. |
| `get_transfer_capabilities` | `workspace.read` | read | no | Show supported source→destination transfer combinations. |
| `inspect_schema_policy` | `connector.read` | read | no | Inspect schema drift policy for added, removed, renamed, or type-changed fields and streams. |
| `introspect_connector_schema` | `connector.read` | read | no | Live-introspect columns and types for a table/collection on a saved connector. |
| `list_cdc_captures` | `workspace.manage` | read | no | List the replication slots and publications on a saved PostgreSQL source: which ones Datawrap created, the CDC schedules using each, whether the ca... |
| `list_cdc_mapping_reviews` | `job.read` | read | no | List the CDC mapping reviews on a saved source: tables whose schema changed while CDC was reading them. |
| `list_connector_objects` | `connector.read` | read | no | List live tables/collections on a saved database connector (Postgres, MySQL, Mongo, Snowflake, …). |
| `list_connectors` | `connector.read` | read | no | List saved database/warehouse connectors. |
| `list_contracts` | `job.read` | read | no | List data contracts. |
| `list_datasets` | `workspace.read` | read | no | List the datasets available — this workspace's uploaded files and the built-in industry templates. |
| `list_jobs` | `job.read` | read | no | List transfer jobs with status, IDs, and record counts, newest first. |
| `list_schedules` | `schedule.read` | read | no | List pipeline schedules (Pipelines page) with cadence, next run, last status, and bound contract / breaker when one is set. |
| `map_connector_schemas` | `connector.read` | read | no | Live-introspect source and destination tables on saved connectors, then run Datawrap's semantic column mapper (same engine as Transfer Studio Map s... |
| `navigate` | `workspace.read` | read | no | Navigate the user to an app screen: dashboard, pilot, transfer, connectors, jobs, schedules (pipelines), contracts, query, mcp, settings, docs, ben... |
| `open_job` | `job.read` | read | no | Open the Jobs screen focused on a specific job id (safe navigate + highlight). |
| `open_schedule` | `schedule.read` | read | no | Open Pipelines focused on a schedule id or name. |
| `profile_quality_rules` | `workspace.read` | read | no | Generate quality, PII, type, nullability, uniqueness, and reconciliation checks for a dataset. |
| `rank_connector_tables` | `query.use` | read | no | Rank every table on one saved connector by exact row count. |
| `recommend_sync_mode` | `workspace.read` | read | no | Recommend full refresh, incremental, append, dedupe, or CDC based on workload requirements. |
| `run_query` | `query.use` | read | no | Run a read-only SQL SELECT (or Mongo JSON filter) against a saved connector. |
| `sample_connector_object` | `query.use` | read | no | Sample live rows from a table/collection on a saved connector (read-only). |
| `search_connectors` | `connector.read` | read | no | Search the connector catalog (tiles ≠ transfer-ready — prefer list_connectors for saved live connections). |
| `search_data` | `workspace.read` | read | no | Search across all datasets for columns or values matching a query. |
| `search_knowledge` | `workspace.read` | read | no | Search trained Datawrap Pilot knowledge base — connectors, transfers, PII, mappings, product help. |
| `start_transfer_studio` | `workspace.read` | read | no | Open Transfer Studio to start or continue a transfer (safe navigate). |
| `test_connector` | `connector.write` | read | no | Probe a saved connector's connectivity now (same as the Test button) and report the result. |
| `plan_transfer` | `job.plan` | plan | no | Plan a real transfer between two saved connectors: introspect both schemas live (or peek a CALL/SELECT result — never treat a procedure stream name... |
| `plan_transfer_route` | `job.plan` | plan | no | Plan an any-to-any transfer route with sync mode, schema policy, validation gates, and risk controls. |
| `remediate_validation` | `job.plan` | plan | no | Propose a Studio remediation for the active Validate step: strip control characters, open Fix bad data, quarantine posture, or review mappings. |
| `acknowledge_cdc_mapping_review` | `job.manage` | mutate | yes | Stage acknowledging one CDC mapping review after the mapping for its table was checked, so the schedules on that table may run again. |
| `cancel_job` | `job.manage` | mutate | yes | Stage cancellation of a running or queued transfer job. |
| `cleanup_cdc_captures` | `workspace.manage` | mutate | yes | Stage dropping orphaned Datawrap replication slots / publications on a saved PostgreSQL source (all cleanable ones, or the slot_names given). |
| `clear_cursor_resnapshot` | `workspace.manage` | mutate | yes | Clear an incremental/CDC watermark so the next run re-snapshots from scratch. |
| `create_connector` | `connector.write` | mutate | yes | Create a saved connector from credentials the user provided (MySQL, PostgreSQL, MongoDB, etc.). |
| `create_schedule` | `schedule.manage` | mutate | yes | Stage a recurring pipeline (schedule) between two saved connectors for the operator to Confirm. |
| `create_transform_recipe` | `connector.write` | mutate | yes | Stage a transform recipe cloned from a starter template (standardise-email-hash-pii, trim-title-case-names, safe-cast-numerics, dedupe-keep-latest,... |
| `delete_connector` | `connector.delete` | mutate | yes | Stage deletion of one saved connector by name or id. |
| `delete_schedule` | `schedule.manage` | mutate | yes | Stage deletion of one pipeline schedule by name or id. |
| `prepare_cdc_source` | `job.run` | mutate | yes | Stage the server settings continuous CDC needs on one saved database. |
| `rebaseline_transfer` | `job.manage` | mutate | yes | Accept the current destination as the append-continuity baseline for a saved destination connector + table (audited, requires a reason). |
| `recover_schedule_destination` | `schedule.manage` | mutate | yes | Recover a schedule whose destination was dropped or changed outside the pipeline (it is parked on a DESTINATION_CONTINUITY finding: enabled, no nex... |
| `repair_cdc_drift` | `job.manage` | mutate | yes | Stage an operator-confirmed, fenced copy of selected CDC drifted rows. |
| `replay_quarantine` | `job.manage` | mutate | yes | Stage a replay of a job's open quarantine rows through the same writer the screen uses. |
| `rerun_job` | `job.manage` | mutate | yes | Stage a new run of a completed, completed-with-quarantine, or cancelled job. |
| `resume_job` | `job.manage` | mutate | yes | Stage resumption of a failed or paused transfer from its last committed checkpoint. |
| `retry_job` | `job.manage` | mutate | yes | Stage a from-zero re-run of a failed or cancelled transfer as a new job. |
| `run_schedule_now` | `schedule.manage` | mutate | yes | Propose an immediate run of a pipeline schedule. |
| `set_schedule_enabled` | `schedule.manage` | mutate | yes | Stage pausing (enabled=false) or resuming (enabled=true) a pipeline schedule. |
| `start_dataset_transfer` | `job.run` | mutate | yes | Stage a transfer from an uploaded file (csv, tsv, json, jsonl, and the other formats the file parser reads) into a saved connector. |
| `start_transfer` | `job.run` | mutate | yes | Stage a transfer between two saved connectors for the operator to Confirm. |
| `update_connector` | `connector.write` | mutate | yes | Stage a change to one saved connector's connection fields (host, port, database, username, password, connection_string, schema, ssl, warehouse, aut... |
| `update_schedule` | `schedule.manage` | mutate | yes | Stage a cadence or name change on one existing pipeline. |

<!-- END GENERATED: mcp-tools -->

## What MCP cannot do

- Approve a Trust Report fix or sign a Trust Report. Only reading and
  explaining a report is available (`explain_parity`).
- Change workspace settings, members, roles or API keys.
- Run SQL that changes data. `run_query` accepts read-only statements.

## Not covered

- Which MCP clients have been tested against this server.
- The full JSON schema of each tool. Read it from `GET /api/v1/mcp/tools`.
- The OAuth authorisation flow end to end.

## Verified against

- `apps/api/src/routers/mcp_router.py`
- `apps/api/services/mcp_protocol.py`, `apps/api/services/mcp_policy.py`,
  `apps/api/services/mcp_oauth.py`, `apps/api/services/mcp_invocation_log.py`
- `apps/api/src/ai/copilot/tools.py` (`TOOL_DEFINITIONS`)
- `apps/api/src/ai/copilot/tool_permissions.py`
- `apps/api/src/ai/copilot/confirm_ack.py`, `apps/api/src/ai/copilot/ack_ledger.py`
- `apps/api/services/mcp_confirm_policy.py` (confirm policy), `apps/api/tests/test_mcp_confirm_requires_human_channel.py`
- `apps/api/services/rbac.py`, `apps/api/tests/fixtures/rbac_route_permissions.json`
