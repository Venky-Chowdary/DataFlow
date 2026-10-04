# MCP operator stories

Live evidence is the 2026-10-04 workspace run through `https://www.datawrap.io/api/v1/mcp`. Case rows are in `DataFlow-QE-Test-Cases.xlsx`. These stories are the work that run forced. They are not a second product.

## User stories

1. **As an operator using Cursor**, I can list connectors, test them, read schemas, sample tables, and count rows, with the same answers Transfer Studio would give.
2. **As an operator**, I can stage a connector, a transfer, or a schedule from MCP and finish it with `confirm_action` and the `ack_id`. Staging does not move data. Confirm does, once. Replaying the same `ack_id` does not create a second connector or a second job.
3. **As an operator**, "how many jobs?" and "how many contracts?" mean one population. The page I was shown is not the census.
4. **As an operator**, asking about an uploaded file by its name analyzes that file. An industry template is used only when no upload matches.
5. **As a viewer**, I can read jobs and connectors. I cannot spend an approval my role does not hold.
6. **As an admin**, I choose the role on a workspace API key. A key that has no role stored is an editor, so MCP can create connectors, transfers, and schedules. A viewer key cannot.

## Bug stories

| Id | What the live run showed | Fix |
| --- | --- | --- |
| BUG-1 | `analyze_dataset("sample_payments")` returned the empty Financial Services template. `scale_100k`, `mapping_golden`, and `sample_schema_types` were listed, then "not found". | `services/dataset_resolve.py`. Exact folded names win. A keyword does not replace an upload. Ambiguous names stay unbound. |
| BUG-2 | Brief said 50 contracts. `list_contracts` at the tool cap returned 200 (141 signed, 57 draft, 2 deprecated). | `count_contracts_by_status`. A full page of 50 is not the population. `list_contracts` now returns `total`, `truncated`, and `by_status`. |
| BUG-3 | Brief job count 544. `list_jobs` total 575. The brief scoped `workspace_id=""`. The list omitted the scope, which means every workspace. | `list_jobs` defaults to `scope=workspace` (same as the brief). `scope=all` is explicit. |
| BUG-4 | MCP could stage `create_connector`, `start_transfer`, and `create_schedule`, and the only way to finish them was the browser `POST /copilot/confirm`. | `confirm_action` calls that same route. The ack stays on the server. |
| BUG-5 | 10 of 11 saved connectors failed a live retest (MySQL reset, Postgres closed the connection, Snowflake account/role, Redis reset). `PostgresVenkat` connected. | Not a code change. The saved endpoints did not accept a connection. Destination counts on the live Postgres still matched completed jobs: `kilo` 10, `mm` 5, `were` 5. |
| BUG-6 | Help and the MCP page told an operator to dial `https://api.datawrap.io` or a relative `/api/v1/mcp`, and Claude's snippet ran a package this product does not ship. | The snippet is the absolute URL of the signed-in host plus `Authorization: Bearer`. Claude and VS Code use that same HTTP endpoint. |
| BUG-7 | A workspace API key stored no role. The request gate treated it as a viewer, so MCP could not create a connector, a transfer, or a schedule once the caller's role was bound. | The key has a role. A key minted before the field existed resolves to editor. Settings lets an admin choose viewer, operator, editor, or admin. `confirm_action` still re-checks that role against the ack kind. |

## Test stories

| Id | Scenario | Expected |
| --- | --- | --- |
| TS-1 | Analyze `sample_payments` when a finance template is also loaded | The 10-row upload, not the template. `.tsv` selects the 3-row sibling. |
| TS-2 | 80 contracts in the store, brief page size 50 | Census reports 80, not 50. |
| TS-3 | `list_jobs` with no scope | `workspace_id=""`, matching the brief. `scope=all` passes `None`. |
| TS-4 | `confirm_action` with no active request | Error, no mutation. Proven without booting the API. |
| TS-5 | `confirm_action` with an `ack_id` from `start_transfer` | One job, through the transfer engine. A second confirm of that ack returns the first result. |
| TS-6 | Viewer calls `confirm_action` on a `create_connector` ack | Refusal from the confirm route. The ack is not consumed. |
| TS-7 | Live connector test matrix | Record pass/fail per saved connector. Do not treat a refused host as an engine defect. |
| TS-8 | Legacy API key with no role field | Resolves to editor. An unknown label resolves to viewer. Creating a key with `owner` is refused. |
| TS-9 | MCP URL from `/api/v1` on `https://www.datawrap.io` | `https://www.datawrap.io/api/v1/mcp`, with a Bearer placeholder. No `api.datawrap.io`. No `mcp-bridge`. |

TS-5 and TS-6 run where the API process and its stores are up. This runner did not boot that process. Production does not have `confirm_action` until this build is deployed, so it was not executed against the live workspace.

## What MCP can do after this change

Read: datasets, connectors, schemas, samples, aggregates, queries, jobs, contracts, schedules, preflight, product explanations.

Stage, then `confirm_action`: create a connector, start a transfer, create a schedule, run a schedule now, cancel / retry / resume a job, replay quarantine, delete a connector (admin key), enable or delete a schedule. `test_connector` is immediate and does not need an ack.

The key's role is the gate. Editor can create connectors, transfers, and schedules. Operator can run jobs and cannot author connectors or schedules. Viewer can read. Confirm checks the role again, against the ack kind, and does not consume the ack when the role cannot perform it.

File uploads are analyzed by dataset name. A file-to-warehouse transfer still starts from Transfer Studio or from a saved connector that can read the file. MCP does not invent a second transfer engine for that path.

## Not claimed

Connector outages in the saved workspace were not "fixed" by skipping the connection test. A retest on 2026-10-04 dialed all 11 saved connectors again: `PostgresVenkat` connected (10 tables in `public`); the other 10 failed the same way (Snowflake account/role, MySQL 2013, Postgres closed the connection, Redis reset). `list_schedules` returned an empty store. No schedule was created and no new transfer was written to `PostgresVenkat`. Production still serves 51 tools and does not yet include `confirm_action`; `analyze_dataset("sample_payments")` on that server still returns the empty Financial Services template. CDC remains at-least-once upsert.
