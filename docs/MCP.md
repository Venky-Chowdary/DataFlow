# DataFlow MCP

DataFlow exposes MCP Streamable HTTP at `/api/v1/mcp` and a compatibility REST
bridge at `/api/v1/mcp/tools/call`. Use HTTPS in production and send an
`Origin` allowed by the server configuration.

## Clients

Cursor, Claude Desktop, VS Code MCP extensions, and generic remote Streamable
HTTP clients (including Grok) use the Streamable HTTP URL. Configure the URL
and an `Authorization: Bearer <workspace-api-key>` header. API keys can be
created in workspace settings; scoped keys carry only the listed DataFlow
permissions. OAuth clients use an access token issued for the MCP resource.

OAuth is advertised through RFC 9728 protected-resource metadata. Tokens must
include an `email` claim and DataFlow permission scopes, either
`datawrap:<permission>` or the bare permission name. Okta and Entra access
tokens commonly need an email claim mapper. The default audience is
`<public_url>/api/v1/mcp`; set `MCP_OAUTH_AUDIENCE` (or the brand-aware
`DATAFLOW_MCP_OAUTH_AUDIENCE`) to override it. Configure the issuer with
`MCP_OAUTH_ISSUER` or the enabled OIDC SSO issuer.

## Tool permissions

The canonical role/scope table is
`src.ai.copilot.tool_permissions.TOOL_PERMISSIONS`. Each OAuth scope is
`datawrap:<permission>` or the bare permission shown here; role grants are
defined by `services.rbac`.

| Tool | Required permission scope | Effect |
| --- | --- | --- |
| `aggregate_data` | `query.use` | `read` |
| `analyze_dataset` | `workspace.read` | `read` |
| `analyze_result` | `query.use` | `read` |
| `brief_workspace` | `workspace.read` | `read` |
| `cancel_job` | `job.manage` | `mutate` |
| `compare_connectors` | `connector.read` | `read` |
| `compare_datasets` | `workspace.read` | `read` |
| `confirm_action` | `workspace.read` | `read` |
| `create_connector` | `connector.write` | `mutate` |
| `create_schedule` | `schedule.manage` | `mutate` |
| `delete_connector` | `connector.delete` | `mutate` |
| `delete_schedule` | `schedule.manage` | `mutate` |
| `describe_pilot` | `workspace.read` | `read` |
| `diff_schemas` | `connector.read` | `read` |
| `explain_mapping_assurance` | `workspace.read` | `read` |
| `explain_product` | `workspace.read` | `read` |
| `filter_result` | `query.use` | `read` |
| `get_job` | `job.read` | `read` |
| `get_preflight_run` | `job.read` | `read` |
| `get_schedule` | `schedule.read` | `read` |
| `get_transfer_capabilities` | `workspace.read` | `read` |
| `inspect_schema_policy` | `connector.read` | `read` |
| `introspect_connector_schema` | `connector.read` | `read` |
| `list_connector_objects` | `connector.read` | `read` |
| `list_connectors` | `connector.read` | `read` |
| `list_contracts` | `job.read` | `read` |
| `list_datasets` | `workspace.read` | `read` |
| `list_jobs` | `job.read` | `read` |
| `list_schedules` | `schedule.read` | `read` |
| `map_connector_schemas` | `connector.read` | `read` |
| `navigate` | `workspace.read` | `read` |
| `open_job` | `job.read` | `read` |
| `open_schedule` | `schedule.read` | `read` |
| `plan_transfer` | `job.plan` | `plan` |
| `plan_transfer_route` | `job.plan` | `plan` |
| `prepare_cdc_source` | `job.run` | `mutate` |
| `profile_quality_rules` | `workspace.read` | `read` |
| `rank_connector_tables` | `query.use` | `read` |
| `recommend_sync_mode` | `workspace.read` | `read` |
| `remediate_validation` | `job.plan` | `plan` |
| `replay_quarantine` | `job.manage` | `mutate` |
| `resume_job` | `job.manage` | `mutate` |
| `retry_job` | `job.manage` | `mutate` |
| `run_query` | `query.use` | `read` |
| `run_schedule_now` | `schedule.manage` | `mutate` |
| `sample_connector_object` | `query.use` | `read` |
| `search_connectors` | `connector.read` | `read` |
| `search_data` | `workspace.read` | `read` |
| `search_knowledge` | `workspace.read` | `read` |
| `set_schedule_enabled` | `schedule.manage` | `mutate` |
| `start_dataset_transfer` | `job.run` | `mutate` |
| `start_transfer` | `job.run` | `mutate` |
| `start_transfer_studio` | `workspace.read` | `read` |
| `test_connector` | `connector.read` | `read` |
| `update_connector` | `connector.write` | `mutate` |
| `update_schedule` | `schedule.manage` | `mutate` |

Unknown tools are denied. `confirm_action` is not implicitly enabled when
another mutating tool is allowed; it is governed independently.

## Administrative policy

Workspace administrators with `workspace.manage` can use `GET` and `PUT
/api/v1/mcp/policy` to disable MCP or allow-list tool names. A disabled server
returns policy error `-32003` (or HTTP 403 through the REST bridge). A
disallowed tool returns the same policy error and the tool name. `tools/list`
is filtered by the allow-list.

## Limits and Origin

Configure MCP rate limits with `DATAFLOW_MCP_RATE_LIMIT`,
`DATAFLOW_MCP_RATE_BURST`, `DATAFLOW_MCP_RATE_QPS`, and
`DATAFLOW_MCP_RATE_MAX_KEYS`. Only `tools/call` consumes the limit.
DNS-rebinding protection rejects disallowed
`Origin` values with HTTP 403; configure allowed origins through the normal
DataFlow CORS settings and optional `CORS_ORIGIN_REGEX`.

## Troubleshooting

* `-32001` / HTTP 401: authentication is missing or invalid; follow the
  `WWW-Authenticate` resource-metadata challenge.
* `-32003` / HTTP 403: the organization policy disabled MCP or disallowed the
  requested tool.
* `-32029` / HTTP 429: rate limit exceeded; retry after `retry_after_sec`.
* `-32600`: invalid JSON-RPC request or Origin.
* `-32602`: malformed tool arguments.
* `isError: true` with text beginning `Your role`: add the required role or
  permission scope; this is not a connector failure.
