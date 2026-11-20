/*
 * API snippets shown in the product pages' code cards. Each is copied from
 * the docs or built only from routes and headers the docs name; the routes
 * are the real ones in apps/api/src/routers.
 *
 *   QUICKSTART_LOGIN    docs/use/quickstart.md:88-90 (verbatim)
 *   QUICKSTART_EXECUTE  docs/use/quickstart.md:133-144 (verbatim)
 *   JOBS_TRUST_REPORT   GET /jobs/{job_id}/parity-report: quickstart.md:163;
 *                       parity_router.py:98. Headers: quickstart.md:93-100.
 *   JOBS_PROOF_PACK     GET /transfer/{job_id}/proof-pack and
 *                       POST /transfer/proof-pack/verify: verification.md:165-169;
 *                       transfer_router.py:2272, 2312 (the verify route takes
 *                       the pack itself, services/proof_pack_request.py).
 *   JOBS_FOLLOW         GET /jobs/{job_id} and GET /jobs?limit=50&offset=0&status=failed:
 *                       quickstart.md:157-159; jobs_router.py:33, 64.
 *   MCP_CLIENT_CONFIG   lib/helpDocs.ts help-mcp "Procedure: connect Cursor" (verbatim);
 *                       docs/use/mcp.md "Connecting".
 *   MCP_DISCOVER        GET /mcp/tools and GET /mcp/status: docs/use/mcp.md "Connecting"
 *                       and "Authentication" (discovery needs no token); mcp_router.py:308, 475.
 *   SCHEDULE_EXPORT     GET /schedules/{schedule_id}/export?format=yaml (schedule.read):
 *                       helpDocs help-pipelines "Schedules as YAML"; schedules_router.py:944;
 *                       the drawer's Export YAML calls it (lib/api.ts exportScheduleYaml).
 *   SCHEDULES_MANIFEST  GET /schedules/export/manifest?format=yaml (schedule.read):
 *                       schedules_router.py:554; the toolbar's Export YAML calls it
 *                       (lib/api.ts exportDatawrapManifest).
 *
 * No Query API snippet: docs/use/api.md names the /query group but documents
 * no request body, so the Query page has no code band.
 */

export const QUICKSTART_LOGIN = `curl -s -X POST "$API/auth/login" \\
  -H 'Content-Type: application/json' \\
  -d '{"email": "you@example.com", "password": "..."}'`;

export const QUICKSTART_EXECUTE = `curl -s -X POST "$API/transfer/execute" \\
  -H "Authorization: Bearer $TOKEN" \\
  -H "X-Workspace-Id: $WORKSPACE" \\
  -H "Idempotency-Key: first-transfer-001" \\
  -H 'Content-Type: application/json' \\
  -d '{
    "source":      {"kind": "database", "format": "postgresql", "connector_id": "<source id>", "table": "orders"},
    "destination": {"kind": "database", "format": "mysql", "connector_id": "<destination id>", "table": "orders"},
    "mappings": [],
    "sync_mode": "full_refresh_overwrite",
    "validation_mode": "strict"
  }'`;

export const JOBS_TRUST_REPORT = `curl -s "$API/jobs/$JOB_ID/parity-report" \\
  -H "Authorization: Bearer $TOKEN" \\
  -H "X-Workspace-Id: $WORKSPACE"`;

export const JOBS_PROOF_PACK = `curl -s "$API/transfer/$JOB_ID/proof-pack" \\
  -H "Authorization: Bearer $TOKEN" \\
  -H "X-Workspace-Id: $WORKSPACE" > pack.json

curl -s -X POST "$API/transfer/proof-pack/verify" \\
  -H "Authorization: Bearer $TOKEN" \\
  -H "X-Workspace-Id: $WORKSPACE" \\
  -H 'Content-Type: application/json' \\
  -d @pack.json`;

export const JOBS_FOLLOW = `curl -s "$API/jobs/$JOB_ID" \\
  -H "Authorization: Bearer $TOKEN" \\
  -H "X-Workspace-Id: $WORKSPACE"

curl -s "$API/jobs?limit=50&offset=0&status=failed" \\
  -H "Authorization: Bearer $TOKEN" \\
  -H "X-Workspace-Id: $WORKSPACE"`;

export const MCP_CLIENT_CONFIG = `{
  "mcpServers": {
    "datawrap": {
      "url": "https://<tenant-host>/api/v1/mcp",
      "headers": { "Authorization": "Bearer <workspace-api-key>" }
    }
  }
}`;

export const MCP_DISCOVER = `# Discovery answers without a token
curl -s "$API/mcp/tools"

curl -s "$API/mcp/status"`;

export const SCHEDULE_EXPORT = `curl -s "$API/schedules/$SCHEDULE_ID/export?format=yaml" \\
  -H "Authorization: Bearer $TOKEN" \\
  -H "X-Workspace-Id: $WORKSPACE" > schedule.yaml`;

export const SCHEDULES_MANIFEST = `curl -s "$API/schedules/export/manifest?format=yaml" \\
  -H "Authorization: Bearer $TOKEN" \\
  -H "X-Workspace-Id: $WORKSPACE" > datawrap.yaml`;
