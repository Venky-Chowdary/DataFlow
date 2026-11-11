# Quickstart: your first transfer

This walks through one transfer from a source to a destination, first in the
web app and then through the API. Allow about fifteen minutes.

## Before you start

You need:

- a running Datawrap (see [../operate/deploy.md](../operate/deploy.md));
- an account with the **editor** role or higher. An operator can run an
  approved plan but cannot create connectors or design a mapping;
- network access from the Datawrap API to your source and destination;
- credentials for each: host, port, database, user and password, or a
  connection string.

By default the API refuses to connect to private, loopback and cloud metadata
addresses at Test time. If your database is on a private network, a workspace
administrator must add its range to the workspace's network allowlist first.

## Part 1: in the web app

### 1. Add two connectors

1. Open **Connectors** and start a new connector.
2. Pick the engine from the catalog. A tile marked **Planned** cannot be
   selected: there is no driver behind it.
3. Enter the connection details and choose **Test connection**. This opens a
   real connection from the API host.
4. Save. The credentials are encrypted before they are stored.
5. Repeat for the destination.

### 2. Open Transfer Studio

Transfer Studio has six steps, shown across the top.

| Step | What you do |
|---|---|
| **Source** | Choose the source connector and table, or upload a file. |
| **Destination** | Choose the destination connector and name the table. |
| **Transform** | Optional. Shape the data before it is mapped. |
| **Map** | Review how each source column maps to a destination column and type. Columns below the confidence floor need your decision. |
| **Validate** | Run the checks. Every gate must pass, or carry an approved risk, before Run unlocks. |
| **Run** | Choose the sync mode and start the transfer. |

### 3. Choose a sync mode

For a first run into a new or empty table, use **full refresh, overwrite**.
For later runs see [sync-modes.md](sync-modes.md). Overwrite replaces the
table's contents, so do not point it at a table you want to keep.

### 4. Validate

Validate reads schemas, permissions and a sample of rows. A blocked gate names
its cause and a suggested fix. Fix it and validate again. Passing Validate
does not prove every row will transfer correctly; that is what the check after
the write is for.

### 5. Run and watch

Start the run. The job page shows the current step and row counts. When it
finishes, the state is one of:

- `completed`: all rows written.
- `completed_with_quarantine`: finished, with some rows rejected or values
  set to NULL. Open the quarantine to see them.
- `failed`: see [../operate/runbooks.md](../operate/runbooks.md).

### 6. Check the result

Open the job's **Trust Report** tab. It tells you whether the destination
matches the source and how that was checked. Read
[verification.md](verification.md#reading-the-verdict) for what each verdict
means. "Proven" is used only when every row was compared.

## Part 2: through the API

All paths are under `/api/v1`. The examples use `curl` and a base URL in
`$API`. For the exact fields of each request, turn on the interactive
reference outside production (`DATAWRAP_ENABLE_DOCS=1`, then open `/docs`).

### 1. Sign in

```
curl -s -X POST "$API/auth/login" \
  -H 'Content-Type: application/json' \
  -d '{"email": "you@example.com", "password": "..."}'
```

The response contains `token` and `expires_at`. Send the token on every
request as `Authorization: Bearer <token>`. For automation, use a workspace
API key in the same header instead of a session token.

If the account has a second factor, the response asks for it and you complete
sign-in at `POST /auth/login/mfa`.

Name your workspace on each request with the `X-Workspace-Id` header.

### 2. Create and test connectors

- `POST /connectors/saved` creates a saved connector.
- `POST /connectors/saved/{connector_id}/test` tests it.
- `GET /connectors/saved` lists them. Note each connector's id.

`GET /catalog/connectors` lists the catalog with each entry's live status on
your server. It needs no sign-in.

### 3. Plan, map and validate

The plan routes mirror the Studio steps.

| Call | Purpose |
|---|---|
| `POST /transfer/plans` | Create a plan for a source and destination |
| `POST /transfer/plans/{plan_id}/map` | Generate the column mapping |
| `POST /transfer/plans/{plan_id}/mappings` | Save your changes to the mapping |
| `POST /transfer/plans/{plan_id}/preflight` | Run Validate |
| `GET /transfer/plans/{plan_id}/preflight` | Read the Validate result |
| `POST /transfer/plans/{plan_id}/approve` | Approve the validated plan |
| `GET /transfer/plans/{plan_id}/run-payload` | Get the request to run it |

These need `job.plan`.

### 4. Run

For a database-to-database transfer, send JSON to `POST /transfer/execute`
(needs `job.run`):

```
curl -s -X POST "$API/transfer/execute" \
  -H "Authorization: Bearer $TOKEN" \
  -H "X-Workspace-Id: $WORKSPACE" \
  -H "Idempotency-Key: first-transfer-001" \
  -H 'Content-Type: application/json' \
  -d '{
    "source":      {"kind": "database", "format": "postgresql", "connector_id": "<source id>", "table": "orders"},
    "destination": {"kind": "database", "format": "mysql", "connector_id": "<destination id>", "table": "orders"},
    "mappings": [],
    "sync_mode": "full_refresh_overwrite",
    "validation_mode": "strict"
  }'
```

Notes:

- State `sync_mode` explicitly. On this route the default is overwrite.
- Take `mappings` from your plan's run payload. The empty list above is a
  placeholder, not a recommendation.
- `Idempotency-Key` lets you repeat the request safely after a timeout.
- A file upload uses `POST /transfer/run` with a multipart form instead.

### 5. Follow the job

- `GET /jobs/{job_id}` returns the job.
- `GET /connectors/jobs/{job_id}/stream` streams progress events.
- `GET /jobs?limit=50&offset=0&status=failed` lists jobs.

### 6. Read the verdict

- `GET /jobs/{job_id}/parity-report` returns the Trust Report.
- `GET /transfer/{job_id}/proof-pack` returns the signed evidence.

## If something goes wrong

| Symptom | Where to look |
|---|---|
| Test fails with an egress or private address message | The workspace network allowlist; [../operate/security.md](../operate/security.md) |
| Validate blocks | The gate's message; [verification.md](verification.md) |
| 403 on a route | Your role; the role table in [../operate/security.md](../operate/security.md) |
| A response ends with `Reference: ...` | [../operate/runbooks.md](../operate/runbooks.md#reading-an-error-reference) |
| The job stops moving | [../operate/runbooks.md](../operate/runbooks.md#a-job-looks-stuck) |

## Not covered

- The request body of every plan route. Read it from the interactive
  reference.
- Uploading files larger than the upload limit, and scheduled pipelines.
- Screenshots. The screen names above are the labels in the app at this
  commit.

## Verified against

- `apps/web/src/pages/transfer/studioConstants.ts` (step names)
- `apps/web/src/components/ConnectorCatalogPanel.tsx`, `apps/web/src/lib/catalogTileEligibility.ts`
- `apps/api/src/routers/auth_router.py` (login request and response)
- `apps/api/src/routers/transfer_router.py` (`ExecuteTransferRequest`, `EndpointDTO`, `Idempotency-Key`)
- `apps/api/src/routers/jobs_router.py` (list parameters)
- `apps/api/services/rbac.py`, `apps/api/tests/fixtures/rbac_route_permissions.json`
- `apps/api/services/egress_guard.py`, `apps/api/services/job_status.py`
- `apps/api/services/platform_config.py` (`docs_enabled`)
