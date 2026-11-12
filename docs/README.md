# Datawrap documentation

Datawrap moves data between databases, warehouses, files and applications,
and checks afterwards that the destination matches the source.

Everything here was written from the code at the commit it was added in. Each
page ends with a "Verified against" list of the files it was checked against,
and a "Not covered" list of what was left out because it could not be
verified. Where the product does not do something, the page says so.

## Start here

| If you want to... | Read |
|---|---|
| Run your first transfer | [use/quickstart.md](use/quickstart.md) |
| Install and run the service | [operate/deploy.md](operate/deploy.md) |
| Know what "verified" means before you rely on it | [use/verification.md](use/verification.md) |
| Check what a connector can really do | [reference/connectors.md](reference/connectors.md) |
| Answer a security questionnaire | [operate/security.md](operate/security.md) |

## For people who use Datawrap

| Page | What it covers |
|---|---|
| [use/quickstart.md](use/quickstart.md) | A first transfer, in the web app and through the API |
| [use/sync-modes.md](use/sync-modes.md) | Full refresh, append, upsert, mirror, incremental, SCD2 and CDC: guarantees and limits |
| [use/verification.md](use/verification.md) | Validate, Gate-8, the L1 to L5 ladder, and what proven, sampled and unverifiable mean |
| [use/parity-studio.md](use/parity-studio.md) | The Trust Report, fixes, sign-off and exports |
| [use/pilot.md](use/pilot.md) | What the assistant can and cannot do, and its use of AI models |
| [use/api.md](use/api.md) | Authentication, route groups, errors and pagination |
| [use/mcp.md](use/mcp.md) | The MCP server: tools, permissions and the confirmation model |

## For people who run Datawrap

| Page | What it covers |
|---|---|
| [operate/deploy.md](operate/deploy.md) | Processes and roles, one node or several, backing services, the Helm chart |
| [operate/configuration.md](operate/configuration.md) | Every environment flag the code reads (generated) |
| [operate/security.md](operate/security.md) | Sign-in, roles and permissions, workspace isolation and file areas, the query playground, secrets, egress, residency, the audit chain, browser headers |
| [operate/runbooks.md](operate/runbooks.md) | Stuck, orphaned and failed jobs, locks, CDC lag, failed verification, error references |
| [operate/backup-and-upgrade.md](operate/backup-and-upgrade.md) | Where state lives, what to back up, what to check after an upgrade |

## Reference

| Page | What it covers |
|---|---|
| [reference/connectors.md](reference/connectors.md) | Capability matrix for every driver and file format (generated) |
| [API_VERSIONING.md](API_VERSIONING.md) | The API version policy as implemented |
| [WEBHOOK_SIGNING.md](WEBHOOK_SIGNING.md) | How to verify a webhook signature |
| [SCIM.md](SCIM.md) | Provisioning users and groups from an identity provider |
| [CHANGELOG.md](CHANGELOG.md) | What merged recently |

The three pages in capitals keep the paths that the product itself cites in
API responses and in the app.

## Generated pages

Three tables are generated from the code, and a test fails when one is out of
date.

| Page | Generator | Test |
|---|---|---|
| operate/configuration.md | `docs/tools/gen_config_reference.py` | `apps/api/tests/test_docs_config_reference.py` |
| reference/connectors.md | `docs/tools/gen_connector_matrix.py` | `apps/api/tests/test_docs_connector_matrix.py` |
| use/mcp.md (tool table) | `docs/tools/gen_mcp_tools.py` | `apps/api/tests/test_docs_mcp_tools.py` |

Run a generator from the repository root, for example
`python docs/tools/gen_connector_matrix.py`. Edit the text outside the
generated block by hand; the generator leaves it alone.

A fourth test, `apps/api/tests/test_docs_references_exist.py`, checks that
every documentation path cited anywhere in the code exists.

## Words used in these pages

| Word | Meaning |
|---|---|
| Workspace | A tenant boundary. Connectors, jobs and schedules belong to one workspace. |
| Connector | A saved connection to a source or destination. |
| Job | One run of a transfer. |
| Pipeline (schedule) | A transfer that runs on a cadence. |
| Validate (preflight) | The checks before a write. |
| Gate-8 | The comparison of source and destination after a write. |
| Trust Report | The signed answer to "does the destination match?" for one job. |
| Quarantine | Rows that were rejected instead of written. |
| CDC | Change data capture: reading a database's change log. |
| Pilot | The assistant in the app. |

## What these pages do not claim

- No compliance certification. The repository holds no certification evidence.
- No performance or capacity figures.
- No exactly-once delivery. Delivery is at-least-once throughout.
- No support, service-level or licensing terms.
