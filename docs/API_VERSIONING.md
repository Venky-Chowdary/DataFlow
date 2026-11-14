# API versioning

This is the versioning policy of the Datawrap API as the code implements it
today. It is short because the mechanism is small.

## What exists

- There is one API version. Its prefix is `/api/v1`.
- `GET /api/v1` returns the version, the prefix, a pointer to this page and a
  `deprecation` field. The field is `null`: nothing is deprecated.
- There is no `/api/v2`. The API's own statement is: the public control-plane
  API is `/api/v1` only until a documented `/api/v2` exists.

## What to rely on

- Use the `/api/v1` prefix on every call.
- The sign-in routes are also served without the prefix (`/auth/...`). Those
  are compatibility aliases for misconfigured clients. Do not build on them.
- Send the workspace in the `X-Workspace-Id` header.

## What is not implemented

Be aware of these before you plan a long-lived integration:

- **No deprecation signalling.** The code sends no `Deprecation` or `Sunset`
  headers and has no per-route deprecation list. The `deprecation` field on
  `GET /api/v1` is the only signal, and it has never been set.
- **No stated notice period.** The repository contains no commitment about
  how long a route or field stays after it is replaced.
- **No field-level stability contract.** Most routes do not declare a response
  schema, so a field can be added, and a change is not caught by a schema
  check.
- **No version negotiation.** There is no version header or content type.

Until those exist, pin your integration to the routes you use, test it against
each new build before you roll it out, and read
[CHANGELOG.md](CHANGELOG.md).

## Related identifiers that are versioned

- GitOps manifests accept the earlier API version string and manifest kind as
  well as the current ones. Existing manifests keep working.
- API keys starting `dfk_` stay valid alongside the current `dwk_` prefix.
- Environment flags keep working under the earlier `DATAFLOW_` prefix. See
  [operate/configuration.md](operate/configuration.md).

## Verified against

- `apps/api/src/main.py` (`GET /api/v1`, router prefixes)
- `apps/api/src/middleware/auth_middleware.py` (unprefixed aliases)
- `apps/api/services/brand.py` (legacy GitOps identifiers)
- `apps/api/services/integrations_store.py` (API key prefixes)
- `apps/api/tests/fixtures/rbac_route_permissions.json`
