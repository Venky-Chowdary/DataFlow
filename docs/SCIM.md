# SCIM provisioning

Datawrap implements SCIM 2.0 so an identity provider can create, update and
deactivate users and groups.

## Set up

1. Sign in as a platform administrator.
2. Create a SCIM token: `POST /api/v1/iam/scim-token`. The token is an API key
   limited to the `scim.provision` scope. It is shown once.
3. In your identity provider, set:
   - **Base URL**: `https://<your-api-host>/api/v1/scim/v2`
   - **Authentication**: bearer token, using the token from step 2.
4. Run the provider's connection test.

Revoke a SCIM token with `DELETE /api/v1/iam/service-accounts/{key_id}`.
Creating and revoking tokens needs `iam.manage`, which only a platform
administrator holds.

## What is supported

| Feature | Supported |
|---|---|
| Users: list, create, read, replace, patch, delete | yes |
| Groups: list, create, read, replace, patch, delete | yes |
| `ServiceProviderConfig`, `ResourceTypes`, `Schemas` | yes |
| PATCH | yes |
| Filtering | yes: `eq`, `sw` and `co`, at most 200 results |
| Paging | `startIndex` and `count` |
| Bulk operations | no |
| Sorting | no |
| ETags | no |
| Password change through SCIM | no |

Filterable attributes:

- users: `userName`, `externalId`, `emails.value`, `active`
- groups: `displayName`, `externalId`

## Mapping groups to roles

A SCIM group does nothing on its own. To give its members a role, map the
group's display name to a role in one workspace:

- `GET /api/v1/iam/scim/group-mappings` reads the mappings.
- `PUT /api/v1/iam/scim/group-mappings` replaces them.

Each mapping names a workspace and one of the workspace roles. Rules:

- The workspace must exist.
- You cannot map a group to a role higher than the one you hold in that
  workspace. An attempt is refused and written to the audit log as
  `iam.privilege_escalation.denied`.

## Things to know

- SCIM manages accounts and group membership. Sign-in still goes through SSO
  or a password. See [operate/security.md](operate/security.md).
- Deactivating a user through SCIM disables the account, revokes that
  person's sessions and revokes the personal API keys they created. Service
  account and SCIM tokens they created are kept, so provisioning does not cut
  itself off.
- SCIM state is kept in MongoDB when it is connected, and in `scim.json` in
  the data directory otherwise.

## Not covered

- Step-by-step instructions for a specific identity provider.
- Conformance testing against a provider's SCIM validator. None was run for
  this page.
- How quickly a group change takes effect on a signed-in session.

## Verified against

- `apps/api/src/routers/scim_router.py` (`ServiceProviderConfig`, routes, paging)
- `apps/api/services/scim_service.py` (filter attributes and operators)
- `apps/api/services/scim_store.py`
- `apps/api/src/routers/iam_router.py` (SCIM token, group mappings)
- `apps/api/services/rbac.py` (`scim.provision`, `iam.manage`)
- `apps/api/tests/fixtures/rbac_route_permissions.json`
