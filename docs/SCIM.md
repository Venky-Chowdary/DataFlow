# SCIM provisioning

The API exposes a SCIM 2.0 subset at `/api/v1/scim/v2` for core User and Group
resources. Create a bearer token with an administrator account at
`POST /api/v1/iam/scim-token`; the returned secret is shown once. Map group
display names to existing workspaces and roles at
`/api/v1/iam/scim/group-mappings`.

User `userName` is normalized to an email address and is immutable after
creation. Platform accounts are keyed by email, so changing `userName` is
rejected with SCIM `mutability`; create a new identity and deprovision the old
one instead. SCIM identity documents are mappings; credentials remain in the
platform user store.

Filters support `eq`, `sw`, and `co`, joined by `and`; page size is capped at
200. Bulk, sorting, ETags, and password changes are not supported. SCIM group
synchronization only changes workspace memberships it created; manual
memberships remain under workspace administration.

Setting a user inactive or deleting the SCIM resource disables the account,
revokes its sessions and personal API keys, and preserves workspace-owned
service-account and SCIM tokens.
