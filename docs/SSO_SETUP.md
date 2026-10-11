# SSO setup notes

## OIDC and Entra

Generic OIDC requires `email_verified=true` by default. Set
`OIDC_REQUIRE_EMAIL_VERIFIED=0` only when the provider does not supply that
claim; an explicit `email_verified=false` is still rejected.

For Entra ID, the application accepts `email`, then `preferred_username`, then
`upn` as the account email. This is **Entra-tenant-trusted**: the selected
claim comes from the configured tenant and is trusted as that tenant asserts
it. Multi-tenant Entra identifiers (`common`, `organizations`, `consumers`) are
not supported.

OIDC policy and cache settings:

- `OIDC_ALLOWED_ALGS`: comma-separated signing algorithm allow-list; default
  `RS256`.
- `OIDC_CLOCK_SKEW_SEC`: token time leeway in seconds; default `60`, allowed
  range `0..300`.
- `OIDC_METADATA_TTL_SEC` and `OIDC_JWKS_TTL_SEC`: discovery and signing-key
  cache lifetimes in seconds; default `3600`.
- `OIDC_JWKS_REFRESH_COOLDOWN_SEC`: minimum seconds between unknown-key refreshes
  per JWKS URI; default `60`.

## SAML and state storage

SAML requires a solicited, correlated response; IdP-initiated (unsolicited) SSO
is rejected. Assertion IDs are claimed in a replay cache. MongoDB is the
multi-host state and replay store. The flocked JSON-file store is safe across
workers on one host only and is not shared across hosts.
