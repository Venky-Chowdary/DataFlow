# Two-factor authentication (TOTP) and the workspace "require MFA" policy

Status: **implemented in round 11 (QA S7).** Round 8 wrote this as a design;
this page now describes what ships and where it deviates from that design.

Code: `services/totp.py` (RFC 6238), `services/mfa.py` (enrolment,
verification, recovery codes, login challenge), `services/mfa_store.py`
(storage), `services/mfa_enforcement.py` (the workspace policy),
`src/routers/mfa_router.py` (routes), `scripts/mfa_reset.py` (break-glass CLI).
Tests: `tests/test_totp_rfc6238.py`, `tests/test_r11_mfa_e2e.py`.

## Factor: TOTP (RFC 6238)

- HMAC-SHA1, 6 digits, 30-second step, ±1 step of drift. Implemented with
  `hmac`/`hashlib` (pyotp is not a dependency) and checked against the RFC 4226
  and RFC 6238 test vectors. Every candidate step is compared with
  `hmac.compare_digest`; nothing returns early.
- The secret (160 bits) is generated on the server and stored only through
  `services.secret_vault` (`enc:v1:` Fernet, or the configured vault).
- A code is accepted once: the last accepted step is stored per factor and a
  code for the same or an earlier step is refused (`replayed`). The code that
  confirmed enrolment cannot be replayed at sign-in.
- Ten recovery codes (`xxxx-xxxx-xxxx-xxxx`, ~79 bits each), stored as salted
  SHA-256 hashes, shown once, each usable once.
- Bad codes: after `DATAFLOW_MFA_MAX_FAILURES` (5) inside
  `DATAFLOW_MFA_FAILURE_WINDOW_SEC` (900 s) the account's second factor locks for
  `DATAFLOW_MFA_LOCKOUT_SEC` (900 s); every code is refused (429) until then.
  The counter is stored with the factor, so every API replica sees it.
- Storage: one document per account (collection `auth_mfa_factors`, or
  `mfa_factors.json` without Mongo), kept apart from `platform_users` so the
  environment bootstrap admin can enrol too. Writes are version-checked
  (compare-and-swap), so two replicas cannot both accept one code.

## Routes

| Route | Who | What |
|---|---|---|
| `GET /auth/mfa` | signed-in person | status, and whether the policy holds this session |
| `POST /auth/mfa/totp/enroll` | signed-in person (password account) | new secret + `otpauth://` URI, pending. Re-enrolment needs `code`, `recovery_code` or `password`; the old factor keeps working until the new one is confirmed |
| `POST /auth/mfa/totp/confirm {code}` | signed-in person | activates; returns the 10 recovery codes once; marks the current session as MFA-verified |
| `POST /auth/mfa/verify {code \| recovery_code}` | signed-in person | steps the current session up (an enrolled person's older password-only session) |
| `POST /auth/mfa/recovery-codes` | signed-in person, with proof | replaces the recovery codes |
| `POST /auth/mfa/totp/disable` | signed-in person, with proof | removes the factor; refused while a workspace they belong to requires MFA |
| `POST /auth/login` | anyone | for an enrolled account: `202 {mfa_required, challenge}` and no token |
| `POST /auth/login/mfa {challenge, code \| recovery_code}` | anyone holding a challenge | the session (`amr = pwd + otp`, or `pwd + recovery`) |
| `POST /iam/mfa/reset {email, reason}` | platform admin, interactive session | break-glass reset of *another* account (see below) |

The challenge is HMAC-SHA256-signed (key derived from `DATAFLOW_AUTH_SECRET`,
domain-separated from session tokens), expires after 5 minutes, names the
factor it was minted for (a reset invalidates it) and buys exactly one session.

API keys and MCP OAuth tokens cannot enrol: two-factor belongs to a person.

## Enforcement (deviation from round 8)

Round 8 proposed gating only *admin* actions of *admin* members, with a 7-day
grace window, behind `DATAFLOW_MFA_ENFORCE`. The owner asked for a stricter and
simpler rule, which is what ships:

- When a workspace tenant sets `mfa_required`, **every** member's password
  session that has not passed a second factor is *held*: it reaches only
  `/auth/mfa*`, `/auth/me`, `/auth/change-password` and `/auth/logout`. Every
  other route answers `403 {reason: "mfa_enrollment_required"}` (not enrolled)
  or `{reason: "mfa_verification_required"}` (enrolled, older session). On
  public routes a held session is treated as anonymous, so the public MCP
  JSON-RPC endpoint cannot be used around the hold.
- The policy also applies to anyone *acting in* that workspace (the request
  names it with `X-Workspace-Id`) — a platform admin who is not a member
  included.
- No grace window and no separate deploy flag: the tenant setting is the
  opt-in (off by default per workspace), and a grace window is exactly the
  "saved but not enforced" state QA reported.
- Fail closed: if the policy lookup errors, the session is held.
- SSO sessions (OIDC, Azure AD, SAML) are **exempt by default** because the
  identity provider performs MFA; `DATAFLOW_MFA_SSO_EXEMPT=0` holds them too.
  The posture reports `sso_exempt`.
- API keys, service accounts, SCIM tokens and MCP OAuth tokens are not
  sessions and are not affected.
- With `DATAFLOW_REQUIRE_AUTH=0` there are no sessions to hold; the posture says
  `mfa_enforced: false` and lists what is missing.

Every session records how it was authenticated (`amr` on the session row:
`pwd`, `otp`, `recovery`, `sso`). Rows written before round 11 carry none and
count as password-only.

### What an admin sees when first turning it on

1. Settings → Enterprise → "Require two-factor authentication" → Save, before
   the admin has set up two-factor themselves: **refused** (409
   `mfa_enroll_self_first`): "Set up two-factor authentication for your own
   account first (Settings → Security), then require it for the workspace."
   Nothing is saved, so the admin cannot hold their own session.
2. Settings → Security → Two-factor authentication → Set up: setup key and
   `otpauth://` link, a code from the app, then the ten recovery codes (Done is
   enabled after "I saved these recovery codes").
3. Save the switch again: saved. The admin keeps working (their session was
   verified at step 2). The posture shows "Two-factor authentication: Enabled —
   enforced … N of M members have set it up", and the report says
   "required and enforced".
4. Members, on their next request (existing sessions included): those without
   two-factor see a full-screen "Set up two-factor authentication — required by
   your workspace" and nothing else; as soon as they confirm a code the app
   opens in the same session. Members who already enrolled but hold an older
   password-only session are asked for one code. SSO members and API keys are
   unaffected.

## Break-glass

- **REST:** `POST /api/v1/iam/mfa/reset {email, reason}` — platform admin only,
  from an interactive session, never for themselves, reason ≥ 10 characters.
  Removes the factor, pending setup and recovery codes, clears a lockout and
  revokes the account's sessions. Audited as `auth.mfa.reset` with the actor,
  target and reason; refusals are audited too.
- **CLI** (the only platform admin is the one locked out):
  `python apps/api/scripts/mfa_reset.py --email <account> --reason "<why>" --operator <who>`
  on a host with the API's environment. Same effect; audited as
  `auth.mfa.reset` with `actor: cli:<operator>` and `via: cli`.
- The last-admin rule still applies; a reset never removes membership.

## Audit events (`services/admin_audit.py` → `MFA_EVENTS`)

`auth.mfa.enrollment.started`, `auth.mfa.enrolled`, `auth.mfa.challenge.passed`,
`auth.mfa.challenge.failed` (reason `bad_code` / `replayed`), `auth.mfa.locked`,
`auth.mfa.recovery_code.used`, `auth.mfa.recovery_codes.regenerated`,
`auth.mfa.removed`, `auth.mfa.reset`, `auth.mfa.enforcement.held`; and
`auth.login` with outcome `held` when a password sign-in is answered with a
challenge. No event carries a secret, a code or a recovery code.

## Not in this round

- A QR code image: no QR library ships with the API or the web app, so the
  setup key is shown as text and as an `otpauth://` link (phone authenticator
  apps open it directly).
- WebAuthn / passkeys.
- Reading the IdP's `amr`/`acr` claim: SSO is exempt as a whole, by setting.
