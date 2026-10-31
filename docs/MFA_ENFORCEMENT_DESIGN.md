# MFA enforcement for workspace admins — design proposal (round 8)

Status: **design only, not implemented.** `tenant.mfa_required` is recorded
policy; `mfa_enforced` in the security posture stays `false` until this ships.

## Why not in round 8

Enforcing MFA needs a second factor to exist first. Today password login
(`POST /auth/login`) issues a session directly; there is no enrolled factor
per account (the OTP service only mails signup/email-change codes). Turning
the flag into a login gate without enrolment would lock every admin out, and
mailed codes are not an acceptable admin factor. That is not small or safe.

## Proposal

1. **Factor: TOTP (RFC 6238)**, optional WebAuthn later.
   - `POST /auth/mfa/totp/enroll` -> secret + otpauth URI (shown once), stored
     encrypted (`secret_vault`) on the user record as `mfa.totp` (pending).
   - `POST /auth/mfa/totp/confirm {code}` activates it and returns 10 one-time
     recovery codes (hashed at rest).
   - `DELETE /auth/mfa/totp` requires a fresh code (or an admin reset, audited).
2. **Login step-up.** When the account has an active factor, `/auth/login`
   returns `202 {mfa_required: true, challenge}` (short-lived, single-use,
   bound to the password check) instead of a token; `POST /auth/login/mfa
   {challenge, code}` issues the session with `amr=["pwd","otp"]` recorded on
   the session row. SSO logins carry the IdP's `amr`/`acr` and count as MFA
   when the IdP asserts it.
3. **Enforcement per workspace.** When a tenant sets `mfa_required`, a member
   whose workspace role is `admin` must hold a session with MFA to perform
   admin actions in that workspace:
   - RBAC gate: `workspace.manage`, `iam.manage`, `scim.provision` require
     `session.amr` to include a second factor for those members; otherwise
     `403 {reason: "mfa_required"}` and the client routes to enrolment.
   - Admins without a factor get a grace window (`mfa_grace_until`, default 7
     days from when the policy is turned on) shown as a banner, then refusal.
   - API keys / service accounts / SCIM tokens are not sessions and are
     unaffected; minting them is an admin action, so it already needs MFA.
4. **Break-glass.** A platform admin can reset a member's factor (audited
   `auth.mfa.reset`); the last-admin rule still applies.
5. **Posture.** `mfa_enforced` becomes true for a workspace only when its
   tenant sets `mfa_required` and the gate is active (flag
   `DATAFLOW_MFA_ENFORCE`, off by default, audited at startup like other
   security flags).

## Audit events

`auth.mfa.enrolled`, `auth.mfa.removed`, `auth.mfa.challenge.failed`,
`auth.mfa.reset`, `authz.denied` with `reason_code=mfa_required`.

## Tests to write when implemented

Enrol/confirm/login step-up through the real routes; an admin without MFA is
refused admin routes in an `mfa_required` workspace but not in another; grace
window; recovery code is single-use; SSO with `amr=mfa` passes.
