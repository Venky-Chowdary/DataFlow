# Self-serve signup (DATAWRAP_SIGNUP_ENABLED)

Individual self-serve signup with email OTP verification. The entire
surface is behind one kill switch: `DATAWRAP_SIGNUP_ENABLED=1`. Off by
default — every new route 404s and the UI renders byte-for-byte as
before.

## Flow

1. `POST /api/v1/auth/signup` — uniform `202 {ok, challenge_id,
   resend_after_sec, expires_in_sec}` for new, pending and existing
   addresses (existing accounts get an "someone tried to sign up"
   notice, not an enumeration oracle). Form-token wall + honeypot,
   Turnstile when configured, velocity buckets, disposable/denylist,
   MX, optional external verifier, risk scorer.
2. `POST /api/v1/auth/verify-email` — 6-digit code (HMAC-SHA256, 10-min
   TTL, 5-attempt burn). On success it creates user + org + workspace +
   membership in one ordered write and sets `__Host-dw_session`
   (HttpOnly/Secure/SameSite=Lax) + `dw_csrf` (readable; JS echoes it
   back as `x-csrf-token`, compared with `hmac.compare_digest`).
3. `POST /api/v1/auth/resend` — 60 s cooldown, 5/h + 10/day caps.
4. Login refuses unverified (`403 {error:{code:"email_unverified"}}`
   + fresh code + `challenge_id`) and banned (neutral 401).

## Recovery / account routes

- `POST /auth/password/forgot` — same uniform 202 + the full bot/velocity
  pipeline (throttling never changes the response shape).
- `POST /auth/password/reset` — argon2id hash, all sessions revoked.
- `POST /auth/email/change` (session) — OTP to the NEW address; runs the
  full signup address gate on it; per-IP/user/target velocity.
- `POST /auth/email/change/confirm` (session) — requires the request to
  be authenticated as the account being rekeyed; canonical claim is
  atomic (sparse unique Mongo index / file lock); rekeys memberships
  and orgs, revokes every session, notifies the old address.
- `GET/DELETE /auth/sessions` — list and revoke your own sessions.
- `POST /auth/onboarding` (session) — persists profile answers on the org.
- `POST /auth/sso/route` — routing hint for claimed email domains
  (per-IP throttled, uniform `{sso: null}` shape).
- `GET /auth/admin/signup-metrics` (iam.manage) — pending signups,
  live challenges, email-quota position.
- `POST /auth/resend-webhook` — Resend (Svix-signed) bounce/complaint
  feed into the suppression list.

## Email

Default driver `resend` (`DATAWRAP_RESEND_API_KEY`), `smtp` for
self-hosted, `console` for dev/test (refuses production). Shared
day/month send quota (`DATAWRAP_EMAIL_DAILY_LIMIT` 100,
`DATAWRAP_EMAIL_MONTHLY_LIMIT` 3000) with an 80% alert; over-quota →
`503 email_quota_exceeded` + `Retry-After`, fail-closed when Redis is
down. `Idempotency-Key` per send. Never logged: codes, passwords,
tokens, API keys.

## Config

| Var | Default | Meaning |
|-----|---------|---------|
| `DATAWRAP_SIGNUP_ENABLED` | off | the kill switch |
| `DATAWRAP_RESEND_API_KEY` | — | Resend key (never logged) |
| `DATAWRAP_RESEND_WEBHOOK_SECRET` | — | Svix verification secret |
| `DATAWRAP_REDIS_URL` | unset | shared counters/session backend |
| `DATAWRAP_OTP_PEPPER` | — | HMAC key for OTP digests |
| `DATAWRAP_TURNSTILE_SECRET` | unset | Cloudflare Turnstile |
| `DATAWRAP_MAXMIND_LICENSE_KEY` | unset | optional GeoIP adapter (off) |
| `DATAWRAP_EMAIL_VERIFIER_*` | unset | optional address verifier (off) |
| `DATAWRAP_HIBP_ENABLED` | off | k-anonymous pwned-password check |
| `DATAWRAP_SIGNUP_IP_HOURLY` | 10 | per-IP signup velocity |
| `DATAWRAP_SIGNUP_SUBNET_HOURLY` | 50 | per-subnet velocity |
| `DATAWRAP_SIGNUP_EMAIL_DAILY` | 5 | per-address signup mails/day |
| `DATAWRAP_FORGOT_*` / `EMCHG_*` | 10/5/3 | recovery + email-change caps |

## Rollout

Merge flag-off → staging → invite-only beta → public. One switch kills
everything.
