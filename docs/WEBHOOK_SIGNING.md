# Verifying Datawrap webhook signatures

Every **webhook** notification channel has its own signing secret
(`whsec_…`). It is shown **once**, in the response that creates the channel
(`POST /api/v1/workspace/notifications`, field `signing_secret`), and again
only when you rotate it
(`POST /api/v1/workspace/notifications/{channel_id}/signing-secret/rotate`).
Reads never return it (`signing_secret_set: true` says one exists). Channels
created before signing existed send no signature until you rotate once.

Each delivery carries two headers:

```
X-Datawrap-Timestamp: 1767225600
X-Datawrap-Signature: t=1767225600,v1=5257a869e7ecebeda32affa62cdca3fa51cad7e77a0e56ff536d0ce8e108d8bd
```

`v1` is the lowercase hex HMAC-SHA256 of the message
`"<timestamp>.<raw request body>"`, keyed with the signing secret.

To verify on the receiver:

1. Read the **raw** request body bytes (before any JSON parsing).
2. Parse `t` and `v1` from `X-Datawrap-Signature`.
3. Reject if `|now - t| > 300` seconds (replay protection).
4. Compute `HMAC_SHA256(secret, t + "." + raw_body)` as hex.
5. Compare with `v1` using a constant-time comparison.

Python:

```python
import hashlib, hmac, time

def verify(secret: str, raw_body: bytes, header: str, tolerance: int = 300) -> bool:
    parts = dict(p.split("=", 1) for p in header.split(",") if "=" in p)
    ts = int(parts.get("t", "0"))
    if abs(time.time() - ts) > tolerance:
        return False
    expected = hmac.new(secret.encode(), f"{ts}.".encode() + raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, parts.get("v1", ""))
```

Node.js:

```js
const crypto = require("crypto");
function verify(secret, rawBody, header, tolerance = 300) {
  const parts = Object.fromEntries(header.split(",").map((p) => p.split("=")));
  const ts = Number(parts.t);
  if (Math.abs(Date.now() / 1000 - ts) > tolerance) return false;
  const expected = crypto.createHmac("sha256", secret).update(`${ts}.`).update(rawBody).digest("hex");
  const got = Buffer.from(parts.v1 || "");
  const want = Buffer.from(expected);
  return got.length === want.length && crypto.timingSafeEqual(got, want);
}
```

Rotating issues a new secret and the old one stops signing immediately;
update the receiver with the new secret right after rotating (or accept
either secret on the receiver for the few seconds of the switch). The
reference implementation is `apps/api/services/webhook_signing.py`.
