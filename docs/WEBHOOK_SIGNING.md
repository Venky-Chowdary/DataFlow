# Verifying webhook signatures

Datawrap signs every outgoing webhook notification so your receiver can check
that it came from your Datawrap deployment and was not replayed.

## The signing secret

- Each webhook channel has its own secret. It starts with `whsec_`.
- The secret is shown **once**, when the channel is created, and again only
  when you rotate it. Store it when you see it.
- Rotate with
  `POST /api/v1/workspace/notifications/{channel_id}/signing-secret/rotate`
  (needs `workspace.manage`). The old secret stops being used.

## What each delivery carries

```
X-Datawrap-Timestamp: 1767225600
X-Datawrap-Signature: t=1767225600,v1=<hex HMAC-SHA256>
```

- The algorithm is HMAC-SHA256.
- The key is the channel's signing secret.
- The signed message is the timestamp, a full stop, then the raw request
  body: `<timestamp>.<raw request body>`.

## How to verify

1. Read the **raw bytes** of the request body. Do not parse and re-serialise
   the JSON first; that changes the bytes.
2. Split the `X-Datawrap-Signature` header on commas and read `t` and `v1`.
3. Compute HMAC-SHA256 over `t`, then `.`, then the raw body, with the secret
   as the key. Encode the result as lowercase hex.
4. Compare your result with `v1` using a constant-time comparison.
5. Reject the request if `t` is more than 300 seconds from your clock. This
   stops a captured delivery from being replayed later.

### Example (Python)

```python
import hashlib
import hmac
import time

def verify(secret: str, body: bytes, header: str, tolerance: int = 300) -> bool:
    parts = dict(p.split("=", 1) for p in header.split(",") if "=" in p)
    try:
        ts = int(parts.get("t", ""))
    except ValueError:
        return False
    if abs(time.time() - ts) > tolerance:
        return False
    message = str(ts).encode("ascii") + b"." + body
    expected = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, parts.get("v1", ""))
```

## Testing a channel

- `POST /api/v1/workspace/notifications/{channel_id}/test` sends a test
  delivery.
- `GET /api/v1/workspace/notifications/{channel_id}/deliveries` lists recent
  deliveries.

## Good practice

- Keep your receiver's clock in sync, or valid deliveries will be rejected.
- Answer quickly and do the work afterwards.
- Treat a delivery as a notice. Read the current state from the API before
  acting on it.
- Webhook URLs are checked by the egress guard. A URL that resolves to a
  private, loopback or metadata address is refused.

## Not covered

- The payload of each event type.
- Retry timing and how many times a failed delivery is retried.
- Whether deliveries can arrive out of order.

## Verified against

- `apps/api/services/webhook_signing.py`
- `apps/api/src/routers/workspace_router.py` (signing disclosure, rotate, test, deliveries)
- `apps/api/services/egress_guard.py`
- `apps/api/tests/fixtures/rbac_route_permissions.json`
