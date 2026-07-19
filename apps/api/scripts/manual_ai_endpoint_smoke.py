"""Manual smoke script for the AI mapping endpoints — not a pytest test.

Requires the API server running locally (e.g. `uvicorn src.main:app --port 8001`).
Run directly: `python scripts/manual_ai_endpoint_smoke.py`.

(Previously named `test_api_endpoints.py` at the apps/api root, which made
module-level HTTP calls at import time — pytest would try to collect it as a
test and fail with a connection error whenever the server wasn't running.)
"""

import json

import httpx

BASE = "http://localhost:8001/api/v1/ai"


def main() -> None:
    checks = []

    r = httpx.post(f"{BASE}/map/enhanced", json={
        "source_columns": ["cust_name", "AMT", "email_addr"],
        "target_columns": ["customer_name", "amount", "email_address"],
    }, timeout=30)
    checks.append(("POST /map/enhanced", r.status_code, r.json()))

    r2 = httpx.get(f"{BASE}/models/status", timeout=30)
    checks.append(("GET /models/status", r2.status_code, r2.json()))

    r3 = httpx.post(f"{BASE}/rag/query", json={"query": "What columns contain customer PII?"}, timeout=30)
    checks.append(("POST /rag/query", r3.status_code, r3.json()))

    r4 = httpx.post(f"{BASE}/analyze/enhanced", json={
        "columns": {
            "cust_id": ["C001", "C002"],
            "email": ["test@example.com"],
            "order_amt": ["150.00", "89.99"],
        }
    }, timeout=30)
    checks.append(("POST /analyze/enhanced", r4.status_code, r4.json()))

    for name, status, data in checks:
        print(f"\n{name}: {status}")
        if status == 200:
            print(json.dumps(data, indent=2)[:600])
        else:
            print(data)


if __name__ == "__main__":
    main()
