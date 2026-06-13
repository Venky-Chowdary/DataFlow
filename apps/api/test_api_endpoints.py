import httpx
import json

base = "http://localhost:8001/api/v1/ai"

tests = []

# Enhanced mapping
r = httpx.post(f"{base}/map/enhanced", json={
    "source_columns": ["cust_name", "AMT", "email_addr"],
    "target_columns": ["customer_name", "amount", "email_address"],
}, timeout=30)
tests.append(("POST /map/enhanced", r.status_code, r.json()))

# Models status
r2 = httpx.get(f"{base}/models/status", timeout=30)
tests.append(("GET /models/status", r2.status_code, r2.json()))

# RAG query
r3 = httpx.post(f"{base}/rag/query", json={"query": "What columns contain customer PII?"}, timeout=30)
tests.append(("POST /rag/query", r3.status_code, r3.json()))

# Enhanced analysis
r4 = httpx.post(f"{base}/analyze/enhanced", json={
    "columns": {
        "cust_id": ["C001", "C002"],
        "email": ["test@example.com"],
        "order_amt": ["150.00", "89.99"],
    }
}, timeout=30)
tests.append(("POST /analyze/enhanced", r4.status_code, r4.json()))

for name, status, data in tests:
    print(f"\n{name}: {status}")
    if status == 200:
        print(json.dumps(data, indent=2)[:600])
    else:
        print(data)
