#!/usr/bin/env bash
# Generate production secrets for .env — run from repo root
set -euo pipefail

AUTH_SECRET=$(openssl rand -hex 32)
MONGO_PASS=$(openssl rand -base64 24 | tr -dc 'a-zA-Z0-9' | head -c 24)
SECRETS_KEY=$(python3 -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())")

echo ""
echo "=== DataFlow production secrets ==="
echo ""
echo "DATAFLOW_AUTH_SECRET=${AUTH_SECRET}"
echo "DATAFLOW_SECRETS_KEY=${SECRETS_KEY}"
echo "MONGO_ROOT_PASSWORD=${MONGO_PASS}"
echo "MONGODB_URI=mongodb://dataflow:${MONGO_PASS}@mongodb:27017/?authSource=admin"
echo ""
echo "To create an admin user (password: AdminPass123!):"
HASH=$(echo -n "AdminPass123!" | shasum -a 256 | awk '{print $1}')
echo "DATAFLOW_AUTH_USERS=[{\"email\":\"admin@yourcompany.com\",\"password_hash\":\"${HASH}\",\"name\":\"Admin\",\"role\":\"owner\"}]"
echo ""
echo "Paste these into your .env file (from .env.production.example)."
