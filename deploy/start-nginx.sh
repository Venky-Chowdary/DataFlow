#!/bin/sh
set -e

PORT=${PORT:-80}
export PORT

# Replace the template variable so nginx listens on the correct port.
envsubst '$PORT' < /etc/nginx/conf.d/default.conf > /etc/nginx/conf.d/default.conf.tmp
mv /etc/nginx/conf.d/default.conf.tmp /etc/nginx/conf.d/default.conf

# Inject the API base URL at runtime so the static bundle can reach the
# correct backend without rebuilding when the domain changes.
DATAFLOW_API_BASE=${DATAFLOW_API_BASE:-${VITE_API_BASE:-/api/v1}}
sed -i "s|</head>|<script>window.DATAFLOW_API_BASE=\"$DATAFLOW_API_BASE\"</script></head>|" /usr/share/nginx/html/index.html

nginx -g 'daemon off;'
