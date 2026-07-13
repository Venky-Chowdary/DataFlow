#!/bin/sh
set -e

PORT=${PORT:-80}
export PORT

# Replace the template variable so nginx listens on the correct port.
envsubst '$PORT' < /etc/nginx/conf.d/default.conf > /etc/nginx/conf.d/default.conf.tmp
mv /etc/nginx/conf.d/default.conf.tmp /etc/nginx/conf.d/default.conf

nginx -g 'daemon off;'
