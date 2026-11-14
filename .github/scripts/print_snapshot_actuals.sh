#!/usr/bin/env bash
# Print each failed visual comparison's captured image (first attempt only) into
# the job log, base64 in 8000-character chunks between markers. A baseline that
# changed on purpose can then be refreshed from CI's own pixels, by anyone who
# can read the log, without downloading the run's artifacts. Prints nothing when
# no comparison failed.
set -euo pipefail
results="${1:-apps/web/test-results}"
[ -d "$results" ] || exit 0
total=0
while IFS= read -r -d '' png; do
  case "$png" in *-retry[0-9]*/*) continue ;; esac
  size=$(stat -c %s "$png")
  total=$((total + size))
  if [ "$size" -gt 2000000 ] || [ "$total" -gt 12000000 ]; then
    echo "DW-SNAPSHOT-SKIP $(basename "$png") ($size bytes)"
    continue
  fi
  name=$(basename "$png" | sed 's/-actual\.png$//')
  echo "DW-SNAPSHOT-BEGIN $name $(sha256sum "$png" | cut -c1-64)"
  base64 -w 8000 "$png" | sed 's/^/DW-SNAPSHOT-DATA /'
  echo "DW-SNAPSHOT-END $name"
done < <(find "$results" -name '*-actual.png' -print0 | sort -z)
