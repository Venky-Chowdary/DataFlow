"""Postgres NUMERIC and the S3 JSON file share one checksum for an uncast decimal.

The file keeps every digit. Verification must not read that JSON number as a
float and roll the load back.
"""

from __future__ import annotations

import io
import json
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.dest_precount import checksum_artifact_stream
from services.reconciliation import canonical_checksum_from_iter
from services.value_serializer import json_dumps_exact_numbers

_AMOUNT = Decimal("12345678901234567890.1234567890123456789")


def test_strict_checksum_matches_the_exact_json_file():
    row = {"id": 1, "amount": _AMOUNT}
    body = json_dumps_exact_numbers([row]).encode()
    count, digest = checksum_artifact_stream(
        io.BytesIO(body),
        name="orders.json",
        columns=["id", "amount"],
        dest_db_type="s3",
    )
    source = canonical_checksum_from_iter(
        iter([row]),
        ["id", "amount"],
        dest_db_type="postgresql",
        dest_types={"id": "INTEGER", "amount": "NUMERIC"},
    )
    assert count == 1
    assert digest == source
    floated = json.loads(body)
    floated_digest = canonical_checksum_from_iter(
        iter(floated),
        ["id", "amount"],
        dest_db_type="s3",
    )
    assert floated_digest != source
