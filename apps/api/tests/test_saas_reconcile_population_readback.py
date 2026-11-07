"""A hosted read-back covers the whole destination, or says it could not.

The SaaS/Kafka Gate-8 verifiers asked their connector for ``limit or 500``
rows. ``limit=0`` is the caller saying "the whole destination population", so
every CRM object holding more than 500 records was hashed as its first 500 and
compared against the source's whole-table digest: a strict reconcile then
reported a checksum mismatch for a load where every row landed intact, and
returned 500 as the destination count for a population nobody counted.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services import reconciliation as rc

CEILING = rc._SAAS_RECONCILE_MAX_ROWS


def _batch(rows: int):
    return SimpleNamespace(
        headers=["id", "email"],
        rows=[{"id": str(i), "email": f"u{i}@x.com"} for i in range(rows)],
    )


def _hubspot(*, population: int, limit: int):
    """Run the HubSpot verifier against a stub object of *population* rows."""
    asked: dict[str, int] = {}

    def read_object(*, cfg, object, limit, **_kw):  # noqa: A002 — connector API
        asked["limit"] = int(limit)
        return _batch(min(population, int(limit)))

    with patch.dict(
        sys.modules,
        {"connectors.hubspot": SimpleNamespace(read_object=read_object)},
    ):
        count, checksum = rc.verify_hubspot_object(
            password="tok",
            object_name="contacts",
            target_columns=["id", "email"],
            limit=limit,
        )
    return count, checksum, asked["limit"]


def test_a_whole_population_readback_is_not_capped_at_500():
    count, checksum, asked = _hubspot(population=1200, limit=0)
    assert asked > 500, "limit=0 must not be read back as a 500-row sample"
    assert count == 1200, "destination count must be the population, not the cap"
    assert checksum


def test_the_digest_of_a_capped_read_is_the_digest_of_the_whole_object():
    """Same 1,200 rows, read whole: the old 500-row prefix hashed differently."""
    whole = _hubspot(population=1200, limit=0)[1]
    prefix = _hubspot(population=1200, limit=500)[1]
    assert whole and prefix
    assert whole != prefix, "a prefix digest must not be mistaken for the population"


def test_a_population_past_the_ceiling_is_unproven_not_a_prefix_digest():
    count, checksum, _asked = _hubspot(population=CEILING + 5, limit=0)
    assert (count, checksum) == (-1, ""), (
        "a read-back that could not see the whole population has no verdict; "
        "reporting its prefix digest either fails a correct load or passes a partial one"
    )


def test_a_caller_scoped_limit_is_honoured_exactly():
    count, checksum, asked = _hubspot(population=1200, limit=50)
    assert (asked, count) == (50, 50)
    assert checksum


def test_kafka_and_saas_share_the_rule():
    assert rc._saas_readback_limit(0) > CEILING
    assert rc._saas_readback_limit(37) == 37
    assert rc._saas_readback_truncated("Kafka", 0, CEILING + 1) is True
    assert rc._saas_readback_truncated("Kafka", 0, CEILING) is False
    # A scoped read is truncated by the caller's own request, not by us.
    assert rc._saas_readback_truncated("Kafka", 10, 10) is False
