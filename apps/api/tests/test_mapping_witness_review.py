"""Mapping Witness, independent review findings 5 and 6: prefer silence to a wrong flag.

Each case was reproduced against the first version of the witness: a wrong
mapping read "witnessed", a correct one was flagged, or a hostile cell crashed
or stalled the check.
"""

from __future__ import annotations

import random
import time
from datetime import datetime, timedelta, timezone

from services.mapping_witness import (
    KIND_UNIT,
    MODE_PROFILE,
    STATUS_CONSISTENT,
    STATUS_FLAGGED,
    STATUS_NO_EVIDENCE,
    STATUS_NOT_COMPARABLE,
    STATUS_WITNESSED,
    norm,
    witness_mappings,
)

KEY = ("id", "id")


def _pair(report, source):
    return next(p for p in report["pairs"] if p["source"] == source)


def _countries(n=100, seed=7):
    rng = random.Random(seed)
    return [rng.choice(["US", "GB", "DE", "FR", "IN", "BR"]) for _ in range(n)]


# 5a ─ "witnessed" needs κ and no strictly better column ─────────────────────


def test_a_better_matching_column_keeps_a_pair_from_being_witnessed():
    ship = _countries()
    bill = list(ship)
    for i in range(0, 90, 10):  # 9 of 100 billed elsewhere
        bill[i] = "JP"
    src = [{"id": i, "ship_country": c} for i, c in enumerate(ship)]
    dst = [{"id": i, "ship_country": s, "bill_country": b} for i, (s, b) in enumerate(zip(ship, bill))]
    report = witness_mappings([{"source": "ship_country", "target": "bill_country"}], src, dst, key=KEY)
    pair = _pair(report, "ship_country")
    assert pair["status"] == STATUS_CONSISTENT, pair
    assert pair["evidence"]["best_other_column"]["target"] == "ship_country"


def test_constant_columns_prove_nothing():
    src = [{"id": i, "discount": 0} for i in range(40)]
    dst = [{"id": i, "discount": 0, "tax": 0} for i in range(40)]
    pair = _pair(witness_mappings([{"source": "discount", "target": "tax"}], src, dst, key=KEY), "discount")
    assert pair["status"] == STATUS_CONSISTENT and pair["flag"] is None


# 5b ─ a date compared with an instant uses its day; the loader's own order is never flagged ──


def test_slash_dates_into_a_timestamp_in_the_run_order_are_witnessed():
    base = datetime(2024, 1, 1, 9, 30)  # noqa: DTZ001 - a TIMESTAMP without zone, as the destination holds it
    src, dst = [], []
    for i in range(30):
        at = base + timedelta(days=i * 3, hours=i)
        src.append({"id": i, "created": at.strftime("%d/%m/%Y")})
        dst.append({"id": i, "created_at": at})
    report = witness_mappings([{"source": "created", "target": "created_at"}], src, dst, key=KEY, date_locale="DMY")
    pair = _pair(report, "created")
    assert pair["flag"] is None and pair["status"] == STATUS_WITNESSED, pair


# 5c ─ sort codes are not dates ──────────────────────────────────────────────


def test_sort_codes_are_not_flagged_as_mixed_dates():
    codes = ["40-47-84", "20-00-00", "60-16-13", "30-99-50", "11-22-33", "40-47-84", "77-12-31"]
    src = [{"id": i, "sort_code": c} for i, c in enumerate(codes * 3)]
    dst = [{"id": i, "sort_code": c} for i, c in enumerate(codes * 3)]
    keyed = witness_mappings([{"source": "sort_code", "target": "sort_code"}], src, dst, key=KEY)
    assert keyed["summary"]["flagged"] == 0, keyed["pairs"]
    before = witness_mappings([{"source": "sort_code", "target": "sort_code", "target_type": "DATE"}], src)
    assert before["summary"]["flagged"] == 0, before["pairs"]


# 5d ─ profile mode flags only a swap that points both ways ───────────────────


def test_new_keys_overlapping_another_column_are_not_flagged_by_profile():
    src = [{"order_id": 500 + i, "customer_id": 9000 + i} for i in range(1, 21)]
    dst = [{"order_id": i, "customer_id": 480 + i} for i in range(1, 41)]
    report = witness_mappings([{"source": "order_id", "target": "order_id"},
                               {"source": "customer_id", "target": "customer_id"}], src, dst)
    assert report["mode"] == MODE_PROFILE
    assert report["summary"]["flagged"] == 0, report["pairs"]
    assert _pair(report, "order_id")["status"] == STATUS_NO_EVIDENCE


def test_profile_mode_still_flags_a_mutual_swap():
    rng = random.Random(3)
    first = [rng.choice(["Ann", "Bob", "Cy", "Dee", "Eve", "Fay", "Gus"]) for _ in range(30)]
    last = [rng.choice(["Kim", "Lo", "Moe", "Ng", "Ott", "Pyle", "Quon"]) for _ in range(30)]
    src = [{"f": a, "l": b} for a, b in zip(first, last)]
    dst = [{"first_name": a, "last_name": b} for a, b in zip(reversed(first), reversed(last))]
    report = witness_mappings([{"source": "f", "target": "last_name"}, {"source": "l", "target": "first_name"}],
                              src, dst)
    assert {p["source"] for p in report["pairs"] if p["status"] == STATUS_FLAGGED} == {"f", "l"}


# 5e ─ a column about an email is not an email column ─────────────────────────


def test_email_metadata_columns_are_not_email_columns():
    rows = [{"d": "example.com", "s": "verified", "n": "weekly"} for _ in range(40)]
    report = witness_mappings([{"source": "d", "target": "email_domain"}, {"source": "s", "target": "email_status"},
                               {"source": "n", "target": "email_notifications"}], rows)
    assert report["summary"]["flagged"] == 0, report["pairs"]
    still = witness_mappings([{"source": "d", "target": "work_email"}], rows)
    assert still["summary"]["flagged"] == 1


# 5f ─ destination column names are compared without case ────────────────────


def test_an_upper_case_destination_column_is_the_same_column():
    names = ["Ann", "Bob", "Cy", "Dee", "Eve", "Fay"]
    src = [{"id": i, "name": names[i % 6]} for i in range(30)]
    dst = [{"ID": i, "NAME": names[i % 6], "CITY": "x" + str(i % 4)} for i in range(30)]
    pair = _pair(witness_mappings([{"source": "name", "target": "name"}], src, dst, key=KEY), "name")
    assert pair["status"] == STATUS_WITNESSED and pair["flag"] is None, pair


# 5g ─ wording, cents, transforms, booleans, time zones ──────────────────────


def test_a_percentage_scale_is_not_called_dollars_into_cents():
    src = [{"id": i, "ratio": f"0.{i + 10}"} for i in range(30)]
    dst = [{"id": i, "rate_pct": i + 10} for i in range(30)]
    pair = _pair(witness_mappings([{"source": "ratio", "target": "rate_pct"}], src, dst, key=KEY), "ratio")
    assert pair["flag"]["kind"] == KIND_UNIT
    assert "dollars" not in pair["flag"]["reason"] and "scale" in pair["flag"]["reason"]


def test_cents_into_an_integer_amount_column_is_not_flagged_before_load():
    rows = [{"amount_cents": 1000 + 37 * i} for i in range(20)]
    report = witness_mappings([{"source": "amount_cents", "target": "amount", "target_type": "BIGINT"}], rows)
    assert report["summary"]["flagged"] == 0
    still = witness_mappings([{"source": "amount_cents", "target": "amount", "target_type": "DECIMAL(12,2)"}], rows)
    assert still["summary"]["flagged"] == 1


def test_masked_or_hashed_columns_are_not_comparable_never_differs():
    src = [{"id": i, "ssn": f"123-45-{i:04d}"} for i in range(20)]
    dst = [{"id": i, "ssn": "***-**-" + f"{i:04d}"} for i in range(20)]
    for transform in ("mask", "hash_sha256", "redact"):
        pair = _pair(witness_mappings([{"source": "ssn", "target": "ssn", "transform": transform}],
                                      src, dst, key=KEY), "ssn")
        assert pair["status"] == STATUS_NOT_COMPARABLE and "transforms" in pair["reason"], (transform, pair)


def test_one_and_zero_into_a_boolean_column_agree():
    src = [{"id": i, "active": "1" if i % 3 else "0"} for i in range(30)]
    dst = [{"id": i, "is_active": bool(i % 3)} for i in range(30)]
    pair = _pair(witness_mappings([{"source": "active", "target": "is_active"}], src, dst, key=KEY), "active")
    assert pair["status"] == STATUS_WITNESSED, pair


def test_the_same_instant_in_another_time_zone_agrees():
    base = datetime(2024, 3, 1, 8, 0, tzinfo=timezone.utc)
    plus2 = timezone(timedelta(hours=2))
    src = [{"id": i, "ts": (base + timedelta(hours=i * 7)).astimezone(plus2).isoformat()} for i in range(30)]
    dst = [{"id": i, "ts": (base + timedelta(hours=i * 7)).replace(tzinfo=None)} for i in range(30)]
    pair = _pair(witness_mappings([{"source": "ts", "target": "ts"}], src, dst, key=KEY), "ts")
    assert pair["status"] == STATUS_WITNESSED, pair


# 6 ─ hostile cells are bounded ──────────────────────────────────────────────


def test_huge_exponents_neither_crash_nor_stall():
    assert norm("1e1000000") == "1e1000000"
    assert norm("1e999999") == "1e999999"
    assert norm("9" * 5000).startswith("9") and len(norm("9" * 5000)) <= 512
    src = [{"id": i, "v": "1e999999" if i % 2 else "1e1000000"} for i in range(500)]
    dst = [{"id": i, "v": "1e999999"} for i in range(500)]
    started = time.monotonic()
    report = witness_mappings([{"source": "v", "target": "v"}], src, dst, key=KEY)
    assert time.monotonic() - started < 2.0
    assert report["summary"]["pairs"] == 1
