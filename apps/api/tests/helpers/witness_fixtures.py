"""Deterministic datasets for the Mapping Witness (and the ai-benchmarks witness suite).

One customer table, generated from a fixed seed, in two shapes:

* ``source_rows`` — what a legacy system exports: ``given``, ``family``,
  ``mail``, ``login``, ``joined`` (day-first ``dd/mm/yyyy`` text) and
  ``balance`` (dollars with cents, as text).
* ``dest_rows`` — the same customers already present in the destination,
  loaded correctly by someone else: ``first_name``, ``last_name``, ``email``,
  ``username``, ``joined_on`` (ISO date), ``balance`` (dollars) and
  ``balance_cents`` (whole cents).

Names come from the witness lexicons' unambiguous halves, so the source-only
name-order check has something to read; nothing here was written from the
held-out mapping benchmark.
"""

from __future__ import annotations

import random
from decimal import Decimal

from services.mapping_witness_lexicon import GIVEN_ONLY, SURNAME_ONLY

SEED = 20261121
KEY = ("cust_id", "customer_id")

CORRECT_MAPPING = [
    {"source": "cust_id", "target": "customer_id"},
    {"source": "given", "target": "first_name"},
    {"source": "family", "target": "last_name"},
    {"source": "mail", "target": "email"},
    {"source": "login", "target": "username"},
    {"source": "joined", "target": "joined_on"},
    {"source": "balance", "target": "balance"},
]


def _remap(changes: dict[str, str]) -> list[dict[str, str]]:
    return [{"source": m["source"], "target": changes.get(m["source"], m["target"])} for m in CORRECT_MAPPING]


SWAPPED_NAMES = _remap({"given": "last_name", "family": "first_name"})
DOLLARS_INTO_CENTS = _remap({"balance": "balance_cents"})
USERNAME_INTO_EMAIL = _remap({"login": "email", "mail": "username"})
LOGIN_INTO_EMAIL_ONLY = [m for m in _remap({"login": "email"}) if m["source"] != "mail"]


def customers(n: int = 60, *, seed: int = SEED) -> tuple[list[dict], list[dict]]:
    rng = random.Random(seed)
    given = sorted(GIVEN_ONLY)
    family = sorted(SURNAME_ONLY)
    source: list[dict] = []
    dest: list[dict] = []
    for i in range(n):
        first = rng.choice(given).title()
        last = rng.choice(family).title()
        login = f"{first[0].lower()}{last.lower()}{rng.randint(1, 99)}"
        day, month, year = rng.randint(1, 28), rng.randint(1, 12), rng.randint(2015, 2025)
        cents = rng.randint(100, 999_999)
        dollars = Decimal(cents) / 100
        email = f"{first.lower()}.{last.lower()}{i}@example.com"
        source.append({
            "cust_id": 1000 + i,
            "given": first,
            "family": last,
            "mail": email,
            "login": login,
            "joined": f"{day:02d}/{month:02d}/{year}",
            "balance": f"{dollars:.2f}",
        })
        dest.append({
            "customer_id": 1000 + i,
            "first_name": first,
            "last_name": last,
            "email": email,
            "username": login,
            "joined_on": f"{year:04d}-{month:02d}-{day:02d}",
            "balance": dollars,
            "balance_cents": cents,
        })
    return source, dest
