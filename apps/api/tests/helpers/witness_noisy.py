"""A synthetic, multi-value benchmark for the Mapping Witness, with real-world drift in the destination.

SYNTHETIC: generated from a fixed seed, written for the witness's own floors.
It is not the held-out mapping benchmark and no published accuracy number is
taken from it. Every column carries at least 30 distinct values (a column with
one value proves nothing), and the destination copy carries the noise a real
existing table has: trailing spaces, other letter case, other numeric
spellings, the same instant in another time zone, and a few stale rows.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

SEED = 20261122
ROWS = 120
STALE_SHARE = 0.04

_FIRST = ["Ann", "Bob", "Cy", "Dee", "Eve", "Fay", "Gus", "Hal", "Ivy", "Jo", "Kai", "Lea", "Max", "Ned", "Oli",
          "Pam", "Quin", "Ray", "Sue", "Tom", "Uma", "Vic", "Wes", "Xia", "Yan", "Zoe", "Abe", "Bea", "Cal", "Dot",
          "Eli", "Flo"]
_LAST = ["Kim", "Lo", "Moe", "Ng", "Ott", "Pyle", "Quon", "Ruiz", "Saar", "Toft", "Ueda", "Vance", "Wolk", "Xu",
         "Yates", "Zane", "Abbo", "Bryn", "Cobb", "Dunn", "Egan", "Fisk", "Gale", "Hume", "Ivey", "Jenks", "Kerr",
         "Lund", "Mott", "Nash", "Orr", "Pike"]
_CITIES = ["Austin", "Leeds", "Lyon", "Porto", "Kyoto", "Accra", "Quito", "Perth", "Oslo", "Riga", "Turin", "Cork",
           "Basel", "Graz", "Split", "Lille", "Malmo", "Bergen", "Brno", "Gent", "Delft", "Aarhus", "Tartu", "Pecs",
           "Kosice", "Sibiu", "Ohrid", "Varna", "Nis", "Ruse", "Kotor", "Bled"]


def _tables(rng: random.Random) -> list[dict]:
    base = datetime(2024, 1, 1, 8, 0, tzinfo=timezone.utc)
    people, orders = [], []
    for i in range(ROWS):
        people.append({
            "pid": 5000 + i,
            "fname": rng.choice(_FIRST) + str(i % 7),
            "lname": rng.choice(_LAST) + str(i % 5),
            "mail": f"user{i}@example.com",
            "town": rng.choice(_CITIES),
            "joined": (base + timedelta(hours=i * 37)).isoformat(),
        })
        cents = rng.randint(100, 900_000)
        orders.append({
            "oid": 90_000 + i,
            "total": f"{Decimal(cents) / 100:.2f}",
            "tax": f"{Decimal(rng.randint(10, 9000)) / 100:.2f}",
            "qty": rng.randint(1, 400),
            "ref": f"R{rng.randint(10_000, 99_999)}",
            "placed": (base + timedelta(minutes=i * 613)).isoformat(),
        })
    return [
        {"id": "people", "key": ("pid", "person_id"), "rows": people,
         "gold": {"pid": "person_id", "fname": "first_name", "lname": "last_name", "mail": "email",
                  "town": "city", "joined": "joined_at"}},
        {"id": "orders", "key": ("oid", "order_id"), "rows": orders,
         "gold": {"oid": "order_id", "total": "order_total", "tax": "tax_amount", "qty": "quantity",
                  "ref": "reference", "placed": "placed_at"}},
    ]


def _noisy(value, rng: random.Random):
    """The same value as another system stored it."""
    if isinstance(value, str) and value.count("T") == 1 and value[:4].isdigit():
        at = datetime.fromisoformat(value)
        return at.astimezone(timezone(timedelta(hours=rng.choice([-5, 2, 9])))).isoformat()
    if isinstance(value, str) and value.replace(".", "", 1).isdigit():
        return rng.choice([Decimal(value), float(value), value + "0"])
    if isinstance(value, str):
        return rng.choice([value + "  ", value.upper(), value.lower(), value])
    if isinstance(value, int):
        return rng.choice([value, str(value), Decimal(value)])
    return value


def build(seed: int = SEED) -> list[dict]:
    """Tables with source rows, destination rows (noisy, a few stale) and the correct mapping."""
    rng = random.Random(seed)
    out = []
    for table in _tables(rng):
        skey, tkey = table["key"]
        dest = []
        for row in table["rows"]:
            d = {}
            for s, t in table["gold"].items():
                d[t] = row[s] if s == skey else _noisy(row[s], rng)
            if rng.random() < STALE_SHARE:  # a row another system changed since
                stale_col = rng.choice([t for s, t in table["gold"].items() if s != skey])
                d[stale_col] = f"stale-{rng.randint(0, 10**6)}"
            dest.append(d)
        out.append({**table, "dest": dest})
    return out
