"""D189 — the schematic lookup answers without building the 2.5M-variant index.

The first Map onto an existing table built every variant in Python (~15s CPU,
fresh after each deploy) inside the 45s tool deadline. ``lookup_schematic`` now
resolves the one name it is asked about. It must give the answer the
materialised index gave: the first ``(variant, canonical)`` pair the
enumeration emits. (An exhaustive run over all 2,523,432 variants matched;
here every variant of a strided sample, every bare term and near misses.)
"""

from __future__ import annotations

import time

import pytest

import services.schematic_index as si


@pytest.fixture(scope="module")
def index() -> dict[str, str]:
    return si._build_index_fresh()


def test_dictionary_terms_are_normalised_so_concatenation_is_the_variant():
    """The resolver splits a variant into prefix + term + suffix; that only
    reproduces ``_normalize(prefix + term + suffix)`` for normalised terms."""
    synonyms, abbreviations = si._dictionaries()
    terms = list(synonyms) + [s for v in synonyms.values() for s in v]
    terms += list(abbreviations) + list(abbreviations.values())
    assert terms
    assert [t for t in terms if not t or si._normalize(t) != t] == []
    assert all(si._normalize(p) + "_" == p for p in si._PREFIXES)
    assert all("_" + si._normalize(x) == x for x in si._SUFFIXES)


def test_lookup_matches_the_materialised_index(index):
    items = list(index.items())
    sample = items[::97] + [(k, v) for k, v in items if "_" not in k]
    misses = [(k, v, si._resolve(k)) for k, v in sample if si._resolve(k) != v]
    assert misses == []


def test_near_misses_are_not_variants(index):
    probes = []
    for key in list(index)[::4001]:
        probes += [key + "x", key + "_q", "zz_" + key, key[:-1], key + "_500", key + "_05"]
    wrong = [
        (p, index.get(si._normalize(p)), si.lookup_schematic(p))
        for p in probes
        if index.get(si._normalize(p)) != si.lookup_schematic(p)
    ]
    assert wrong == []


def test_a_cold_lookup_builds_nothing(monkeypatch):
    for name in ("_vocabulary", "_lookup_normalized"):
        getattr(si, name).cache_clear()
    monkeypatch.setattr(si, "_build_index_fresh", lambda: pytest.fail("built the index"))
    started = time.monotonic()
    assert si.lookup_schematic("stg_cust_id") == "customer_id"
    assert si.lookup_schematic("Dim_CustomerID") == "customer_id"
    assert si.lookup_schematic("customer_id_499") == "customer_id"
    assert si.lookup_schematic("customer_id_500") is None
    assert si.lookup_schematic("") is None
    assert si.lookup_schematic("x" * 129) is None
    assert time.monotonic() - started < 2.0
