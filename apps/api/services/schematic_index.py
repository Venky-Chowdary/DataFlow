"""
Schematic index — million-scale column name variant lookup.

Builds an inverted index: variant (lowercase) → canonical semantic form.
Used by the semantic mapper for O(1) abbreviation / alias resolution.
"""

from __future__ import annotations

import pickle
import re
from functools import lru_cache
from pathlib import Path

_CACHE_PATH = Path(__file__).resolve().parents[1] / "data" / "schematic_index.cache.pkl"

# Enterprise data-warehouse prefixes/suffixes seen in real schemas
_PREFIXES = (
    "src_", "dst_", "tgt_", "dim_", "fact_", "stg_", "raw_", "hist_", "curr_",
    "prev_", "bk_", "lk_", "fk_", "pk_", "ref_", "tmp_", "wrk_", "ods_", "dwh_",
    "mart_", "rpt_", "agg_", "sum_", "avg_", "cnt_", "max_", "min_", "tot_",
    "net_", "gross_", "orig_", "new_", "old_", "cur_", "prv_", "nxt_", "lst_",
    "fst_", "lcl_", "glb_", "ext_", "int_", "sys_", "usr_", "app_", "db_",
    "tbl_", "col_", "fld_", "attr_", "prop_", "val_", "key_", "id_", "num_",
    "cd_", "typ_", "cat_", "sub_", "meta_", "aux_", "sup_", "alt_", "pri_",
    "sec_", "ter_", "lvl_", "tier_", "grp_", "seg_", "cls_", "typ_", "stat_",
    "flg_", "ind_", "sw_", "bit_", "yn_", "is_", "has_", "can_", "was_",
    "billing_", "shipping_", "mailing_", "home_", "work_", "primary_", "secondary_",
    "customer_", "order_", "product_", "vendor_", "employee_", "account_",
    "transaction_", "payment_", "invoice_", "shipment_", "inventory_", "sales_",
    "marketing_", "finance_", "hr_", "legal_", "compliance_", "audit_",
)

_SUFFIXES = (
    "_id", "_key", "_code", "_num", "_no", "_nbr", "_name", "_desc", "_type",
    "_status", "_flag", "_ind", "_dt", "_date", "_ts", "_time", "_amt", "_qty",
    "_cnt", "_val", "_pct", "_rate", "_score", "_rank", "_seq", "_idx",
    "_txt", "_str", "_json", "_blob", "_hash", "_uuid", "_guid", "_ref",
    "_1", "_2", "_3", "_01", "_02", "_03", "_v1", "_v2", "_old", "_new",
)


def _normalize(name: str) -> str:
    s = name.strip()
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


@lru_cache(maxsize=1)
def _build_index() -> dict[str, str]:
    """Build inverted index variant → canonical. Target ~1M entries."""
    if _CACHE_PATH.exists():
        try:
            with open(_CACHE_PATH, "rb") as f:
                cached = pickle.load(f)
            if isinstance(cached, dict) and len(cached) > 10_000:
                return cached
        except Exception:
            pass

    index = _build_index_fresh()
    try:
        _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(_CACHE_PATH, "wb") as f:
            pickle.dump(index, f)
    except Exception:
        pass
    return index


def _build_index_fresh() -> dict[str, str]:
    """Compute schematic index from synonym dictionaries."""
    index: dict[str, str] = {}

    try:
        from src.ai.knowledge.synonyms import SYNONYM_DICTIONARY, TOKEN_ABBREVIATIONS
    except ImportError:
        try:
            from ai.knowledge.synonyms import SYNONYM_DICTIONARY, TOKEN_ABBREVIATIONS
        except ImportError:
            return index

    def add(variant: str, canonical: str) -> None:
        v = _normalize(variant)
        if v and len(v) <= 128:
            index.setdefault(v, canonical)

    for canonical, syns in SYNONYM_DICTIONARY.items():
        add(canonical, canonical)
        for s in syns:
            add(s, canonical)
        for prefix in _PREFIXES:
            add(f"{prefix}{canonical}", canonical)
            for s in syns[:8]:
                add(f"{prefix}{s}", canonical)
            for suffix in _SUFFIXES:
                add(f"{prefix}{canonical}{suffix}", canonical)
                for s in syns[:4]:
                    add(f"{prefix}{s}{suffix}", canonical)
        for suffix in _SUFFIXES:
            add(f"{canonical}{suffix}", canonical)

    for abbr, full in TOKEN_ABBREVIATIONS.items():
        add(abbr, full)
        add(full, full)
        for prefix in _PREFIXES[:40]:
            add(f"{prefix}{abbr}", full)
            add(f"{prefix}{full}", full)
            for suffix in _SUFFIXES[:20]:
                add(f"{prefix}{abbr}{suffix}", full)
                add(f"{prefix}{full}{suffix}", full)

    # Numeric suffix variants for high-volume enterprise schemas
    for canonical in list(SYNONYM_DICTIONARY.keys())[:200]:
        for n in range(500):
            add(f"{canonical}_{n}", canonical)
            add(f"{canonical}{n}", canonical)
            add(f"{canonical}_v{n}", canonical)
            if n < 50:
                for prefix in _PREFIXES[:15]:
                    add(f"{prefix}{canonical}_{n}", canonical)

    return index


def lookup_schematic(name: str) -> str | None:
    """Return canonical form if name is a known schematic variant."""
    return _build_index().get(_normalize(name))


def schematic_count() -> int:
    return len(_build_index())


def schematic_match_boost(source: str, target: str) -> float | None:
    """High-confidence score when both columns resolve to same canonical via index."""
    src_canon = lookup_schematic(source)
    tgt_canon = lookup_schematic(target)
    if src_canon and tgt_canon and src_canon == tgt_canon:
        return 0.97
    if src_canon and _normalize(target) == src_canon:
        return 0.95
    if tgt_canon and _normalize(source) == tgt_canon:
        return 0.95
    return None
