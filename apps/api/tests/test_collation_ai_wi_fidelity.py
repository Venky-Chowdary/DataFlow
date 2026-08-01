"""Accent/width-insensitive collation equality — SQL Server / MySQL engine class."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.reconciliation import normalize_cell  # noqa: E402
from services.type_system import (  # noqa: E402
    fold_diacritics,
    is_accent_insensitive_collation,
    is_case_insensitive_collation,
    is_kana_insensitive_collation,
    is_width_insensitive_collation,
    unique_equality_key,
)


def test_ai_collation_detection():
    assert is_accent_insensitive_collation(
        "NVARCHAR(50) COLLATE Latin1_General_CI_AI"
    )
    assert is_accent_insensitive_collation(
        "VARCHAR(50) COLLATE utf8mb4_0900_ai_ci"
    )
    assert is_accent_insensitive_collation(
        "VARCHAR(50) COLLATE utf8mb4_general_ci"
    )
    assert is_accent_insensitive_collation(
        "VARCHAR(50) COLLATE utf8mb4_unicode_ci"
    )
    # Accent-sensitive must not fold — else Validate false-blocks cafe/café.
    assert not is_accent_insensitive_collation(
        "NVARCHAR(50) COLLATE Latin1_General_CI_AS"
    )
    assert not is_accent_insensitive_collation(
        "VARCHAR(50) COLLATE utf8mb4_0900_as_ci"
    )
    assert not is_accent_insensitive_collation("CITEXT")
    assert not is_accent_insensitive_collation("VARCHAR(50)")


def test_ci_as_still_casefolds_without_accent_fold():
    ddl = "NVARCHAR(50) COLLATE Latin1_General_CI_AS"
    assert is_case_insensitive_collation(ddl)
    assert not is_accent_insensitive_collation(ddl)
    assert unique_equality_key("Cafe", ddl) == unique_equality_key("cafe", ddl)
    assert unique_equality_key("Cafe", ddl) != unique_equality_key("Café", ddl)


def test_ci_ai_equates_accented_keys():
    ddl = "NVARCHAR(100) COLLATE Latin1_General_100_CI_AI"
    assert fold_diacritics("Café") == "Cafe" or fold_diacritics("Café").casefold() == "cafe"
    assert unique_equality_key("Cafe", ddl) == unique_equality_key("Café", ddl)
    assert unique_equality_key("JEREMIE", ddl) == unique_equality_key("Jérémie", ddl)
    assert normalize_cell("Café", ddl_type=ddl) == normalize_cell("Cafe", ddl_type=ddl)


def test_mysql_ai_ci_and_as_ci_polarity():
    ai = "VARCHAR(50) COLLATE utf8mb4_0900_ai_ci"
    as_ci = "VARCHAR(50) COLLATE utf8mb4_0900_as_ci"
    assert unique_equality_key("resume", ai) == unique_equality_key("résumé", ai)
    assert unique_equality_key("resume", as_ci) != unique_equality_key("résumé", as_ci)


def test_width_insensitive_fullwidth_fold():
    # SQL Server: omitting _WS means width-insensitive (MS collation docs).
    ddl = "NVARCHAR(20) COLLATE Latin1_General_100_CI_AI"
    assert is_width_insensitive_collation(ddl)
    assert not is_width_insensitive_collation(
        "NVARCHAR(20) COLLATE Latin1_General_100_CI_AI_WS"
    )
    # Fullwidth Latin 'Ａ' (U+FF21) → 'A'
    assert unique_equality_key("Ａbc", ddl) == unique_equality_key("Abc", ddl)


def test_kana_insensitive_hiragana_katakana_fold():
    ddl = "NVARCHAR(20) COLLATE Japanese_CI_AS"
    assert is_kana_insensitive_collation(ddl)
    assert not is_kana_insensitive_collation(
        "NVARCHAR(20) COLLATE Japanese_CI_AS_KS"
    )
    # あ (hiragana) ≡ ア (katakana) when KS omitted
    assert unique_equality_key("あ", ddl) == unique_equality_key("ア", ddl)
    assert unique_equality_key("あ", "NVARCHAR(20) COLLATE Japanese_CI_AS_KS") != (
        unique_equality_key("ア", "NVARCHAR(20) COLLATE Japanese_CI_AS_KS")
    )


def test_integrity_blocks_ai_collation_accent_dupes():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "name", "target": "name"}],
        [{"name": "Cafe"}, {"name": "Café"}],
        "strict",
        dest_kind="sqlserver",
        primary_key="name",
        sync_mode="append",
        destination_unique_keys=[{"name": "uq_name", "columns": ["name"]}],
        target_types={"name": "NVARCHAR(100) COLLATE Latin1_General_CI_AI"},
    )
    assert result["passed"] is False
    assert result["blocks_transfer"] is True


def test_integrity_allows_accent_dupes_on_ci_as():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "name", "target": "name"}],
        [{"name": "Cafe"}, {"name": "Café"}],
        "strict",
        dest_kind="sqlserver",
        primary_key="name",
        sync_mode="append",
        destination_unique_keys=[{"name": "uq_name", "columns": ["name"]}],
        target_types={"name": "NVARCHAR(100) COLLATE Latin1_General_CI_AS"},
    )
    assert result["passed"] is True
