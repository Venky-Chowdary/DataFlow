"""Unit pins for round-10 extras (end to end: test_r10b_key_collation_and_encoding_e2e)."""

from __future__ import annotations

import pytest

from services.csv_profiler import detect_encoding
from services.file_parser import FileParser

_TEXT = "id,name\r\n1,café €\r\n2,“q” – …\r\n3,Ærø\r\n"


@pytest.mark.parametrize(
    ("codec", "want"),
    [("utf-8", "utf-8"), ("utf-8-sig", "utf-8-sig"), ("utf-16", "utf-16"),
     ("utf-16-le", "utf-16-le"), ("utf-16-be", "utf-16-be"), ("cp1252", "cp1252")],
)
def test_detect_encoding(codec, want):
    assert detect_encoding(_TEXT.encode(codec)) == want


def test_utf8_prefix_cut_mid_character_is_still_utf8():
    body = ("a" + "é" * 40000).encode("utf-8")  # the 64 KiB prefix ends mid-character
    assert body[:65536].decode("utf-8", errors="ignore") != body[:65536].decode("latin-1")
    assert detect_encoding(body) == "utf-8"


def test_undefined_cp1252_byte_keeps_latin1_reading():
    assert detect_encoding(b"id,name\n1,a\x81b\n") == "latin-1"


@pytest.mark.parametrize("codec", ["cp1252", "utf-16", "utf-16-le"])
def test_parser_reads_legacy_encoded_csv_exactly(codec):
    result = FileParser().parse(_TEXT.encode(codec), "t.csv")
    assert result.success, result.error
    assert [r["name"] for r in result.data] == ["café €", "“q” – …", "Ærø"]


def test_created_key_collation_follows_the_source_equality():
    from services.created_key_collation import key_collation_suffix
    from services.source_engine_scope import bind_source_engine

    with bind_source_engine("s3"):
        assert key_collation_suffix("k", ["k"], ["NVARCHAR(450)"], "sqlserver") == [
            "COLLATE Latin1_General_BIN"]
        assert key_collation_suffix("k", ["k"], ["VARCHAR(768)"], "mysql") == [
            "COLLATE utf8mb4_bin"]
        assert key_collation_suffix("k", ["k"], ["BIGINT"], "sqlserver") == []
        assert key_collation_suffix(
            "k", ["k"], ["NVARCHAR(10) COLLATE Latin1_General_CI_AS"], "sqlserver") == []
    assert key_collation_suffix("k", ["k"], ["NVARCHAR(450)"], "sqlserver") == []


def test_key_oracle_only_for_non_binary_text_keys():
    from services.dest_key_oracle import engine_key_oracle

    cfg = {"type": "sqlserver"}
    eq_bin = {"k": (str, "NVARCHAR(450) COLLATE Latin1_General_BIN", [])}
    eq_ci = {"k": (str, "NVARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS", [])}
    eq_bare = {"k": (str, "NVARCHAR(100)", [])}
    assert engine_key_oracle(eq_bin, ["k"], dest_kind="sqlserver", destination_config=cfg) is None
    assert engine_key_oracle(eq_ci, ["k"], dest_kind="sqlserver", destination_config=cfg)
    assert engine_key_oracle(eq_bare, ["k"], dest_kind="sqlserver", destination_config=cfg)
    assert engine_key_oracle(eq_ci, ["k"], dest_kind="postgresql", destination_config=cfg) is None
    assert engine_key_oracle(eq_ci, ["k"], dest_kind="sqlserver", destination_config=None) is None
