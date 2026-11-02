"""Encoding capacity is physical storage of Unicode scalars, not a charset name.

MySQL utf8 is BMP. Oracle UTF8 is CESU-8. PostgreSQL UTF8 is Unicode.
DMS substitutes characters. These tests pin the classifier, CESU-8
recompose, bind refusal, and the fidelity aspect so the certificate cannot
say carried for utf8mb3 that would store '?'.
"""

from __future__ import annotations

import pytest

from connectors.writer_common import quarantine_unfit_strings
from services.encoding_capacity import (
    bind_unicode_text,
    cell_encoding,
    classify_capacity,
    compose_unicode_scalars,
    decide_encoding,
    decode_source_bytes,
    looks_like_cesu8,
)
from services.schema_fidelity import SourceSchemaCatalog, plan_create_new_fidelity

# U+1F600 GRINNING FACE — UTF-8 F0 9F 98 80; CESU-8 ED A0 BD ED B8 80
EMOJI = "\U0001f600"
CESU8_EMOJI = bytes.fromhex("eda0bdedb880")
UTF8_EMOJI = EMOJI.encode("utf-8")
SURROGATE_LEAK = "\ud83d\ude00"  # UTF-16 units of U+1F600


def test_mysql_utf8_is_bmp_not_unicode():
    cap = classify_capacity("mysql", "VARCHAR(32) CHARACTER SET utf8")
    assert cap.form == "utf8mb3"
    cap4 = classify_capacity("mysql", "VARCHAR(32) CHARACTER SET utf8mb4")
    assert cap4.form == "utf8"
    cap3 = classify_capacity("mariadb", "TEXT COLLATE utf8_general_ci")
    assert cap3.form == "utf8mb3"


def test_oracle_utf8_is_cesu8_al32utf8_is_utf8():
    assert classify_capacity("oracle", "VARCHAR2(100)", "UTF8").form == "cesu8"
    assert classify_capacity("oracle", "VARCHAR2(100)", "AL32UTF8").form == "utf8"
    assert classify_capacity("", "TIMESTAMP WITH TIME ZONE").form != "cesu8"


def test_mysql_latin1_is_cp1252_so_euro_fits():
    cap = classify_capacity("mysql", "VARCHAR(10) CHARACTER SET latin1")
    assert cap.form == "cp1252"
    assert cap.codec == "cp1252"
    assert bind_unicode_text("€", engine="mysql", dest_type="VARCHAR CHARACTER SET latin1") == "€"


def test_iso_latin1_rejects_euro():
    with pytest.raises(ValueError, match="U\\+20AC"):
        bind_unicode_text(
            "€",
            engine="postgresql",
            dest_type="VARCHAR CHARACTER SET latin1",
        )


def test_cesu8_bytes_recompose_to_scalar_not_invalid_utf8():
    assert looks_like_cesu8(CESU8_EMOJI)
    assert not looks_like_cesu8(UTF8_EMOJI)
    assert decode_source_bytes(CESU8_EMOJI) == EMOJI
    assert decode_source_bytes(UTF8_EMOJI) == EMOJI
    assert UTF8_EMOJI == b"\xf0\x9f\x98\x80"
    with pytest.raises(UnicodeDecodeError):
        CESU8_EMOJI.decode("utf-8")


def test_surrogate_pair_leak_recomposes():
    assert compose_unicode_scalars(SURROGATE_LEAK) == EMOJI
    bound = bind_unicode_text(
        SURROGATE_LEAK, engine="postgresql", dest_type="TEXT"
    )
    assert bound == EMOJI
    assert bound.encode("utf-8") == UTF8_EMOJI


def test_unpaired_surrogate_refuses_fffd_invent():
    with pytest.raises(ValueError, match="unpaired"):
        compose_unicode_scalars("\ud83d")


def test_ill_formed_bytes_refuse_latin1_invent():
    with pytest.raises(ValueError, match="latin-1"):
        decode_source_bytes(b"\xed\xa0\xbd")  # lone CESU-8 high surrogate


def test_emoji_to_utf8mb3_is_unsupported_not_carried():
    decision = decide_encoding(
        source_engine="postgresql",
        source_type="TEXT",
        dest_engine="mysql",
        dest_type="VARCHAR(32) CHARACTER SET utf8mb3",
        source_column="txt",
        dest_column="txt",
    )
    assert decision is not None
    assert decision.status == "unsupported"
    assert decision.dest_capacity is not None
    assert decision.dest_capacity.form == "utf8mb3"


def test_pg_text_to_mysql_utf8mb4_is_carried():
    decision = decide_encoding(
        source_engine="postgresql",
        source_type="TEXT",
        dest_engine="mysql",
        dest_type="VARCHAR(32)",
        source_column="txt",
        dest_column="txt",
    )
    assert decision is not None
    assert decision.status == "carried"
    assert decision.dest_capacity is not None
    assert decision.dest_capacity.form == "utf8"


def test_oracle_cesu8_to_pg_utf8_is_carried_with_recompose():
    decision = decide_encoding(
        source_engine="oracle",
        source_type="VARCHAR2(100)",
        dest_engine="postgresql",
        dest_type="TEXT",
        source_charset="UTF8",
        source_column="txt",
        dest_column="txt",
    )
    assert decision is not None
    assert decision.status == "carried"
    assert "CESU-8" in decision.reason


def test_nvarchar_to_utf8mb3_is_unsupported():
    decision = decide_encoding(
        source_engine="sqlserver",
        source_type="NVARCHAR(50)",
        dest_engine="mysql",
        dest_type="VARCHAR(50) CHARACTER SET utf8",
        source_column="txt",
        dest_column="txt",
    )
    assert decision is not None
    assert decision.status == "unsupported"


def test_bind_emoji_to_utf8mb3_raises():
    with pytest.raises(ValueError, match="U\\+1F600"):
        bind_unicode_text(
            EMOJI,
            engine="mysql",
            dest_type="VARCHAR(32) CHARACTER SET utf8mb3",
        )


def test_fffd_is_prior_loss_and_still_a_character():
    cell = cell_encoding("bad\ufffdname")
    assert cell is not None
    assert cell.prior_loss is True
    assert bind_unicode_text(
        cell.text, engine="postgresql", dest_type="TEXT"
    ) == "bad\ufffdname"


def test_quarantine_holds_emoji_on_utf8mb3_does_not_substitute():
    rejected: list[dict] = []
    out = quarantine_unfit_strings(
        [(1, EMOJI), (2, "ascii")],
        ["id", "txt"],
        ["BIGINT", "VARCHAR(32) CHARACTER SET utf8mb3"],
        rejected,
        "quarantine",
        dialect_label="MySQL VARCHAR",
        dest_db="mysql",
    )
    assert out == [(2, "ascii")]
    assert rejected
    assert any("U+1F600" in str(d.get("reason")) for d in rejected)
    assert all("?" not in str(d.get("value") or "") for d in rejected)


def test_quarantine_recomposes_surrogate_leak_onto_utf8mb4():
    rejected: list[dict] = []
    out = quarantine_unfit_strings(
        [(1, SURROGATE_LEAK)],
        ["id", "txt"],
        ["BIGINT", "TEXT"],
        rejected,
        "quarantine",
        dialect_label="PostgreSQL VARCHAR",
        dest_db="postgresql",
    )
    assert not rejected
    assert out == [(1, EMOJI)]


def test_create_new_mysql_utf8mb3_stamp_is_refused_not_silently_promoted():
    """A stamped utf8mb3 target is graded on what it can store, not on intent.

    This case previously reported ``carried`` on the strength of a utf8mb4
    charset the collation plan wanted. It could not deliver it: MySQL takes one
    ``CHARACTER SET`` clause per column, so appending ``CHARACTER SET utf8mb4``
    to a type that already says ``utf8mb3`` is a syntax error and the CREATE
    fails outright — and if it did run as stamped, a supplementary scalar from
    the source TEXT would die at the write with 1366. The honest answer is a
    refusal the operator can act on (restamp the column), which is also what
    keeps the emitted DDL executable.
    """
    catalog = SourceSchemaCatalog(
        dialect="postgresql",
        columns=["id", "txt"],
        column_types={"id": "BIGINT", "txt": "TEXT"},
        primary_key=["id"],
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="mysql",
        target_columns=["id", "txt"],
        target_types=["BIGINT", "VARCHAR(32) CHARACTER SET utf8mb3"],
        source_to_target={"id": "id", "txt": "txt"},
    )
    items = [i for i in plan.report.items if i.aspect == "encoding"]
    assert items
    assert not any(i.status == "carried" for i in items)
    txt = [i for i in items if i.name in {"txt", "txt -> txt"}]
    assert txt and txt[0].status == "unsupported"
    assert "utf8mb3" in (txt[0].reason or "")
    # No second charset clause may be planned for a column that already has one.
    fragments = " ".join(plan.column_suffixes.get("txt") or []).upper()
    assert "CHARACTER SET" not in fragments, fragments


def test_create_new_mysql_utf8mb4_encoding_is_carried():
    catalog = SourceSchemaCatalog(
        dialect="postgresql",
        columns=["id", "txt"],
        column_types={"id": "BIGINT", "txt": "TEXT"},
        primary_key=["id"],
        charsets={"txt": "UTF8"},
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="mysql",
        target_columns=["id", "txt"],
        target_types=["BIGINT", "VARCHAR(32)"],
        source_to_target={"id": "id", "txt": "txt"},
    )
    items = [i for i in plan.report.items if i.aspect == "encoding"]
    assert items
    assert any(i.status == "carried" for i in items)
