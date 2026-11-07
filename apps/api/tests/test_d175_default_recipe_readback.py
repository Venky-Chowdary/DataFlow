"""D175: a ``default:`` recipe on a column holding real NULLs verifies at Gate-8.

The writer's ``apply_transform`` turns a NULL into the ``default:`` literal, but
``fingerprint_for_reconcile`` skipped every transform for NULL, so the expected
source value stayed NULL and the read-back ("EMPTYNOTE") failed
"row 3 column note->note: source='null' dest='EMPTYNOTE'" on a correct write.
The verifier now runs the transform on NULL exactly as the writer does.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services.reconciliation import fingerprint_for_reconcile  # noqa: E402
from services.transform_engine import apply_transform  # noqa: E402
from services.value_serializer import SQL_NULL_SENTINEL  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

NULL_SPELLINGS = [None, SQL_NULL_SENTINEL, "__df_ddb_null__"]


@pytest.mark.parametrize("null", NULL_SPELLINGS)
def test_default_recipe_on_null_fingerprints_as_the_literal(null) -> None:
    expected = fingerprint_for_reconcile("EMPTYNOTE", ddl_type="TEXT", engine="postgresql")
    got = fingerprint_for_reconcile(
        null, ddl_type="TEXT", engine="postgresql", transform="default:EMPTYNOTE"
    )
    assert got == expected


@pytest.mark.parametrize(
    "transform",
    ["default:X", "concat:-sfx", "replace:a:b", "upper", "lower", "trim", "integer", "hash_pii"],
)
@pytest.mark.parametrize("null", NULL_SPELLINGS)
def test_null_fingerprint_follows_what_the_writer_does_with_null(transform, null) -> None:
    """Whatever ``apply_transform`` returns for NULL is what the verifier expects."""
    written, err = apply_transform(None, transform)
    assert err is None
    assert fingerprint_for_reconcile(
        null, ddl_type="TEXT", engine="postgresql", transform=transform
    ) == fingerprint_for_reconcile(written, ddl_type="TEXT", engine="postgresql")


def test_concat_and_replace_keep_null_as_null() -> None:
    null_fp = fingerprint_for_reconcile(None, ddl_type="TEXT", engine="postgresql")
    for transform in ("concat:-sfx", "replace:a:b"):
        assert (
            fingerprint_for_reconcile(
                None, ddl_type="TEXT", engine="postgresql", transform=transform
            )
            == null_fp
        )


def test_literal_null_text_stays_a_string_under_default() -> None:
    """Guard: the text 'null' is data, not NULL, so ``default:`` leaves it alone."""
    assert fingerprint_for_reconcile(
        "null", ddl_type="TEXT", engine="postgresql", transform="default:EMPTYNOTE"
    ) == fingerprint_for_reconcile("null", ddl_type="TEXT", engine="postgresql")
    assert fingerprint_for_reconcile(
        "null", ddl_type="TEXT", engine="postgresql", transform="default:EMPTYNOTE"
    ) != fingerprint_for_reconcile("EMPTYNOTE", ddl_type="TEXT", engine="postgresql")


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def test_overwrite_with_default_recipe_over_nulls_completes_and_verifies(
    tmp_path: Path, isolated
) -> None:
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    with sqlite3.connect(src) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)")
        conn.executemany(
            "INSERT INTO t VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, None), (4, "null"), (5, None), (6, "")],
        )
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER", "confidence": 1.0},
            {
                "source": "note", "target": "note", "target_type": "TEXT",
                "transform": "default:EMPTYNOTE", "transform_override": True, "confidence": 0.99,
            },
        ],
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
    )
    job_id = uuid.uuid4().hex[:24]
    isolated.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success, result.error
    with sqlite3.connect(dst) as conn:
        got = conn.execute("SELECT id, note FROM t ORDER BY id").fetchall()
    # NULL and '' take the default (writer rule); the text 'null' is data.
    assert got == [
        (1, "a"), (2, "b"), (3, "EMPTYNOTE"), (4, "null"), (5, "EMPTYNOTE"), (6, "EMPTYNOTE"),
    ]
