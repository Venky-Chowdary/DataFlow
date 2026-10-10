"""Oracle redo-inventory continuity checks."""

from __future__ import annotations

import logging
import os
import subprocess
import uuid
from unittest.mock import MagicMock

import pytest

from connectors.oracle_logminer import CdcScnGapError


def test_fetch_redo_inventory_deduplicates_archive_destinations() -> None:
    from connectors.oracle_logminer import fetch_redo_inventory

    cur = MagicMock()
    archived = [
        (1, 10, 100, 200),
        (1, 10, 100, 200),
        (1, 11, 200, 300),
    ]
    online = [(1, 12, 300, 400)]
    cur.fetchall.side_effect = [archived, online]

    inventory = fetch_redo_inventory(cur)

    assert inventory == [
        (1, 10, 100, 200, "archived"),
        (1, 11, 200, 300, "archived"),
        (1, 12, 300, 400, "online"),
    ]
    archive_sql = str(cur.execute.call_args_list[0].args[0])
    assert "DELETED = 'NO'" in archive_sql
    assert "STANDBY_DEST = 'NO'" in archive_sql


def test_fetch_redo_inventory_deduplicates_online_archived_overlap() -> None:
    from connectors.oracle_logminer import fetch_redo_inventory

    cur = MagicMock()
    cur.fetchall.side_effect = [
        [(1, 12, 300, 400)],
        [(1, 12, 300, 400), (1, 13, 400, 500)],
    ]

    assert fetch_redo_inventory(cur) == [
        (1, 12, 300, 400, "online"),
        (1, 13, 400, 500, "online"),
    ]


def test_assert_redo_continuity_detects_single_thread_hole() -> None:
    from connectors.oracle_logminer import assert_redo_continuity

    inventory = [
        (1, 10, 100, 200, "archived"),
        (1, 12, 300, 400, "online"),
    ]

    with pytest.raises(CdcScnGapError) as exc:
        assert_redo_continuity(150, inventory, cursor_key="oracle:test")

    assert "thread 1" in str(exc.value).lower()
    assert "11-11" in str(exc.value)
    assert "RMAN RESTORE ARCHIVELOG" in str(exc.value)
    assert "re-snapshot" in str(exc.value).lower()


def test_assert_redo_continuity_detects_hole_when_resume_is_between_logs() -> None:
    from connectors.oracle_logminer import assert_redo_continuity

    with pytest.raises(CdcScnGapError):
        assert_redo_continuity(
            175,
            [
                (1, 10, 100, 150, "archived"),
                (1, 12, 200, 300, "online"),
            ],
            cursor_key="oracle:resume-in-hole",
        )


def test_assert_redo_continuity_detects_purged_rac_thread() -> None:
    from connectors.oracle_logminer import assert_redo_continuity

    inventory = [
        (1, 4, 100, 200, "archived"),
        (1, 5, 200, 300, "online"),
        (2, 8, 150, 250, "archived"),
        (2, 9, 250, 350, "online"),
    ]

    with pytest.raises(CdcScnGapError) as exc:
        assert_redo_continuity(120, inventory, cursor_key="oracle:rac")

    assert "thread 2" in str(exc.value).lower()
    assert "1-7" in str(exc.value)


def test_assert_redo_continuity_accepts_healthy_chain() -> None:
    from connectors.oracle_logminer import assert_redo_continuity

    assert_redo_continuity(
        250,
        [
            (1, 10, 100, 200, "archived"),
            (1, 11, 200, 300, "archived"),
            (1, 12, 300, 400, "online"),
        ],
        cursor_key="oracle:healthy",
    )


def test_assert_redo_continuity_empty_inventory_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from connectors.oracle_logminer import assert_redo_continuity

    with caplog.at_level(logging.WARNING):
        assert_redo_continuity(100, [], cursor_key="oracle:unknown")

    assert "redo inventory" in caplog.text.lower()


def test_assert_redo_continuity_undetermined_inventory_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from connectors.oracle_logminer import assert_redo_continuity

    with caplog.at_level(logging.WARNING):
        assert_redo_continuity(100, None, cursor_key="oracle:unknown")

    assert "unverified" in caplog.text.lower()


def _oracle_admin_sql(sql: str) -> str:
    proc = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            "-u",
            "oracle",
            os.getenv("DATAFLOW_ORACLE_CONTAINER", "df-oracle"),
            "bash",
            "-lc",
            "sqlplus -s / as sysdba",
        ],
        input=f"WHENEVER SQLERROR EXIT SQL.SQLCODE\n{sql}\nEXIT\n",
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr or proc.stdout
    return proc.stdout


@pytest.mark.skipif(
    os.getenv("DATAFLOW_RUN_ORACLE_REDO_GAP_LIVE") != "1",
    reason="set DATAFLOW_RUN_ORACLE_REDO_GAP_LIVE=1 to delete a generated archive log",
)
def test_live_logminer_poll_fails_closed_on_purged_archive_sequence() -> None:
    from connectors.generic_sql import get_connection
    from connectors.oracle_logminer import (
        OracleLogMinerCdc,
        encode_logminer_token,
    )
    from services.brand_env import getenv_brand
    from test_cdc_oracle_logminer_transfer_e2e import (
        _oracle_cfg,
        _oracle_logminer_ready,
    )

    if not _oracle_logminer_ready():
        pytest.skip("df-oracle LogMiner is not ready")
    container = os.getenv("DATAFLOW_ORACLE_CONTAINER", "df-oracle")

    cfg = _oracle_cfg()
    table = "M2_REDO_" + uuid.uuid4().hex[:10].upper()
    thread = 1
    first_sequence = 0
    resume_scn = 0
    table_created = False
    with get_connection(
        host=cfg["host"],
        port=cfg["port"],
        database=cfg["database"],
        username=cfg["username"],
        password=cfg["password"],
        connection_string="",
        ssl=False,
        db_type="oracle",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT LOG_MODE FROM V$DATABASE")
            if str(cur.fetchone()[0]).upper() != "ARCHIVELOG":
                pytest.skip("df-oracle is not in ARCHIVELOG mode")
            cur.execute(
                f'CREATE TABLE "{cfg["schema"]}"."{table}" '
                "(ID NUMBER PRIMARY KEY, VALUE_TEXT VARCHAR2(80))"
            )
            table_created = True
            cur.execute(
                f'ALTER TABLE "{cfg["schema"]}"."{table}" '
                "ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS"
            )
            cur.execute(
                f'INSERT INTO "{cfg["schema"]}"."{table}" VALUES (1, \'seed\')'
            )
            cur.execute("SELECT current_scn FROM v$database")
            resume_scn = int(cur.fetchone()[0])
        conn.commit()

    logminer_cfg = {
        "host": cfg["host"],
        "port": cfg["port"],
        "database": getenv_brand("ORACLE_CDB_SERVICE", "FREE") or "FREE",
        "username": getenv_brand("ORACLE_LOGMINER_USER", "C##DATAFLOW")
        or "C##DATAFLOW",
        "password": getenv_brand(
            "ORACLE_LOGMINER_PASSWORD", cfg["password"]
        )
        or cfg["password"],
    }
    with get_connection(
        **logminer_cfg,
        connection_string="",
        ssl=False,
        db_type="oracle",
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT thread#, sequence# FROM v$log WHERE status = 'CURRENT'"
            )
            thread, first_sequence = map(int, cur.fetchone())

    reader = OracleLogMinerCdc(
        cfg,
        table=table,
        primary_key="ID",
        schema=cfg["schema"],
        resume_token=encode_logminer_token(
            resume_scn,
            table=table,
            phase="streaming",
        ),
    )
    reader.phase = "streaming"
    reader.scn = resume_scn

    try:
        list(reader.poll())
        rman_available = subprocess.run(
            [
                "docker",
                "exec",
                "-u",
                "oracle",
                container,
                "bash",
                "-lc",
                'command -v rman || test -x "$ORACLE_HOME/bin/rman"',
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if rman_available.returncode != 0:
            pytest.skip("df-oracle image does not include the RMAN executable")
        for _ in range(4):
            _oracle_admin_sql("ALTER SYSTEM SWITCH LOGFILE;\nALTER SYSTEM ARCHIVE LOG CURRENT;")

        with get_connection(
            **logminer_cfg,
            connection_string="",
            ssl=False,
            db_type="oracle",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT DISTINCT SEQUENCE#
                    FROM V$ARCHIVED_LOG
                    WHERE THREAD# = :thread
                      AND SEQUENCE# > :first_sequence
                      AND DELETED = 'NO'
                      AND STANDBY_DEST = 'NO'
                    ORDER BY SEQUENCE#
                    """,
                    {"thread": thread, "first_sequence": first_sequence},
                )
                sequences = [int(row[0]) for row in cur.fetchall()]
        assert len(sequences) >= 3, sequences
        missing_sequence = sequences[len(sequences) // 2]
        rman = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                "-u",
                "oracle",
                os.getenv("DATAFLOW_ORACLE_CONTAINER", "df-oracle"),
                "bash",
                "-lc",
                "rman target /",
            ],
            input=f"DELETE NOPROMPT ARCHIVELOG SEQUENCE {missing_sequence} THREAD {thread};\nEXIT\n",
            capture_output=True,
            text=True,
            check=False,
        )
        assert rman.returncode == 0, rman.stderr or rman.stdout

        reader.scn = resume_scn
        reader.rs_id = ""
        reader.ssn = 0
        with pytest.raises(CdcScnGapError) as exc:
            batches = list(reader.poll())
            pytest.fail(f"purged redo returned batches instead of failing: {batches!r}")
        assert f"thread {thread}" in str(exc.value).lower()
        assert str(missing_sequence) in str(exc.value)
    finally:
        reader.close()
        if table_created:
            with get_connection(
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                username=cfg["username"],
                password=cfg["password"],
                connection_string="",
                ssl=False,
                db_type="oracle",
            ) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f'DROP TABLE "{cfg["schema"]}"."{table}" PURGE'
                    )
                conn.commit()
