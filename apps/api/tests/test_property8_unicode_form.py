"""PROPERTY 8 (unicode form) — dest UNIQUE and HEX keep NFC ≠ NFD, or we say they do not.

PostgreSQL TEXT and MariaDB utf8mb4_bin / general_ci treat NFC café and NFD
café as distinct keys. MariaDB utf8mb4_unicode_ci UCA-folds them (second
UNIQUE insert SECOND_REJECT) and equates ß with ss. Certify from dest HEX
(C3A9 vs CC81) and UNIQUE outcome, not from Python unicodedata after a rewrite.
Bind does not NFC.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.unicode_form import (
    NFC_CAFE,
    NFC_CAFE_UTF8_HEX,
    NFD_CAFE,
    NFD_CAFE_UTF8_HEX,
    SHARP_S,
    SS_EXPANSION,
    classify_uca,
    dest_is_nfc_sql,
    dest_utf8_hex_sql,
    unique_second_outcome,
    utf8_form_hex,
)
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _run(req: TransferRequest):
    return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])


def _pg_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=0.4):
            return True
    except OSError:
        return False


def _mysql_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 3306), timeout=0.4):
            return True
    except OSError:
        return False


def _pg_creds() -> dict:
    return {
        "host": os.environ.get("P8_PG_HOST", os.environ.get("P2_PG_HOST", "127.0.0.1")),
        "port": int(os.environ.get("P8_PG_PORT", os.environ.get("P2_PG_PORT", "5432"))),
        "database": os.environ.get("P8_PG_DB", os.environ.get("P2_PG_DB", "dataflow")),
        "username": os.environ.get("P8_PG_USER", os.environ.get("P2_PG_USER", "dataflow")),
        "password": os.environ.get(
            "P8_PG_PASSWORD", os.environ.get("P2_PG_PASSWORD", "dataflow")
        ),
    }


def _mysql_creds() -> dict:
    return {
        "host": os.environ.get("P8_MYSQL_HOST", os.environ.get("P2_MYSQL_HOST", "127.0.0.1")),
        "port": int(os.environ.get("P8_MYSQL_PORT", os.environ.get("P2_MYSQL_PORT", "3306"))),
        "database": os.environ.get("P8_MYSQL_DB", os.environ.get("P2_MYSQL_DB", "dataflow")),
        "username": os.environ.get("P8_MYSQL_USER", os.environ.get("P2_MYSQL_USER", "dataflow")),
        "password": os.environ.get(
            "P8_MYSQL_PASSWORD", os.environ.get("P2_MYSQL_PASSWORD", "dataflow")
        ),
    }


def _maps():
    return [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
            "approved": True,
            "confidence": 0.99,
        },
        {
            "source": "code",
            "target": "code",
            "source_type": "TEXT",
            "target_type": "VARCHAR(32)",
            "approved": True,
            "confidence": 0.99,
        },
    ]


def _unicode_form_status(report: dict) -> set[str]:
    return {
        i["status"]
        for i in (report.get("items") or [])
        if i.get("aspect") == "unicode_form"
    }


def _mysql_unique_probe(cur, table: str, collation: str, first: str, second: str) -> str:
    cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    cur.execute(
        f"""
        CREATE TABLE `{table}` (
          code VARCHAR(32) CHARACTER SET utf8mb4 COLLATE {collation} PRIMARY KEY
        )
        """
    )
    first_ok = True
    try:
        cur.execute(f"INSERT INTO `{table}` (code) VALUES (%s)", (first,))
    except Exception:
        first_ok = False
    second_ok = True
    try:
        cur.execute(f"INSERT INTO `{table}` (code) VALUES (%s)", (second,))
    except Exception:
        second_ok = False
    return unique_second_outcome(first_ok=first_ok, second_ok=second_ok)


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not listening")
def test_pg_text_unique_keeps_nfc_and_nfd_as_distinct_keys():
    """Dest-engine proof: PG TEXT PK lands both forms; HEX differs; stored is NFC only for the NFC row."""
    import psycopg2

    pg = _pg_creds()
    table = f"p8_form_pg_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        dbname=pg["database"],
        user=pg["username"],
        password=pg["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            cur.execute(
                f'CREATE TABLE public."{table}" (code TEXT PRIMARY KEY)'
            )
            cur.execute(
                f'INSERT INTO public."{table}" (code) VALUES (%s), (%s)',
                (NFC_CAFE, NFD_CAFE),
            )
            hex_sql = dest_utf8_hex_sql("postgresql", "code")
            nfc_sql = dest_is_nfc_sql("postgresql", "code")
            assert nfc_sql is not None
            cur.execute(
                f'SELECT {hex_sql}, {nfc_sql} FROM public."{table}" ORDER BY octet_length(code)'
            )
            rows = cur.fetchall()
            hexes = {str(r[0] or "").upper() for r in rows}
            assert NFC_CAFE_UTF8_HEX in hexes
            assert NFD_CAFE_UTF8_HEX in hexes
            # Shorter UTF-8 (NFC) is composed; longer (NFD) is not.
            by_hex = {str(r[0] or "").upper(): bool(r[1]) for r in rows}
            assert by_hex[NFC_CAFE_UTF8_HEX] is True
            assert by_hex[NFD_CAFE_UTF8_HEX] is False
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            assert int(cur.fetchone()[0]) == 2
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        finally:
            conn.close()


@pytest.mark.skipif(not _mysql_up(), reason="MariaDB not listening")
def test_mariadb_collation_unique_nfc_nfd_and_sharp_s():
    """Dest-engine matrix: bin/general_ci BOTH_LAND; unicode_ci / 520 SECOND_REJECT."""
    import pymysql

    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    conn = pymysql.connect(
        host=my["host"],
        port=my["port"],
        database=my["database"],
        user=my["username"],
        password=my["password"],
        autocommit=True,
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLLATION_NAME FROM information_schema.COLLATIONS "
                "WHERE COLLATION_NAME IN ("
                "'utf8mb4_bin','utf8mb4_general_ci',"
                "'utf8mb4_unicode_ci','utf8mb4_unicode_520_ci',"
                "'utf8mb4_0900_ai_ci','utf8mb4_uca1400_ai_ci')"
            )
            present = {str(r[0]) for r in cur.fetchall()}
            assert "utf8mb4_bin" in present
            assert "utf8mb4_general_ci" in present
            assert "utf8mb4_unicode_ci" in present

            bin_nfc = _mysql_unique_probe(
                cur, f"p8_form_bin_{suffix}", "utf8mb4_bin", NFC_CAFE, NFD_CAFE
            )
            gen_nfc = _mysql_unique_probe(
                cur, f"p8_form_gen_{suffix}", "utf8mb4_general_ci", NFC_CAFE, NFD_CAFE
            )
            uni_nfc = _mysql_unique_probe(
                cur, f"p8_form_uni_{suffix}", "utf8mb4_unicode_ci", NFC_CAFE, NFD_CAFE
            )
            gen_ss = _mysql_unique_probe(
                cur, f"p8_form_gen_ss_{suffix}", "utf8mb4_general_ci", SHARP_S, SS_EXPANSION
            )
            uni_ss = _mysql_unique_probe(
                cur, f"p8_form_uni_ss_{suffix}", "utf8mb4_unicode_ci", SHARP_S, SS_EXPANSION
            )
            assert bin_nfc == "BOTH_LAND", bin_nfc
            assert gen_nfc == "BOTH_LAND", gen_nfc
            assert uni_nfc == "SECOND_REJECT", uni_nfc
            assert gen_ss == "BOTH_LAND", gen_ss
            assert uni_ss == "SECOND_REJECT", uni_ss

            if "utf8mb4_unicode_520_ci" in present:
                u520 = _mysql_unique_probe(
                    cur,
                    f"p8_form_520_{suffix}",
                    "utf8mb4_unicode_520_ci",
                    NFC_CAFE,
                    NFD_CAFE,
                )
                assert u520 == "SECOND_REJECT", u520
            # 0900 / 1400 exist on some hosts and not others (MySQL 8 has
            # 0900; MariaDB 10.11 has neither; MariaDB 11.4 has 1400). The
            # claim under test is the model's, not the host's inventory: where
            # the host offers the collation, its measured NFC/NFD uniqueness
            # must equal what classify_uca says about that weight table.
            for label, collation in (
                ("0900", "utf8mb4_0900_ai_ci"),
                ("1400", "utf8mb4_uca1400_ai_ci"),
            ):
                if collation not in present:
                    continue
                measured = _mysql_unique_probe(
                    cur,
                    f"p8_form_{label}_{suffix}",
                    collation,
                    NFC_CAFE,
                    NFD_CAFE,
                )
                profile = classify_uca("mysql", collation)
                expected = (
                    "SECOND_REJECT"
                    if profile.canonical_equivalence
                    else "BOTH_LAND"
                )
                assert measured == expected, (collation, measured, profile.to_dict())

            cur.execute(f"DROP TABLE IF EXISTS `p8_form_hex_{suffix}`")
            cur.execute(
                f"""
                CREATE TABLE `p8_form_hex_{suffix}` (
                  id BIGINT PRIMARY KEY,
                  code VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin
                )
                """
            )
            cur.execute(
                f"INSERT INTO `p8_form_hex_{suffix}` (id, code) VALUES (%s, %s), (%s, %s)",
                (1, NFC_CAFE, 2, NFD_CAFE),
            )
            hex_sql = dest_utf8_hex_sql("mariadb", "code")
            cur.execute(
                f"SELECT id, {hex_sql} FROM `p8_form_hex_{suffix}` ORDER BY id"
            )
            got = [(int(r[0]), str(r[1] or "").upper()) for r in cur.fetchall()]
            assert got == [(1, NFC_CAFE_UTF8_HEX), (2, NFD_CAFE_UTF8_HEX)]
            assert utf8_form_hex(NFC_CAFE) == NFC_CAFE_UTF8_HEX
    finally:
        try:
            with conn.cursor() as cur:
                for name in (
                    f"p8_form_bin_{suffix}",
                    f"p8_form_gen_{suffix}",
                    f"p8_form_uni_{suffix}",
                    f"p8_form_gen_ss_{suffix}",
                    f"p8_form_uni_ss_{suffix}",
                    f"p8_form_520_{suffix}",
                    f"p8_form_hex_{suffix}",
                ):
                    cur.execute(f"DROP TABLE IF EXISTS `{name}`")
        finally:
            conn.close()


@pytest.mark.skipif(not (_pg_up() and _mysql_up()), reason="PostgreSQL or MariaDB not listening")
def test_pg_nfc_nfd_pair_lands_on_mariadb_bin_hex_not_folded():
    """Source TEXT UNIQUE accepted both forms; dest utf8mb4_bin stores both HEX spellings."""
    import psycopg2
    import pymysql

    pg = _pg_creds()
    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p8_form_src_{suffix}"
    dst_table = f"p8_form_dst_{suffix}"
    conn = psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        dbname=pg["database"],
        user=pg["username"],
        password=pg["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(
                f'''
                CREATE TABLE public."{src_table}" (
                  id BIGINT PRIMARY KEY,
                  code TEXT NOT NULL,
                  UNIQUE (code)
                )
                '''
            )
            cur.execute(
                f'INSERT INTO public."{src_table}" (id, code) VALUES (%s, %s), (%s, %s)',
                (1, NFC_CAFE, 2, NFD_CAFE),
            )
    finally:
        conn.close()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="postgresql",
            host=pg["host"],
            port=pg["port"],
            database=pg["database"],
            username=pg["username"],
            password=pg["password"],
            schema="public",
            table=src_table,
            ssl=False,
        ),
        destination=EndpointConfig(
            kind="database",
            format="mysql",
            host=my["host"],
            port=my["port"],
            database=my["database"],
            username=my["username"],
            password=my["password"],
            schema="",
            table=dst_table,
            ssl=False,
        ),
        mappings=_maps(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    try:
        result = _run(req)
        assert result.success, result.error
        fid = (result.destination_summary or {}).get("schema_fidelity") or {}
        assert "carried" in _unicode_form_status(fid), fid

        conn = pymysql.connect(
            host=my["host"],
            port=my["port"],
            database=my["database"],
            user=my["username"],
            password=my["password"],
            autocommit=True,
            charset="utf8mb4",
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT collation_name FROM information_schema.columns
                     WHERE table_schema = DATABASE()
                       AND table_name = %s AND column_name = 'code'
                    """,
                    (dst_table,),
                )
                coll = str((cur.fetchone() or [""])[0] or "").lower()
                assert "_bin" in coll, coll
                hex_sql = dest_utf8_hex_sql("mariadb", "code")
                cur.execute(
                    f"SELECT id, code, {hex_sql} FROM `{dst_table}` ORDER BY id"
                )
                rows = cur.fetchall()
                assert len(rows) == 2, rows
                hexes = [str(r[2] or "").upper() for r in rows]
                assert hexes == [NFC_CAFE_UTF8_HEX, NFD_CAFE_UTF8_HEX], hexes
                assert rows[0][1] == NFC_CAFE
                assert rows[1][1] == NFD_CAFE
        finally:
            conn.close()
    finally:
        conn = psycopg2.connect(
            host=pg["host"],
            port=pg["port"],
            dbname=pg["database"],
            user=pg["username"],
            password=pg["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
        finally:
            conn.close()
        conn = pymysql.connect(
            host=my["host"],
            port=my["port"],
            database=my["database"],
            user=my["username"],
            password=my["password"],
            autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{dst_table}`")
        finally:
            conn.close()
