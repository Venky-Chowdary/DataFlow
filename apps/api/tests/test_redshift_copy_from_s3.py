"""Redshift COPY FROM S3 — SQL shape, COPY TEXT bytes, fail-closed config."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.postgresql_writer import _copy_text_value  # noqa: E402
from connectors.redshift_copy import (  # noqa: E402
    RedshiftCopyConfig,
    build_redshift_copy_sql,
    copy_redshift_rows_from_s3,
    probe_redshift_staging,
    render_redshift_copy_text,
    resolve_redshift_copy_config,
    resolve_redshift_stage_format,
    should_gzip_redshift_stage,
    should_use_redshift_s3_copy_for_insert,
    staging_object_key,
)
_ROLE = "arn:aws:iam::123456789012:role/DatawrapRedshiftCopy"


def test_resolve_requires_bucket_and_role():
    assert resolve_redshift_copy_config({}, env_get=lambda *_a, **_k: "") is None
    assert resolve_redshift_copy_config({"staging_bucket": "b"}, env_get=lambda *_a, **_k: "") is None
    cfg = resolve_redshift_copy_config(
        {"staging_bucket": "dw-stage", "iam_role": _ROLE, "region": "us-east-1"},
        env_get=lambda *_a, **_k: "",
    )
    assert cfg is not None
    assert cfg.bucket == "dw-stage"
    assert cfg.iam_role == _ROLE
    assert cfg.region == "us-east-1"


def test_resolve_refuses_quoted_role():
    with pytest.raises(ValueError, match="quotes"):
        resolve_redshift_copy_config(
            {"staging_bucket": "dw-stage", "iam_role": "arn:aws:iam::123456789012:role/o'brian"},
            env_get=lambda *_a, **_k: "",
        )


def test_copy_sql_is_tab_delimited_null_slash_n():
    sql = build_redshift_copy_sql(
        "analytics",
        "orders",
        ["id", "amount"],
        "s3://dw-stage/dataflow/redshift-stage/run/orders/part-00001.tsv",
        iam_role=_ROLE,
        region="us-west-2",
    )
    assert 'COPY "analytics"."orders" ("id", "amount") FROM' in sql
    assert "s3://dw-stage/dataflow/redshift-stage/run/orders/part-00001.tsv" in sql
    assert f"IAM_ROLE '{_ROLE}'" in sql
    assert "DELIMITER '\\t'" in sql
    assert "NULL AS '\\\\N'" in sql
    assert "REGION 'us-west-2'" in sql
    assert "FORMAT AS CSV" not in sql


def test_copy_sql_temp_table_is_unqualified():
    sql = build_redshift_copy_sql(
        "",
        "_df_merge_stage_1",
        ["id"],
        "s3://dw-stage/x/part-00000.tsv",
        iam_role=_ROLE,
    )
    assert sql.startswith('COPY "_df_merge_stage_1" ("id") FROM')
    assert '"".' not in sql


def test_copy_sql_refuses_unsafe_uri_and_role():
    with pytest.raises(ValueError, match="unsafe"):
        build_redshift_copy_sql("s", "t", ["id"], "https://evil.example/x", iam_role=_ROLE)
    with pytest.raises(ValueError, match="IAM_ROLE"):
        build_redshift_copy_sql(
            "s", "t", ["id"], "s3://dw-stage/x/part-00000.tsv", iam_role="not-an-arn"
        )


def test_copy_text_matches_postgres_copy_ssot():
    rows = [
        (1, None, "a\tb"),
        (2, {"path": "C:\\temp"}, b"hello"),
    ]
    body = render_redshift_copy_text(rows)
    expected = (
        "\t".join(_copy_text_value(v) for v in rows[0])
        + "\n"
        + "\t".join(_copy_text_value(v) for v in rows[1])
        + "\n"
    ).encode("utf-8")
    assert body == expected
    assert b"\\N" in body


def test_stage_and_copy_execute_order():
    puts: list[tuple[str, str, bytes]] = []
    deletes: list[str] = []
    executed: list[str] = []

    class FakeS3:
        def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
            puts.append((Bucket, Key, Body))

        def delete_object(self, Bucket, Key):  # noqa: N803
            deletes.append(Key)

    class FakeCur:
        def execute(self, sql, params=None):
            executed.append(str(sql))

    cfg = RedshiftCopyConfig(bucket="dw-stage", iam_role=_ROLE, region="us-east-1")
    uri = copy_redshift_rows_from_s3(
        FakeCur(),
        schema="public",
        table="orders",
        columns=["id", "note"],
        rows=[(1, "ok"), (2, None)],
        config=cfg,
        s3_client=FakeS3(),
        job_id="job-abc",
        chunk_idx=1,
    )
    assert uri.startswith("s3://dw-stage/")
    assert puts and puts[0][0] == "dw-stage"
    assert puts[0][1].endswith("part-00001.tsv")
    assert b"\\N" in puts[0][2]
    assert executed and executed[0].startswith('COPY "public"."orders"')
    assert "s3://dw-stage/" in executed[0]
    assert deletes  # staging object removed after COPY


def test_empty_batch_does_not_copy():
    class Boom:
        def put_object(self, **_k):
            raise AssertionError("empty batch must not stage")

        def execute(self, *_a, **_k):
            raise AssertionError("empty batch must not COPY")

    assert (
        copy_redshift_rows_from_s3(
            Boom(),
            schema="public",
            table="t",
            columns=["id"],
            rows=[],
            config=RedshiftCopyConfig(bucket="b", iam_role=_ROLE),
            s3_client=Boom(),
        )
        == ""
    )


def test_merge_stage_uses_s3_copy_when_configured():
    from connectors.postgresql_writer import _redshift_merge_upsert

    executed: list[str] = []
    puts: list[str] = []

    class FakeS3:
        def put_object(self, Bucket, Key, Body, ContentType):  # noqa: N803
            puts.append(Key)

        def delete_object(self, Bucket, Key):  # noqa: N803
            return {}

    class _Frag:
        def __init__(self, text: str = ""):
            self.text = text

        def format(self, *args: object, **kwargs: object) -> "_Frag":
            pieces = [self.text, *[str(a) for a in args], *[str(v) for v in kwargs.values()]]
            return _Frag(" ".join(pieces))

        def join(self, parts: object) -> "_Frag":
            return _Frag(self.text.join(str(p) for p in parts))

        def __str__(self) -> str:
            return self.text

    class _SQL:
        @staticmethod
        def SQL(text: str) -> _Frag:
            return _Frag(text)

        @staticmethod
        def Identifier(name: str) -> str:
            return name

        @staticmethod
        def Placeholder() -> str:
            return "%s"

    class FakeCur:
        def execute(self, query, params=None):
            executed.append(str(query))

        def executemany(self, query, params=None):
            raise AssertionError("MERGE stage must not executemany when COPY is configured")

        def fetchone(self):
            return None

    cfg = RedshiftCopyConfig(bucket="dw-stage", iam_role=_ROLE)
    left = _redshift_merge_upsert(
        FakeCur(),
        _SQL,
        schema="public",
        table_name="orders",
        target_cols=["id", "v"],
        conflict_cols=["id"],
        batch=[(1, "a"), (2, "b")],
        copy_config=cfg,
        s3_client=FakeS3(),
        job_id="j1",
    )
    assert left == []
    assert puts
    assert any("COPY" in q and "FROM" in q for q in executed)
    assert any("MERGE INTO" in q for q in executed)


def test_capability_mentions_s3_copy_prerequisite():
    from services.connector_capability_registry import CAPABILITY_REGISTRY

    issues = " ".join(CAPABILITY_REGISTRY["redshift"].get("common_issues") or [])
    assert "COPY FROM S3" in issues
    assert "staging_bucket" in issues
    assert "not exactly-once" in issues.lower() or "at-least-once" in issues.lower()


def test_staging_key_is_stable_per_chunk():
    a = staging_object_key(table="Orders!", job_id="job-1", chunk_idx=2)
    b = staging_object_key(table="Orders!", job_id="job-1", chunk_idx=2)
    assert a == b
    assert a.endswith("part-00002.tsv")
    assert "Orders" in a or "orders" in a.lower() or "Orders_" in a


def test_insert_copy_respects_row_threshold():
    cfg = RedshiftCopyConfig(bucket="dw-stage", iam_role=_ROLE)
    assert should_use_redshift_s3_copy_for_insert(
        copy_config=cfg, write_mode="insert", conflict_columns=[], row_count=2, threshold=1000
    ) is False
    assert should_use_redshift_s3_copy_for_insert(
        copy_config=cfg, write_mode="insert", conflict_columns=[], row_count=1000, threshold=1000
    ) is True
    assert should_use_redshift_s3_copy_for_insert(
        copy_config=cfg, write_mode="upsert", conflict_columns=["id"], row_count=5000, threshold=1000
    ) is False
    assert should_use_redshift_s3_copy_for_insert(
        copy_config=None, write_mode="insert", conflict_columns=[], row_count=5000
    ) is False


def test_gzip_only_for_large_tsv():
    small = b"1\tok\n"
    huge = b"x" * 1_048_576
    assert should_gzip_redshift_stage(small, stage_format="tsv") is False
    assert should_gzip_redshift_stage(huge, stage_format="tsv") is True
    assert should_gzip_redshift_stage(huge, stage_format="parquet") is False
    assert should_gzip_redshift_stage(small, stage_format="tsv", threshold=1) is True


def test_copy_sql_gzip_and_parquet_shapes():
    gzip_sql = build_redshift_copy_sql(
        "analytics",
        "orders",
        ["id"],
        "s3://dw-stage/x/part-00000.tsv",
        iam_role=_ROLE,
        gzip=True,
    )
    assert " GZIP" in gzip_sql
    assert "DELIMITER" in gzip_sql
    pq_sql = build_redshift_copy_sql(
        "analytics",
        "orders",
        ["id", "amount"],
        "s3://dw-stage/x/part-00000.parquet",
        iam_role=_ROLE,
        region="us-east-1",
        stage_format="parquet",
    )
    assert "FORMAT AS PARQUET" in pq_sql
    assert "DELIMITER" not in pq_sql
    assert "NULL AS" not in pq_sql
    assert "GZIP" not in pq_sql
    assert "REGION 'us-east-1'" in pq_sql


def test_stage_and_copy_gzips_when_threshold_is_one():
    puts: list[dict] = []
    executed: list[str] = []

    class FakeS3:
        def put_object(self, **kwargs):
            puts.append(kwargs)

        def delete_object(self, Bucket, Key):  # noqa: N803
            return {}

    class FakeCur:
        def execute(self, sql, params=None):
            executed.append(str(sql))

    cfg = RedshiftCopyConfig(bucket="dw-stage", iam_role=_ROLE)
    copy_redshift_rows_from_s3(
        FakeCur(),
        schema="public",
        table="orders",
        columns=["id", "note"],
        rows=[(1, "ok")],
        config=cfg,
        s3_client=FakeS3(),
        gzip_threshold=1,
    )
    assert puts[0]["ContentEncoding"] == "gzip"
    assert puts[0]["ContentType"] == "application/gzip"
    assert puts[0]["Body"][:2] == b"\x1f\x8b"
    assert " GZIP" in executed[0]


def test_parquet_stage_uses_format_as_parquet():
    pytest.importorskip("pyarrow")
    puts: list[dict] = []
    executed: list[str] = []

    class FakeS3:
        def put_object(self, **kwargs):
            puts.append(kwargs)

        def delete_object(self, Bucket, Key):  # noqa: N803
            return {}

    class FakeCur:
        def execute(self, sql, params=None):
            executed.append(str(sql))

    cfg = RedshiftCopyConfig(bucket="dw-stage", iam_role=_ROLE)
    uri = copy_redshift_rows_from_s3(
        FakeCur(),
        schema="public",
        table="orders",
        columns=["id", "amount"],
        rows=[(1, 10.5), (2, 20.0)],
        config=cfg,
        s3_client=FakeS3(),
        dest_types={"id": "INTEGER", "amount": "DOUBLE"},
        stage_format="parquet",
    )
    assert uri.endswith(".parquet")
    assert puts[0]["Key"].endswith(".parquet")
    assert "parquet" in str(puts[0]["ContentType"]).lower()
    assert "ContentEncoding" not in puts[0]
    assert "FORMAT AS PARQUET" in executed[0]
    assert "DELIMITER" not in executed[0]


def test_resolve_stage_format():
    assert resolve_redshift_stage_format({}) == "tsv"
    assert resolve_redshift_stage_format({"redshift_stage_format": "parquet"}) == "parquet"
    assert resolve_redshift_stage_format({"stage_format": "PQ"}) == "parquet"


def test_probe_not_configured_is_not_a_block():
    out = probe_redshift_staging({}, env_get=lambda *_a, **_k: "")
    assert out["status"] == "not_configured"
    assert "PostgreSQL-wire" in out["detail"]


def test_probe_denied_on_access_denied_head():
    class Denied:
        def head_bucket(self, Bucket):  # noqa: N803
            err = Exception("denied")
            err.response = {"Error": {"Code": "AccessDenied"}}
            raise err

        def put_object(self, **_k):
            raise AssertionError("probe must never PutObject")

    out = probe_redshift_staging(
        {"staging_bucket": "dw-stage", "iam_role": _ROLE},
        s3_client=Denied(),
        env_get=lambda *_a, **_k: "",
    )
    assert out["status"] == "denied"
    assert out["bucket"] == "dw-stage"


def test_probe_ok_when_acl_grants_write():
    class Ok:
        def head_bucket(self, Bucket):  # noqa: N803
            return {}

        def get_bucket_acl(self, Bucket):  # noqa: N803
            return {"Grants": [{"Permission": "FULL_CONTROL"}]}

        def put_object(self, **_k):
            raise AssertionError("probe must never PutObject")

    out = probe_redshift_staging(
        {"staging_bucket": "dw-stage", "iam_role": _ROLE},
        s3_client=Ok(),
        env_get=lambda *_a, **_k: "",
    )
    assert out["status"] == "ok"
    assert out["method"] == "GetBucketAcl"


def test_probe_denied_when_acl_is_read_only():
    class ReadOnly:
        def head_bucket(self, Bucket):  # noqa: N803
            return {}

        def get_bucket_acl(self, Bucket):  # noqa: N803
            return {"Grants": [{"Permission": "READ"}]}

        def put_object(self, **_k):
            raise AssertionError("probe must never PutObject")

    out = probe_redshift_staging(
        {"staging_bucket": "dw-stage", "iam_role": _ROLE},
        s3_client=ReadOnly(),
        env_get=lambda *_a, **_k: "",
    )
    assert out["status"] == "denied"


def test_probe_unavailable_when_acl_disabled():
    class NoAcl:
        def head_bucket(self, Bucket):  # noqa: N803
            return {}

        def get_bucket_acl(self, Bucket):  # noqa: N803
            err = Exception("acls off")
            err.response = {"Error": {"Code": "AccessDenied"}}
            raise err

        def put_object(self, **_k):
            raise AssertionError("probe must never PutObject")

    out = probe_redshift_staging(
        {"staging_bucket": "dw-stage", "iam_role": _ROLE},
        s3_client=NoAcl(),
        env_get=lambda *_a, **_k: "",
    )
    assert out["status"] == "unavailable"
    assert "GetBucketAcl" in out["detail"] or out["method"] == "GetBucketAcl"
