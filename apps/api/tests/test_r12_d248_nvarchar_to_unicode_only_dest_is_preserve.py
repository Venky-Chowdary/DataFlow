"""D248 — SQL Server NVARCHAR text into Elasticsearch blocked strict preflight as a
"fidelity collapse" (pf_c03f1076c56f: name/long_txt/payload/note), while the same
seed from MySQL/PG blocked only the real millisecond collapse on updated_at.

Cause: the Unicode-only destination set listed only SQL engines + MongoDB, so
every document, search, key-value, stream, graph, vector and lakehouse store was
treated as a code-page sink. None of them has a single-byte text type.
"""
from __future__ import annotations

import pytest

from services.type_system import destination_is_unicode_only, national_charset_would_collapse

_NVARCHAR = "NVARCHAR(200) COLLATE SQL_Latin1_General_CP1_CI_AS"
_UTF8_ONLY = (
    "elasticsearch", "opensearch", "dynamodb", "couchbase", "redis", "kafka",
    "neo4j", "weaviate", "qdrant", "pinecone", "milvus", "influxdb", "iceberg",
)


@pytest.mark.parametrize("dest", _UTF8_ONLY)
@pytest.mark.parametrize("target", ["TEXT", "STRING", "VARCHAR(200)"])
def test_national_text_into_unicode_only_store_is_not_a_collapse(dest, target):
    assert destination_is_unicode_only(dest)
    assert not national_charset_would_collapse(_NVARCHAR, target, dest_db=dest)


@pytest.mark.parametrize("dest", ["mysql", "mariadb", "oracle", "sqlserver", "db2", "cassandra"])
def test_code_page_capable_destinations_still_report_the_collapse(dest):
    assert national_charset_would_collapse(_NVARCHAR, "VARCHAR(200)", dest_db=dest)
