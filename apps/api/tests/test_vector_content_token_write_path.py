"""Vector content / source_id use present_cell_text, not ``or ""`` / ``str()``.

``or ""`` dropped ``0`` / ``False`` into empty hash material so distinct
chunks collided. ``str(True)`` invented ``True`` so dest ``true`` missed
retry identity. Reader-null sentinels hashed as the wire spelling.
"""

from __future__ import annotations

import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.milvus_writer import build_milvus_entities  # noqa: E402
from connectors.pinecone_writer import build_pinecone_vectors  # noqa: E402
from connectors.qdrant_writer import build_qdrant_points  # noqa: E402
from connectors.weaviate_writer import build_weaviate_objects  # noqa: E402
from services.value_serializer import (  # noqa: E402
    DF_MISSING_SENTINEL,
    SQL_NULL_SENTINEL,
    Missing,
)
from services.vector_embedding import (  # noqa: E402
    vector_cell_token,
    vector_fallback_material,
)

_EMBED = [0.1, 0.2, 0.3]


def _row(**overrides):
    base = {
        "id": "vec-1",
        "content": "hello",
        "source_id": "src-1",
        "chunk_index": 0,
        "embedding": list(_EMBED),
        "metadata": {},
    }
    base.update(overrides)
    return base


def _build_all(rows):
    pine, pr = build_pinecone_vectors(rows, dimension=3)
    mil, mr = build_milvus_entities(rows, dimension=3)
    qdr, qr = build_qdrant_points(rows, dimension=3)
    wea, wr = build_weaviate_objects(rows, class_name="DataflowChunk", dimension=3)
    return {
        "pinecone": (pine, pr),
        "milvus": (mil, mr),
        "qdrant": (qdr, qr),
        "weaviate": (wea, wr),
    }


def _stored_content(name: str, built):
    if name == "pinecone":
        return built[0]["metadata"]["content"]
    if name == "milvus":
        return built[0]["content"]
    if name == "qdrant":
        return built[0]["payload"]["content"]
    return built[0]["properties"]["content"]


def _stored_source(name: str, built):
    if name == "pinecone":
        return built[0]["metadata"]["source_id"]
    if name == "milvus":
        return built[0]["source_id"]
    if name == "qdrant":
        return built[0]["payload"]["source_id"]
    return built[0]["properties"]["source_id"]


def test_vector_cell_token_present_and_absent():
    assert vector_cell_token(None) == ""
    assert vector_cell_token("") == ""
    assert vector_cell_token("   ") == ""
    assert vector_cell_token(SQL_NULL_SENTINEL) == ""
    assert vector_cell_token("__df_ddb_null__") == ""
    assert vector_cell_token(Missing) == ""
    assert vector_cell_token(DF_MISSING_SENTINEL) == ""
    assert vector_cell_token(0) == "0"
    assert vector_cell_token(False) == "false"
    assert vector_cell_token(True) == "true"
    assert vector_cell_token("true") == "true"
    assert vector_cell_token(True) != "True"
    assert vector_cell_token(float("nan")) == ""


def test_vector_fallback_material_refuses_absent_pair():
    assert vector_fallback_material(None, 0, SQL_NULL_SENTINEL) is None
    assert vector_fallback_material("", 0, "") is None
    assert vector_fallback_material(0, 0, None) == "0\x000\x00"
    assert vector_fallback_material("src", 1, True) == "src\x001\x00true"
    assert vector_fallback_material("src", 1, True) == vector_fallback_material(
        "src", 1, "true"
    )


def test_builders_store_zero_and_false_not_empty():
    for content, token in ((0, "0"), (False, "false")):
        built = _build_all([_row(content=content)])
        for name, (items, rejected) in built.items():
            assert rejected == [], name
            assert len(items) == 1, name
            assert _stored_content(name, items) == token, name


def test_builders_store_true_as_dest_true():
    built = _build_all([_row(content=True)])
    for name, (items, rejected) in built.items():
        assert rejected == [], name
        assert _stored_content(name, items) == "true", name
        assert _stored_content(name, items) != "True", name


def test_builders_store_reader_null_content_as_empty():
    for wire in (None, SQL_NULL_SENTINEL, "__df_ddb_null__", Missing):
        built = _build_all([_row(content=wire)])
        for name, (items, rejected) in built.items():
            assert rejected == [], (name, wire)
            assert _stored_content(name, items) == "", (name, wire)


def test_builders_store_source_id_on_dest_wire():
    built = _build_all([_row(source_id=True)])
    for name, (items, rejected) in built.items():
        assert rejected == [], name
        assert _stored_source(name, items) == "true", name

    null_built = _build_all([_row(source_id=SQL_NULL_SENTINEL)])
    for name, (items, rejected) in null_built.items():
        assert rejected == [], name
        assert _stored_source(name, items) == "", name


def test_fallback_id_true_shares_dest_true():
    native = _build_all([_row(id=None, content=True, source_id="src")])
    dest = _build_all([_row(id=None, content="true", source_id="src")])
    for name in native:
        a, ra = native[name]
        b, rb = dest[name]
        assert ra == [] and rb == [], name
        assert a[0]["id"] == b[0]["id"], name
        assert a[0]["id"], name


def test_fallback_id_zero_does_not_collide_with_empty():
    # empty content + empty source quarantines; source "doc" keeps empty present
    zero_only = _build_all([_row(id=None, content=0, source_id="")])
    empty_content = _build_all([_row(id=None, content="", source_id="doc")])
    for name in zero_only:
        z_items, z_rej = zero_only[name]
        e_items, e_rej = empty_content[name]
        assert z_rej == [] and e_rej == [], name
        assert z_items[0]["id"] != e_items[0]["id"], name
        assert z_items[0]["id"], name


def test_fallback_id_reader_null_pair_quarantines():
    built = _build_all(
        [_row(id=SQL_NULL_SENTINEL, content=SQL_NULL_SENTINEL, source_id=None)]
    )
    for name, (items, rejected) in built.items():
        assert items == [], name
        assert rejected, name
        assert rejected[0]["column"] == "id", name


def test_pinecone_fallback_uses_sha256_of_present_tokens():
    rows = [_row(id=None, content=True, source_id="src")]
    vectors, rejected = build_pinecone_vectors(rows, dimension=3)
    assert rejected == []
    material = vector_fallback_material("src", 0, True)
    assert material is not None
    assert vectors[0]["id"] == hashlib.sha256(material.encode("utf-8")).hexdigest()
