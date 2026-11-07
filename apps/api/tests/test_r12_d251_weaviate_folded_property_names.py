"""D251 (prod job 6ae924a24fb8a205cd02ee12): Oracle QAENV_SEED -> new Weaviate
class stored NAME as ``nAME``, UPDATED_AT as ``uPDATED_AT`` and ID as ``iD``
(Weaviate lower-cases a property's first letter). Folded upper-case names are
now stored lower-case, ID is the object identity like ``id``, and Gate-8 reads
each field back under the stored name and discloses the mapping.
"""
from __future__ import annotations


def _row(meta):
    return {
        "source_id": "1", "chunk_index": 0, "content": "c",
        "embedding": [0.1, 0.2], "metadata": meta,
    }


def test_upper_case_columns_are_stored_lower_case():
    from connectors.weaviate_writer import build_weaviate_objects

    objects, rejected = build_weaviate_objects(
        [_row({"ID": 1, "NAME": "n", "UPDATED_AT": "2026-11-02T12:00:00.654321"})],
        class_name="MxOra", dimension=2,
    )
    assert rejected == []
    props = objects[0]["properties"]
    assert props["name"] == "n" and props["updated_at"].endswith(".654321")
    assert not {"nAME", "uPDATED_AT", "iD", "ID", "id"} & set(props)


def test_existing_class_property_keeps_its_spelling():
    from connectors.weaviate_writer import build_weaviate_objects

    objects, _ = build_weaviate_objects(
        [_row({"NAME": "n"})], class_name="MxOra", dimension=2,
        live_property_names=["nAME", "content"],
    )
    assert objects[0]["properties"]["nAME"] == "n"


def test_mixed_case_and_writer_owned_names_follow_weaviate_rule():
    from connectors.weaviate_writer import weaviate_property_name

    assert weaviate_property_name("FirstName") == "firstName"
    assert weaviate_property_name("name") == "name"
    assert weaviate_property_name("CONTENT") == "cONTENT"
    assert weaviate_property_name("SOURCE_ID") == "sOURCE_ID"


def test_two_keys_never_share_one_property():
    from connectors.weaviate_writer import build_weaviate_objects

    objects, _ = build_weaviate_objects(
        [_row({"NAME": "upper", "name": "lower"})], class_name="MxOra", dimension=2,
    )
    props = objects[0]["properties"]
    assert props["name"] == "lower" and props["nAME"] == "upper"


def test_gate8_reads_fields_under_stored_names_and_discloses(monkeypatch):
    import connectors.weaviate_writer as ww
    from services.weaviate_document_verify import gate8_weaviate_report

    src = [{"ID": 1, "NAME": "n1"}, {"ID": 2, "NAME": "n2"}]
    docs = [
        {"source_id": "1", "id": "1", "name": "n1", "chunk_index": 0, "_df_object_count": 1},
        {"source_id": "2", "id": "2", "name": "n2", "chunk_index": 0, "_df_object_count": 1},
    ]
    monkeypatch.setattr(
        ww, "read_document_properties",
        lambda cfg, table_name: (docs, {"name": "text", "source_id": "text"}),
    )
    report = gate8_weaviate_report(
        cfg={}, table_name="MxOra",
        mappings=[{"source": "ID", "target": "ID"}, {"source": "NAME", "target": "NAME"}],
        source_rows=2, source_endpoint=None,
        read_sample=lambda *a, **k: src, run_records=src, base={},
    )
    assert report["passed"] is True, report["message"]
    assert report["document_value_proof"]["property_names"] == {"ID": "id", "NAME": "name"}
    assert "NAME -> name" in report["message"]
