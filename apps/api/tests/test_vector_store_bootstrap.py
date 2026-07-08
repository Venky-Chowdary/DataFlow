import os
import shutil

from src.ai.rag.bootstrap import rebuild_vector_store


def test_rebuild_vector_store_recreates_knowledge_index(tmp_path, monkeypatch):
    persist_dir = tmp_path / "vector_store"
    monkeypatch.setenv("DATAFLOW_VECTOR_STORE_DIR", str(persist_dir))

    result = rebuild_vector_store()

    assert result["ingested"] > 0
    assert result["patterns"] > 0
    assert persist_dir.exists()
    assert result["documents"] >= result["ingested"]
