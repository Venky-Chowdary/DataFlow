"""Guard: web multi-stream helper never treats 'a, b' as one object name."""

from __future__ import annotations

from pathlib import Path


def test_source_streams_helper_splits_csv_names():
    root = Path(__file__).resolve().parents[2]
    helper = (root / "web" / "src" / "lib" / "sourceStreams.ts").read_text(encoding="utf-8")
    assert "export function parseStreamNames" in helper
    assert "split(\",\")" in helper
    page = (root / "web" / "src" / "pages" / "TransferPage.tsx").read_text(encoding="utf-8")
    assert "parseStreamNames" in page
    assert "primarySourceStream" in page
    assert "MultiStreamSchemaPreview" in (root / "web" / "src" / "components" / "transfer" / "SourceStepAside.tsx").read_text(
        encoding="utf-8"
    )
