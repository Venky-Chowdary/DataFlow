"""Embedding / VECTOR inference components use vector_component_carrier.

float(item) invented Auto 1.234 and collapsed 2**53+1 on Weaviate / Milvus /
Qdrant / Pinecone / pgvector writes. Native IEEE floats pass through.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from services.schema_inference import infer_type  # noqa: E402
from services.vector_embedding import coerce_embedding  # noqa: E402


def test_plain_ieee_embedding_still_binds():
    vals, err = coerce_embedding([0.1, 0.2, 0.3], expected_dimension=3)
    assert err is None
    assert vals == [0.1, 0.2, 0.3]
    parsed, perr = coerce_embedding("[0.1, 0.2, 0.3]", expected_dimension=3)
    assert perr is None
    assert parsed == [0.1, 0.2, 0.3]


def test_locale_money_component_binds():
    vals, err = coerce_embedding(["$1.50", "2"])
    assert err is None
    assert vals == [1.5, 2.0]


def test_auto_grouping_and_bool_refuse():
    vals, err = coerce_embedding(["1.234", "2"])
    assert vals is None and err and "refuse invent" in err
    vals, err = coerce_embedding(["1,234", "2"])
    assert vals is None and err and "refuse invent" in err
    vals, err = coerce_embedding([True, 1.0])
    assert vals is None and err and "refuse invent" in err


def test_ieee_lossy_mantissa_refuses_write_and_infer():
    vals, err = coerce_embedding([9007199254740993, 1.0])
    assert vals is None and err and "refuse invent" in err
    # Eight dims so unnamed samples meet VECTOR min-dim; collapsed mantissa
    # must not invent VECTOR(8).
    huge = "[" + ",".join(["9007199254740993"] * 8) + "]"
    plain = "[" + ",".join(["0.1"] * 8) + "]"
    assert infer_type([huge, huge]) != "VECTOR(8)"
    assert infer_type([plain, plain]) == "VECTOR(8)"
