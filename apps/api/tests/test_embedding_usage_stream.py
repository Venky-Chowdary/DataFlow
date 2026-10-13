from types import SimpleNamespace

from src.transfer.stream import _merge_embedding_usage, _writer_diagnostics


def test_writer_diagnostics_exposes_embedding_usage():
    usage = {
        "provider": "openai",
        "model": "text-embedding-3-small",
        "calls": 2,
        "retries": 1,
        "input_tokens": 45,
        "token_count_estimated": False,
        "cache_hits": 1,
        "cache_misses": 4,
        "estimated_cost_usd": 0.0000009,
    }
    result = SimpleNamespace(
        rejected_rows=0,
        coerced_null_rows=0,
        rows_skipped=0,
        rejected_details=[],
        warnings=[],
        meta={"embedding_usage": usage},
    )
    assert _writer_diagnostics(result)["embedding_usage"] == usage


def test_embedding_usage_accumulates_across_batches():
    first = {
        "provider": "openai",
        "model": "text-embedding-3-small",
        "calls": 1,
        "retries": 0,
        "input_tokens": 10,
        "token_count_estimated": False,
        "cache_hits": 2,
        "cache_misses": 1,
        "estimated_cost_usd": 0.0000002,
    }
    second = {
        **first,
        "calls": 3,
        "retries": 1,
        "input_tokens": 40,
        "token_count_estimated": True,
        "cache_hits": 4,
        "cache_misses": 5,
        "estimated_cost_usd": 0.0000008,
    }
    accumulated = _merge_embedding_usage(None, first)
    assert accumulated == first
    accumulated = _merge_embedding_usage(accumulated, second)
    assert accumulated == {
        **first,
        "calls": 4,
        "retries": 1,
        "input_tokens": 50,
        "token_count_estimated": True,
        "cache_hits": 6,
        "cache_misses": 6,
        "estimated_cost_usd": 0.000001,
    }

    second["estimated_cost_usd"] = None
    assert _merge_embedding_usage(first, second)["estimated_cost_usd"] is None
