"""Saving a contract must not rewrite the whole store.

The file store used to hold every contract in one JSON blob, so each save read,
re-encoded and rewrote all of them — a 10k-contract store spent seconds per
transfer — and two workers saving different contracts at once lost one write.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from concurrent.futures import ThreadPoolExecutor  # noqa: E402

from services.contract_file_store import FileContractStore  # noqa: E402
from services.data_contract import DataContract  # noqa: E402


def _contract(index: int) -> DataContract:
    return DataContract(
        id=f"dfc-{index:06d}",
        name=f"contract-{index}",
        metadata={"note": "x" * 512},
    )


def test_save_cost_does_not_grow_with_store_size(tmp_path):
    store = FileContractStore(path=tmp_path / "contracts.json")

    for i in range(200):
        store.save_contract(_contract(i))

    start = time.perf_counter()
    store.save_contract(_contract(200))
    small = time.perf_counter() - start

    for i in range(201, 2_000):
        store.save_contract(_contract(i))

    start = time.perf_counter()
    store.save_contract(_contract(2_000))
    large = time.perf_counter() - start

    # 10x the records must not cost meaningfully more per save.
    assert large < max(small * 4, 0.05), (small, large)
    assert store.get_contract("dfc-002000") is not None


def test_concurrent_saves_keep_every_contract(tmp_path):
    store = FileContractStore(path=tmp_path / "contracts.json")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: store.save_contract(_contract(i)), range(400)))

    for i in range(400):
        assert store.get_contract(f"dfc-{i:06d}") is not None, i


def test_legacy_single_file_store_is_migrated(tmp_path):
    legacy = tmp_path / "contracts.json"
    legacy.write_text(
        '{"contracts": {"dfc-000001": {"id": "dfc-000001", "name": "legacy"}},'
        ' "breakers": {}}',
        encoding="utf-8",
    )

    store = FileContractStore(path=legacy)

    kept = store.get_contract("dfc-000001")
    assert kept is not None and kept.name == "legacy"
    assert not legacy.exists()
    assert (tmp_path / "contracts.json.migrated").exists()
