"""``get_capabilities`` looks each endpoint up in the catalog once per call (PR #433 CI).

Pilot reads the capability matrix on every chat turn. Each of its ~2,600 live
route combinations looked its source and its destination up in the catalog
again (``get_connector_by_id`` enriches the entry: proof files, host policy,
environment reads), ~5,200 lookups and ~0.25 s a turn for ~100 distinct
endpoints. The wave-45 answer corpus (1,805 turns) spent most of its time
there and ran past its 900 s timeout on a slower CI runner. The categories in
the answer are unchanged (QA D08); only the repeated lookups go.
"""

from __future__ import annotations


def test_each_endpoint_category_is_read_once_and_stays_the_catalog_s(monkeypatch):
    from services import catalog_service
    from src.transfer.registry import get_capabilities

    real = catalog_service.get_connector_by_id
    looked_up: list[str] = []

    def counting(connector_id, *args, **kwargs):
        looked_up.append(str(connector_id))
        return real(connector_id, *args, **kwargs)

    monkeypatch.setattr(catalog_service, "get_connector_by_id", counting)
    caps = get_capabilities()
    combos = caps["live_combinations"]
    assert len(combos) > 100, "the matrix this guards is the full live one"

    formats = {c["source_format"].lower() for c in combos} | {c["dest_format"].lower() for c in combos}
    assert len(looked_up) <= len(formats), (
        f"{len(looked_up)} catalog lookups for {len(formats)} distinct endpoints"
    )

    # The categories are still the catalog's, endpoint by endpoint (QA D08).
    for combo in combos:
        for side in ("source", "dest"):
            entry = real(combo[f"{side}_format"].lower()) or {}
            assert combo[f"{side}_category"] == (str(entry.get("category") or "") or "database"), combo
