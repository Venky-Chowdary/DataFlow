"""Accuracy review finding 2: the Pilot schema tool presents what Map would.

The bare scorer rates ``AMT -> amount`` 0.99 with no review flag; Map's
pipeline keeps it for review (no samples: weak type evidence). The Pilot tool
must show the pipeline's confidence and list the row as needing review.
"""

from __future__ import annotations

import src.ai.copilot.schema_tools as st

SRC = [{"name": "AMT", "inferred_type": "VARCHAR"}, {"name": "cust_nm", "inferred_type": "VARCHAR"}]
DST = [{"name": "amount", "inferred_type": "VARCHAR"}, {"name": "customer_name", "inferred_type": "VARCHAR"}]


class _Res:
    def __init__(self, out):
        self.success, self.output, self.error = True, out, None


def test_pilot_mapping_matches_map_pipeline(monkeypatch) -> None:
    from services.mapping_pipeline import run_mapping_pipeline

    monkeypatch.setattr(st, "introspect_connector_schema", lambda cid, name, table: _Res(
        {"type": "postgresql", "connector_name": name, "columns": SRC if name == "src" else DST}))
    res = st.map_connector_schemas(source_connector_name="src", source_table="t",
                                   dest_connector_name="dst", dest_table="t")
    assert res.success
    shown = {m["source"]: m for m in res.output["mappings"]}
    pipe = {m["source"]: m for m in run_mapping_pipeline(
        ["AMT", "cust_nm"], ["amount", "customer_name"], use_llm=False,
        source_schemas=[dict(c, samples=[]) for c in SRC], target_schemas=DST,
        destination_db_type="postgresql", source_db_type="postgresql", destination_table_exists=True,
    )["mappings"]}
    for name in ("AMT", "cust_nm"):
        assert shown[name]["confidence"] == pipe[name]["confidence"]
        if pipe[name].get("requires_review") or pipe[name]["confidence"] < 0.85:
            assert name in {r["source"] for r in res.output["low_confidence"]}
    assert all(not (m["confidence"] >= 0.85 and pipe[m["source"]].get("requires_review"))
               for m in res.output["mappings"])
