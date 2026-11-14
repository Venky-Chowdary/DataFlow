"""The set-based engine stays honest about its scope: no shipped set engine, no
exactly-once claim, and the six wizard labels unchanged.

This was the regression for the phase-1 design note
(the set-based engine plan, under the old docs tree). 1415f1f0f ("chore: remove
non-runtime documentation") deleted that note with the rest of ``docs/``, and
the note no longer described the code (it said the canvas files were not on
this branch and that join/lookup/aggregate were future work; both changed).
The constraints it existed to protect now live in the product itself, so the
checks read them there:

- ``services/set_graph.py``: the set engine is published as unavailable, no set
  operation is executable, and post-load-only operations stay off the
  executable list (``test_set_graph_contract.py`` covers the refusal paths).
- ``services/cdc_effectively_once.py`` / ``cdc_exactly_once.py``: delivery is
  at-least-once; exactly-once is not claimed.
- ``apps/web/src/pages/transfer/studioConstants.ts``: the wizard labels are an
  operator contract.
"""

import re
from pathlib import Path

WIZARD_STEPS = Path(__file__).resolve().parents[3] / "apps" / "web" / "src" / "pages" / "transfer" / "studioConstants.ts"


def test_set_engine_is_published_as_unavailable_and_runs_nothing():
    from services.set_graph import (
        POST_LOAD_ONLY_OPERATIONS,
        catalog_set_fields,
        executable_set_ops,
    )

    catalog = catalog_set_fields()
    assert catalog["set_engine"]["available"] is False
    assert catalog["set_operations"] == []
    assert executable_set_ops() == frozenset()
    post_load = catalog["post_load_only"]
    assert post_load["operations"] == list(POST_LOAD_ONLY_OPERATIONS)
    assert POST_LOAD_ONLY_OPERATIONS, "post-load-only operations must stay published"
    assert not set(POST_LOAD_ONLY_OPERATIONS) & executable_set_ops()
    assert "after the load" in post_load["reason"]


def test_delivery_stays_at_least_once_and_exactly_once_is_not_claimed():
    from services.cdc_effectively_once import EXACTLY_ONCE_CLAIMED
    from services.cdc_exactly_once import PLATFORM_EXACTLY_ONCE_CLAIMED

    assert EXACTLY_ONCE_CLAIMED is False
    assert PLATFORM_EXACTLY_ONCE_CLAIMED is False


def test_wizard_labels_are_unchanged():
    # The wizard labels are an operator contract; nothing may rename them.
    text = WIZARD_STEPS.read_text(encoding="utf-8")
    labels = re.findall(r'\{ n: STEP_\w+, label: "([^"]+)"', text)
    assert labels == ["Source", "Destination", "Transform", "Map", "Validate", "Run"]
