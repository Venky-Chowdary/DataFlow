"""Ops review Medium 7: the control-plane backend switch and its cache are documented.

With ``CONTROL_STORE_BACKEND`` unset, a single-node install whose MongoDB is
connected moved its tenant, ACL and BYOK stores from files to the database
on upgrade, and reads are cached for 2 s; neither was written down outside
code and Helm values. The operator notes in the chart must name every
setting the control-plane store modules read, the default backend rule and
the cache window.
"""

from __future__ import annotations

import re
from pathlib import Path

API = Path(__file__).resolve().parents[1]
README = API.parents[1] / "deploy/enterprise/helm/dataflow/README.md"
MODULES = ("services/shared_doc_store.py", "services/control_plane_bounds.py")


def _settings() -> set[str]:
    names: set[str] = set()
    for module in MODULES:
        names |= set(re.findall(r'(?:getenv_brand|_setting)\(\s*"([A-Z0-9_]+)"', (API / module).read_text(encoding="utf-8")))
    return names


def test_every_control_plane_setting_is_documented():
    text = README.read_text(encoding="utf-8")
    names = _settings()
    assert {"CONTROL_STORE_BACKEND", "CONTROL_STORE_CACHE_SECONDS", "CONTROL_STORE_TIMEOUT_SECONDS"} <= names
    missing = sorted(name for name in names if f"DATAWRAP_{name}" not in text and f"DATAFLOW_{name}" not in text)
    assert not missing, f"operator notes do not document: {missing}"


def test_the_default_backend_rule_and_the_cache_window_are_explained():
    text = README.read_text(encoding="utf-8")
    section = text[text.index("## Choosing the backend"):]
    for phrase in ("MULTI_REPLICA", "unset", "file", "mongo", "2 s", "revoked"):
        assert phrase in section, phrase
