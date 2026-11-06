"""Second-pass review #5: a projected audit search says when it stopped at its scan window.

A masked reader's search runs over at most ``SEARCH_SCAN_LIMIT`` records
(Browser 20, 257439737). Fewer matches than asked, after walking the whole
window, is not "no more results": the page now carries ``search_truncated``.
"""

from __future__ import annotations

import services.audit_log as audit_log


class _Cursor(list):
    def sort(self, *_a, **_k):
        return self

    def limit(self, n):
        return _Cursor(self[:n])


class _Coll:
    def __init__(self, docs):
        self.docs = docs

    def find(self, _query):
        return _Cursor(self.docs)


def _run(monkeypatch, docs, *, scan, limit=5):
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: _Coll(docs))
    monkeypatch.setattr(audit_log, "SEARCH_SCAN_LIMIT", scan)
    return audit_log.list_audit_events(limit=limit, search="needle", search_projection=lambda ev: ev)


def test_a_search_that_walked_the_whole_window_says_it_was_truncated(monkeypatch):
    docs = [{"action": "x", "resource": "r", "actor": "a"} for _ in range(10)]
    docs[2]["resource"] = "needle"
    page = _run(monkeypatch, docs, scan=4)
    assert len(page) == 1 and page.search_truncated is True


def test_a_search_that_filled_its_page_or_saw_everything_is_not(monkeypatch):
    docs = [{"action": "needle", "resource": "r", "actor": "a"} for _ in range(10)]
    assert _run(monkeypatch, docs, scan=20).search_truncated is False
    few = [{"action": "needle", "resource": "r", "actor": "a"}]
    assert _run(monkeypatch, few, scan=20).search_truncated is False


def test_the_route_reports_the_flag():
    from pathlib import Path

    router = (Path(__file__).resolve().parents[1] / "src/routers/audit_router.py").read_text()
    assert '"search_truncated": search_truncated' in router
