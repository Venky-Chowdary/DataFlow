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

    def find(self, query):
        below = (query.get("chain_seq") or {}).get("$lt")
        return _Cursor([d for d in self.docs if below is None or d.get("chain_seq", 0) < below])


def _run(monkeypatch, docs, *, scan, limit=5, before_seq=None):
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: _Coll(docs))
    monkeypatch.setattr(audit_log, "SEARCH_SCAN_LIMIT", scan)
    return audit_log.list_audit_events(
        limit=limit, search="needle", search_projection=lambda ev: ev, before_seq=before_seq
    )


def _docs(n, needles=()):
    # newest first, as the store sorts them
    return [
        {"action": "needle" if seq in needles else "x", "resource": "r", "actor": "a", "chain_seq": seq}
        for seq in range(n, 0, -1)
    ]


def test_a_search_that_walked_the_whole_window_says_it_was_truncated(monkeypatch):
    docs = _docs(10, needles={9, 3})
    page = _run(monkeypatch, docs, scan=4)
    assert len(page) == 1 and page.search_truncated is True
    # The cursor is the last record SCANNED (seq 7), not the last match.
    assert page.search_resume_before_seq == 7
    more = _run(monkeypatch, docs, scan=4, before_seq=page.search_resume_before_seq)
    assert [e["chain_seq"] for e in more] == [3]


def test_a_window_that_ends_exactly_at_the_last_record_is_not_truncated(monkeypatch):
    page = _run(monkeypatch, _docs(4, needles={2}), scan=4)
    assert len(page) == 1 and page.search_truncated is False and page.search_resume_before_seq is None


def test_a_search_that_filled_its_page_or_saw_everything_is_not(monkeypatch):
    docs = [{"action": "needle", "resource": "r", "actor": "a"} for _ in range(10)]
    assert _run(monkeypatch, docs, scan=20).search_truncated is False
    few = [{"action": "needle", "resource": "r", "actor": "a"}]
    assert _run(monkeypatch, few, scan=20).search_truncated is False


def test_the_route_reports_the_flag():
    from pathlib import Path

    router = (Path(__file__).resolve().parents[1] / "src/routers/audit_router.py").read_text()
    assert '"search_truncated": search_truncated' in router
    assert '"search_resume_before_seq": search_resume_before_seq' in router
