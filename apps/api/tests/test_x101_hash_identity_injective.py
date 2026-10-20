"""X-101 — hash_identity must be injective.

rk1 joined cells with a raw \x1f and sentineled NULL as "\x1eEMPTY", so
(fullname='a\x1f', phone='b') and (fullname='a', phone='\x1fb') hashed to
the same row key — and literal '\x1eEMPTY' collided with NULL. Gate-8 uses
this key for alignment when there is no PK, so a collision silently
mis-aligned rows on all 5 engines. rk2 tags and length-prefixes every
field: A=absent, N=NULL, E=blank non-NULL, V=value.
"""

from __future__ import annotations

from services.shape_engine import _hash_identity_value

COLS = ["fullname", "phone"]


def _h(**row):
    return _hash_identity_value(row, COLS)


def test_separator_bytes_inside_values_cannot_collide():
    # The exact live pair: \x1f inside a value vs as a field boundary.
    assert _h(fullname="a\x1f", phone="b") != _h(fullname="a", phone="\x1fb")
    assert _h(fullname="a\x1fb", phone="c") != _h(fullname="a", phone="b\x1fc")


def test_sentinel_text_cannot_collide_with_tags():
    # Literal sentinel-looking text is data, not a tag.
    assert _h(fullname="\x1eEMPTY", phone="x") != _h(fullname=None, phone="x")
    assert _h(fullname="\x1eABSENT", phone="x") != _h(phone="x")
    assert _h(fullname="rk2", phone="x") != _h(fullname="", phone="x")


def test_null_empty_absent_are_three_distinct_states():
    n = _h(fullname=None, phone="x")
    e = _h(fullname="", phone="x")
    a = _h(phone="x")
    assert len({n, e, a}) == 3


def test_whitespace_blank_is_not_null_or_empty():
    assert _h(fullname=" ", phone="x") not in {
        _h(fullname=None, phone="x"),
        _h(fullname="", phone="x"),
    }


def test_same_inputs_same_key():
    assert _h(fullname="a", phone="b") == _h(fullname="a", phone="b")
    assert _h(fullname=None, phone="") == _h(fullname=None, phone="")


def test_different_lengths_never_collide():
    # Field boundaries are length-bound: no concatenation trick can merge
    # or split fields into a shared payload.
    assert _h(fullname="ab", phone="c") != _h(fullname="a", phone="bc")
    assert _h(fullname="ab", phone="c") != _h(fullname="abc", phone="")
