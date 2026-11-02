"""Gate-8 digests the mapped projection, not whatever the destination row holds."""

from __future__ import annotations

from services.readback_projection import project_readback


def test_untouched_destination_column_is_excluded_from_the_digest():
    names = ["id", "email", "note"]
    rows = [(1, "a@x.com", None), (2, "b@x.com", None)]

    cols, projected = project_readback(names, ["id", "email"], iter(rows))

    assert cols == ["id", "email"]
    assert list(projected) == [(1, "a@x.com"), (2, "b@x.com")]


def test_destination_spelling_wins_over_the_mapping_spelling():
    cols, projected = project_readback(["ID", "EMAIL", "NOTE"], ["id", "email"], iter([(1, "a")]))

    assert cols == ["ID", "EMAIL"]
    assert list(projected) == [(1, "a")]


def test_missing_mapped_column_keeps_the_full_row_so_the_mismatch_surfaces():
    """A mapped column the destination never returned is a real proof failure."""
    names = ["id", "note"]
    rows = [(1, None)]

    cols, projected = project_readback(names, ["id", "email"], iter(rows))

    assert cols == names
    assert list(projected) == rows


def test_dict_rows_are_projected_by_name():
    rows = [{"id": 1, "email": "a@x.com", "note": None}]

    cols, projected = project_readback(["id", "email", "note"], ["id", "email"], iter(rows))

    assert cols == ["id", "email"]
    assert list(projected) == [{"id": 1, "email": "a@x.com"}]


def test_full_width_and_empty_requests_are_left_alone():
    names = ["id", "email"]
    rows = [(1, "a@x.com")]

    assert project_readback(names, [], iter(rows))[0] == names
    assert project_readback(names, names, iter(rows))[0] == names
    assert project_readback([], ["id"], iter(rows))[0] == ["id"]


def test_short_row_pads_rather_than_raising():
    cols, projected = project_readback(["id", "email", "note"], ["id", "note"], iter([(1,)]))

    assert cols == ["id", "note"]
    assert list(projected) == [(1, None)]
