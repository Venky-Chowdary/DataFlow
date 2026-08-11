"""A live text destination must not receive an invented typed cast.

Sample inference legitimately calls a ``Y``/``N`` (or ``yes``/``no``) column
BOOLEAN — but ``transform_engine._parse_boolean`` coerces canonical wire only,
so writing that column into a destination that physically *is* ``TEXT`` failed
every row with ``Invalid boolean: 'Y'``. The sink stores the token verbatim, so
the cast can only invent a failure the destination never had.

The distinction is proven-live vs projected: a Map ``target_type`` stamp is a
projection (create-new still invents a BOOLEAN carrier and must keep the cast),
while ``dest_types`` comes from introspecting an existing object.
"""

from __future__ import annotations

import pytest

from services.schema_inference import infer_type
from services.transform_engine import (
    CANONICAL_BOOLEAN_TOKENS,
    _parse_boolean,
    infer_transform_for_mapping,
)
from services.transform_resolver import LiveDestTypes, resolve_transform

BOOL_MAP = {"source": "extra", "target": "extra", "target_type": "BOOLEAN"}


@pytest.mark.parametrize("token", sorted(CANONICAL_BOOLEAN_TOKENS))
def test_every_canonical_token_is_write_coercible(token: str) -> None:
    assert _parse_boolean(token) is not None


@pytest.mark.parametrize("token", ["Y", "N", "yes", "no", "on", "off"])
def test_informal_tokens_are_not_write_coercible(token: str) -> None:
    # Precisely why they must never be routed through the boolean transform.
    assert _parse_boolean(token) is None


@pytest.mark.parametrize(
    "samples", [["yes", "no"], ["Y", "N"], ["true", "false"], ["t", "f"]]
)
def test_boolean_inference_is_preserved(samples: list[str]) -> None:
    assert infer_type(samples, field_name="is_active") == "BOOLEAN"


@pytest.mark.parametrize("dest_type", ["TEXT", "VARCHAR", "LONGTEXT", "VARCHAR(50)"])
def test_live_text_destination_drops_the_boolean_cast(dest_type: str) -> None:
    resolved = resolve_transform(
        dict(BOOL_MAP),
        column_types={"extra": "BOOLEAN"},
        dest_types=LiveDestTypes({"extra": dest_type}),
    )
    assert resolved == "none"


@pytest.mark.parametrize("cased", ["extra", "EXTRA"])
def test_live_lookup_is_case_insensitive(cased: str) -> None:
    """Folding dialects (Oracle) report the column uppercased."""
    resolved = resolve_transform(
        dict(BOOL_MAP),
        column_types={"extra": "BOOLEAN"},
        dest_types=LiveDestTypes({cased: "TEXT"}),
    )
    assert resolved == "none"


@pytest.mark.parametrize("dest_type", ["BOOLEAN", "BOOL"])
def test_live_boolean_destination_still_casts(dest_type: str) -> None:
    resolved = resolve_transform(
        dict(BOOL_MAP),
        column_types={"extra": "BOOLEAN"},
        dest_types=LiveDestTypes({"extra": dest_type}),
    )
    assert resolved == "boolean"


def test_projected_text_target_keeps_the_cast() -> None:
    """No live probe: a create-new stamp is not evidence of a text carrier."""
    assert (
        infer_transform_for_mapping("is_gift", "IS_GIFT", "BOOLEAN", "VARCHAR")
        == "boolean"
    )
    assert (
        resolve_transform(
            {"source": "extra", "target": "extra", "target_type": "VARCHAR"},
            column_types={"extra": "BOOLEAN"},
            dest_types={},
        )
        == "boolean"
    )


def test_plain_dict_of_text_is_not_treated_as_proven_live() -> None:
    """Studio/Map hand the same argument a projection — provenance must gate it."""
    assert (
        resolve_transform(
            dict(BOOL_MAP),
            column_types={"extra": "BOOLEAN"},
            dest_types={"extra": "VARCHAR"},
        )
        == "boolean"
    )


def test_rematerialize_marks_its_output_live() -> None:
    """The canonical producer is the only source of the live marker."""
    from connectors.writer_common import rematerialize_live_dest_types

    live = rematerialize_live_dest_types(
        {"extra": "TEXT"}, ["extra"], product="PostgreSQL"
    )
    assert isinstance(live, LiveDestTypes)
    assert (
        resolve_transform(
            dict(BOOL_MAP), column_types={"extra": "BOOLEAN"}, dest_types=live
        )
        == "none"
    )
