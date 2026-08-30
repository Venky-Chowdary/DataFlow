"""PRODUCTION_SKU honesty: every committed route validates or skips with reason.

Routes whose optional DBAPI / cloud drivers are missing must not silently fail —
they skip with an explicit driver-unavailable message. Planned brands must never
appear in PRODUCTION_SKU as live.
"""

from __future__ import annotations

import pytest

from services.sku_honesty import route_driver_gap
from src.transfer.connector_capabilities import resolve_driver_type
from src.transfer.registry import LIVE_MATRIX, PRODUCTION_SKU, validate_transfer


def _route_skip_reason(src_fmt: str, dst_fmt: str) -> str | None:
    return route_driver_gap(src_fmt, dst_fmt)


@pytest.mark.parametrize(
    "route",
    PRODUCTION_SKU,
    ids=lambda r: f"{r[0]}_{r[1]}_to_{r[2]}_{r[3]}",
)
def test_production_sku_validate_or_explicit_skip(route: tuple[str, str, str, str]) -> None:
    src_kind, src_fmt, dst_kind, dst_fmt = route
    skip = _route_skip_reason(src_fmt, dst_fmt)
    ok, msg = validate_transfer(src_kind, src_fmt, dst_kind, dst_fmt)
    if skip:
        if not ok:
            pytest.skip(f"{skip}; validate_transfer={msg}")
        # Driver missing but route still in LIVE_MATRIX — document skip for execute tests.
        pytest.skip(skip)
    assert ok, f"PRODUCTION_SKU route must validate when drivers present: {route} → {msg}"
    assert "Planned" not in msg, f"PRODUCTION_SKU must not include Planned brands: {route} → {msg}"


def test_production_sku_routes_are_reachable() -> None:
    """Every committed SKU route must be declared live — no driver probe involved.

    ``test_production_sku_validate_or_explicit_skip`` skips whenever an optional
    DBAPI is absent, so a route that is unreachable for a *non-driver* reason
    (a destination declaring ``preflight: False`` / ``introspect: False``, say)
    stays invisible on hosts that lack the package. LIVE_MATRIX membership is a
    static declaration, so this invariant cannot be skipped into silence and
    fails the moment PRODUCTION_SKU advertises a route Validate would refuse.
    """
    unreachable = [route for route in PRODUCTION_SKU if route not in LIVE_MATRIX]
    assert not unreachable, (
        "PRODUCTION_SKU advertises routes absent from LIVE_MATRIX, so validate_sku "
        f"can never pass them: {unreachable}"
    )


def test_production_sku_has_no_duplicate_routes() -> None:
    """A duplicated route inflates the committed-route count we quote to buyers."""
    duplicates = sorted({route for route in PRODUCTION_SKU if PRODUCTION_SKU.count(route) > 1})
    assert not duplicates, f"PRODUCTION_SKU contains duplicate routes: {duplicates}"


def test_production_sku_has_no_planned_rest_stubs() -> None:
    for route in PRODUCTION_SKU:
        _, src_fmt, _, dst_fmt = route
        for fmt in (src_fmt, dst_fmt):
            driver = resolve_driver_type(fmt)
            assert fmt not in {"zendesk", "shopify", "netsuite", "servicenow"}, route
            if driver == "rest_api" and fmt != "rest_api":
                pytest.fail(f"REST brand stub in PRODUCTION_SKU: {route}")
