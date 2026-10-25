"""UI-1: a disabled policy checkbox must be visibly distinct from an enabled
empty one (3:1 non-text contrast, both themes).

Regression: a native ``input[type=checkbox]:disabled`` is rendered identically
to an unchecked enabled box at narrow widths, so the OCR toggle read as
"available, off" when it was actually "unavailable". The fix custom-draws the
box: enabled keeps the token border, checked gets the brand fill + check,
disabled gets a dashed border + sunken fill + diagonal slash.
"""
import re
from pathlib import Path

CSS = (
    Path(__file__).resolve().parents[2]
    / "web"
    / "src"
    / "styles"
    / "dataflow-ui.css"
)


def _block(css: str, selector: str) -> str:
    m = re.search(re.escape(selector) + r"\s*\{([^}]*)\}", css)
    assert m, f"missing rule block for {selector!r}"
    return m.group(1)


def test_policy_toggle_box_is_custom_drawn():
    body = _block(CSS.read_text(), ".df2-policy-toggle input")
    # appearance:none is what lets the disabled state look different at all
    assert "appearance: none" in body


def test_disabled_box_has_distinguishing_marker():
    body = _block(CSS.read_text(), ".df2-policy-toggle input:disabled")
    # three independent visual differences vs the enabled-empty box
    assert "dashed" in body, "disabled border must differ (dashed)"
    assert "not-allowed" in body
    assert "--df-surface-sunken" in body or "--df-surface-muted" in body, (
        "disabled fill must be a token so dark theme remaps it"
    )


def test_disabled_box_has_slash_glyph():
    body = _block(CSS.read_text(), ".df2-policy-toggle input:disabled::before")
    # the slash is the non-text-contrast signal; it must use the tertiary ink
    # token (4.8:1 on the light sunken fill, 5+:1 on the dark one)
    assert "--df-text-tertiary" in body
    assert "rotate" in body, "slash needs a diagonal rotation"


def test_checked_box_uses_brand_fill():
    body = _block(CSS.read_text(), ".df2-policy-toggle input:checked")
    assert "--df-brand" in body


def test_disabled_label_is_dimmed_and_not_clickable():
    css = CSS.read_text()
    body = _block(css, ".df2-policy-toggle:has(input:disabled)")
    assert "not-allowed" in body
    assert "strong" in css and ":has(input:disabled) strong" in css
