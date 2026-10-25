"""UI-2: keyboard focus must be visible — one --df-focus-ring token (solid,
>=3:1 on both themes' surfaces), applied via :focus-visible / :focus-within,
and no bare `outline:none` on an interactive field without a ring somewhere.

Regression: --df-focus-ring resolved to rgba(13,148,136,0.28) (~1.5:1); seven
fields stripped outline with no replacement (df2-sql-input, the dest
connector/engine search inputs, .docs-search input, the pilot composer
textarea); .df2-command-search:focus-within was defined 4x with a ~1.3:1 ring.
"""
import re
from pathlib import Path

STYLES = Path(__file__).resolve().parents[2] / "web" / "src" / "styles"


def _css(name: str) -> str:
    return (STYLES / name).read_text()


def _blocks(css: str, selector_pat: str) -> list[str]:
    return [
        m.group(1)
        for m in re.finditer(selector_pat + r"\s*\{([^}]*)\}", css)
    ]


def test_focus_ring_token_is_solid_in_light_theme():
    tokens = _css("tokens.css")
    m = re.search(r"--df-focus-ring:\s*([^;]+);", tokens)
    assert m, "--df-focus-ring missing"
    val = m.group(1).strip()
    # must resolve to the solid brand, not the 0.28-alpha brand-ring
    assert "rgba" not in val and "brand-ring" not in val


def test_focus_ring_token_has_dark_override():
    tokens = _css("tokens.css")
    dark = tokens[tokens.index('data-theme="dark"') :]
    m = re.search(r"--df-focus-ring:\s*(#[0-9a-fA-F]{3,8})\s*;", dark)
    assert m, "dark theme has no solid --df-focus-ring override"


def test_app_focus_visible_uses_token():
    css = _css("ui-consistency.css")
    blocks = _blocks(css, r"\.df2-app :focus-visible")
    assert blocks
    assert any("--df-focus-ring" in b for b in blocks)


def test_sql_input_overlay_has_focus_ring():
    css = _css("transfer-studio.css")
    blocks = _blocks(css, r"\.df2-sql-input:focus-visible")
    assert blocks, "transparent sql overlay needs its own ring"
    assert any("--df-focus-ring" in b for b in blocks)


def test_borderless_inputs_have_wrapper_focus_within():
    cases = [
        ("transfer-studio.css", r"\.df2-dest-connector-search:focus-within"),
        ("transfer-studio.css", r"\.df2-dest-engine-search:focus-within"),
        ("docs-page.css", r"\.docs-search:focus-within"),
        ("pilot-chat.css", r"\.df2-pilot-composer-bar:focus-within"),
    ]
    for fname, pat in cases:
        blocks = _blocks(_css(fname), pat)
        assert blocks, f"{fname}: missing {pat}"
        assert any("--df-focus-ring" in b for b in blocks), fname


def test_command_search_ring_is_token_and_strong():
    for fname in ("dataflow-ui.css", "premium-theme.css",
                  "shell-polish.css", "enterprise-ui.css"):
        css = _css(fname)
        blocks = _blocks(css, r"(?:\.df2-app )?\.df2-command-search:focus-within")
        for b in blocks:
            assert "0.12)" not in b and "0.14)" not in b and "#99f6e4" not in b, (
                f"{fname}: command-search ring below 3:1"
            )
            assert "--df-focus-ring" in b or "--df-brand" in b, fname
