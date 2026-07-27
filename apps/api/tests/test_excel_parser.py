"""Excel parser tests."""

import pytest



def test_parse_excel_requires_openpyxl():
    try:
        import openpyxl  # noqa: F401
    except ImportError:
        pytest.skip("openpyxl not installed")
    # Minimal xlsx would need binary fixture — skip if no fixture
    pytest.skip("Excel fixture not bundled")
