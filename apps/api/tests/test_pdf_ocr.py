"""PDF OCR proofs — real dependency probes + fail-closed paths (no mocks)."""

from __future__ import annotations

import io
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _make_text_pdf(text: str) -> bytes:
    safe = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")[:200]
    content_stream = f"BT /F1 12 Tf 50 700 Td ({safe}) Tj ET".encode("latin-1", errors="replace")
    objects = [
        b"1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n",
        b"2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj\n",
        (
            b"3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>endobj\n"
        ),
        (
            f"4 0 obj<< /Length {len(content_stream)} >>stream\n".encode("ascii")
            + content_stream
            + b"\nendstream\nendobj\n"
        ),
        b"5 0 obj<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>endobj\n",
    ]
    header = b"%PDF-1.4\n"
    offsets = [0]
    pos = len(header)
    body_parts = []
    for obj in objects:
        offsets.append(pos)
        body_parts.append(obj)
        pos += len(obj)
    body = b"".join(body_parts)
    xref_pos = pos
    xref = [f"xref\n0 {len(offsets)}\n".encode("ascii"), b"0000000000 65535 f \n"]
    for off in offsets[1:]:
        xref.append(f"{off:010d} 00000 n \n".encode("ascii"))
    trailer = (
        f"trailer<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode(
            "ascii"
        )
    )
    return header + body + b"".join(xref) + trailer


def _make_blank_page_pdf() -> bytes:
    """PDF with a page but no text operators — text-layer extract yields empty."""
    objects = [
        b"1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n",
        b"2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj\n",
        b"3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] >>endobj\n",
    ]
    header = b"%PDF-1.4\n"
    offsets = [0]
    pos = len(header)
    body_parts = []
    for obj in objects:
        offsets.append(pos)
        body_parts.append(obj)
        pos += len(obj)
    body = b"".join(body_parts)
    xref_pos = pos
    xref = [f"xref\n0 {len(offsets)}\n".encode("ascii"), b"0000000000 65535 f \n"]
    for off in offsets[1:]:
        xref.append(f"{off:010d} 00000 n \n".encode("ascii"))
    trailer = (
        f"trailer<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode(
            "ascii"
        )
    )
    return header + body + b"".join(xref) + trailer


def test_ocr_dependency_status_shape():
    from services.pdf_ocr import ocr_dependency_status

    status = ocr_dependency_status()
    assert "available" in status
    assert "message" in status
    assert isinstance(status["present"], list)
    assert isinstance(status["missing"], list)


def test_blank_pdf_without_ocr_fail_closed():
    from services.file_parser import FileParser

    result = FileParser.parse(_make_blank_page_pdf(), "blank.pdf", enable_ocr=False)
    assert not result.success
    assert "OCR" in (result.error or "")
    assert "not supported yet" not in (result.error or "").lower()


def test_blank_pdf_with_ocr_when_unavailable_fail_closed():
    from services.file_parser import FileParser
    from services.pdf_ocr import ocr_available

    available, _ = ocr_available()
    if available:
        pytest.skip("OCR stack present — cannot assert unavailable fail-closed here")

    result = FileParser.parse(_make_blank_page_pdf(), "blank.pdf", enable_ocr=True)
    assert not result.success
    err = (result.error or "").lower()
    assert "ocr" in err or "tesseract" in err or "unavailable" in err


def test_text_layer_pdf_does_not_need_ocr():
    from services.file_parser import FileParser

    result = FileParser.parse(_make_text_pdf("Hello DataFlow OCR"), "text.pdf", enable_ocr=False)
    assert result.success, result.error
    assert result.row_count >= 1
    assert result.ocr_used is False
    assert any("Hello" in str(r.get("content", "")) for r in result.data)


def test_capabilities_include_ocr_status():
    from src.transfer.registry import get_capabilities

    caps = get_capabilities()
    assert "ocr" in caps
    assert "available" in caps["ocr"]
    assert "message" in caps["ocr"]


def test_engine_read_source_honors_enable_ocr_flag():
    """Transfer source.extra.enable_ocr reaches FileParser (text PDF path)."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    engine = UniversalTransferEngine()
    req = TransferRequest(
        source=EndpointConfig(kind="file", format="pdf", extra={"enable_ocr": True}),
        destination=EndpointConfig(kind="database", format="postgresql"),
        source_filename="text.pdf",
        source_content=_make_text_pdf("Engine OCR flag"),
    )
    rows, columns, schema = engine._read_source(req)
    assert rows
    assert "content" in columns
    assert schema


@pytest.mark.skipif(
    __import__("services.pdf_ocr", fromlist=["ocr_available"]).ocr_available()[0] is False,
    reason="Tesseract / pypdfium2 / Pillow not available",
)
def test_live_ocr_image_roundtrip():
    """When Tesseract is installed, OCR a rendered PIL image with clear glyphs."""
    from PIL import Image, ImageDraw, ImageFont

    from services.pdf_ocr import ocr_image

    img = Image.new("RGB", (400, 80), color=(255, 255, 255))
    draw = ImageDraw.Draw(img)
    draw.text((10, 20), "DataFlow", fill=(0, 0, 0), font=ImageFont.load_default())
    text = ocr_image(img)
    assert text
    # OCR of tiny default font can be imperfect — require at least a letter match.
    assert any(ch.isalpha() for ch in text)
