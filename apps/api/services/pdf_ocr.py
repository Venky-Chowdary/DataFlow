"""PDF OCR — render scanned pages and extract text via Tesseract.

Honesty
-------
Opt-in only. Requires:
  - Python: ``pypdfium2``, ``Pillow``, ``pytesseract``
  - System: ``tesseract`` binary on PATH

When OCR is requested but unavailable, DataFlow fail-closes with an actionable
error — never invents text. Delivery of OCR chunks is at-least-once when paired
with vector destinations (same as text-layer document chunking).
"""

from __future__ import annotations

import importlib.util
import logging
import shutil
from typing import Any

logger = logging.getLogger(__name__)


def ocr_dependency_status() -> dict[str, Any]:
    """Probe Python packages + Tesseract binary (no PDF I/O)."""
    missing: list[str] = []
    present: list[str] = []
    for mod in ("pypdfium2", "PIL", "pytesseract"):
        name = "Pillow" if mod == "PIL" else mod
        if importlib.util.find_spec(mod) is None:
            missing.append(name)
        else:
            present.append(name)

    tesseract = shutil.which("tesseract")
    if not tesseract:
        missing.append("tesseract (system binary)")
    else:
        present.append(f"tesseract@{tesseract}")

    available = not missing
    if available:
        message = "OCR ready (pypdfium2 + Pillow + pytesseract + tesseract)"
    else:
        message = (
            "OCR unavailable — install: pip install pypdfium2 Pillow pytesseract "
            "and the system tesseract binary. Missing: " + ", ".join(missing)
        )
    return {
        "available": available,
        "message": message,
        "present": present,
        "missing": missing,
        "tesseract_path": tesseract or "",
    }


def ocr_available() -> tuple[bool, str]:
    status = ocr_dependency_status()
    return bool(status["available"]), str(status["message"])


def render_pdf_page_images(content: bytes, *, dpi: int = 200) -> list[tuple[int, Any]]:
    """Rasterize each PDF page to a PIL Image. Raises RuntimeError if deps missing."""
    try:
        import pypdfium2 as pdfium
        from PIL import Image
    except ImportError as exc:
        raise RuntimeError(
            "OCR render requires pypdfium2 and Pillow — "
            "pip install pypdfium2 Pillow"
        ) from exc

    if not content:
        raise RuntimeError("Empty PDF content")

    scale = max(72, int(dpi)) / 72.0
    try:
        doc = pdfium.PdfDocument(content)
    except Exception as exc:
        raise RuntimeError(f"Failed to open PDF for OCR render: {exc}") from exc

    pages: list[tuple[int, Any]] = []
    try:
        for idx in range(len(doc)):
            page = doc[idx]
            try:
                bitmap = page.render(scale=scale)
                pil = bitmap.to_pil()
                if not isinstance(pil, Image.Image):
                    pil = Image.fromarray(pil)
                if pil.mode not in ("RGB", "L"):
                    pil = pil.convert("RGB")
                pages.append((idx + 1, pil))
            finally:
                page.close()
    finally:
        doc.close()
    return pages


def ocr_image(image: Any, *, language: str = "eng") -> str:
    """Run Tesseract on a single PIL image."""
    try:
        import pytesseract
    except ImportError as exc:
        raise RuntimeError(
            "pytesseract is required for OCR — pip install pytesseract"
        ) from exc

    if shutil.which("tesseract") is None:
        raise RuntimeError(
            "tesseract binary not found on PATH — install Tesseract OCR "
            "(e.g. brew install tesseract / apt install tesseract-ocr)"
        )

    try:
        text = pytesseract.image_to_string(image, lang=language or "eng")
    except Exception as exc:
        raise RuntimeError(f"Tesseract OCR failed: {exc}") from exc
    return (text or "").strip()


def ocr_pdf_pages(
    content: bytes,
    *,
    dpi: int = 200,
    language: str = "eng",
) -> list[tuple[int, str]]:
    """OCR every page → ``[(page_number, text), ...]`` (empty strings omitted)."""
    ok, msg = ocr_available()
    if not ok:
        raise RuntimeError(msg)

    results: list[tuple[int, str]] = []
    for page_num, image in render_pdf_page_images(content, dpi=dpi):
        text = ocr_image(image, language=language)
        if text:
            results.append((page_num, text))
        else:
            logger.info("OCR produced no text on page %s", page_num)
    return results
