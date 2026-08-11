"""Audit PDF rendering for the Migration Certificate.

The PDF is a *rendering* of the signed certificate markdown, never a second
derivation of the facts: an auditor who re-renders the same signed body gets
byte-identical output, and every page footer carries the content hash the
``/certificate/verify`` endpoint checks. Anything the markdown declines to
claim (unmeasured counts, unavailable burn-down) stays declined here.
"""

from __future__ import annotations

import re
from io import BytesIO
from typing import Any

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from services.migration_certificate import render_certificate_markdown

__all__ = ["render_certificate_pdf"]

_INK = colors.HexColor("#111827")
_MUTED = colors.HexColor("#6B7280")
_RULE = colors.HexColor("#E5E7EB")
_PROVEN = colors.HexColor("#047857")
_OPEN = colors.HexColor("#B91C1C")

_BOLD = re.compile(r"\*\*(.+?)\*\*")
_CODE = re.compile(r"`([^`]*)`")
_ALIGN_ROW = re.compile(r"^\|[\s:|-]+\|$")


def _styles() -> dict[str, ParagraphStyle]:
    base = getSampleStyleSheet()["BodyText"]
    body = ParagraphStyle(
        "df_body",
        parent=base,
        fontName="Helvetica",
        fontSize=9,
        leading=13,
        textColor=_INK,
        alignment=TA_LEFT,
    )
    return {
        "body": body,
        "cell": ParagraphStyle("df_cell", parent=body, fontSize=8, leading=11),
        "h1": ParagraphStyle(
            "df_h1", parent=body, fontName="Helvetica-Bold", fontSize=17, leading=21
        ),
        "h2": ParagraphStyle(
            "df_h2",
            parent=body,
            fontName="Helvetica-Bold",
            fontSize=11,
            leading=15,
            spaceBefore=8,
            textColor=_MUTED,
        ),
        "verdict": ParagraphStyle(
            "df_verdict", parent=body, fontName="Helvetica-Bold", fontSize=13, leading=17
        ),
    }


def _inline(text: str) -> str:
    """Markdown emphasis to the minimal RML the PDF engine understands."""
    escaped = (
        str(text or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    escaped = _BOLD.sub(r"<b>\1</b>", escaped)
    return _CODE.sub(r'<font face="Courier">\1</font>', escaped)


def _cells(line: str) -> list[str]:
    return [c.strip() for c in line.strip().strip("|").split("|")]


def _table(rows: list[list[str]], styles: dict[str, ParagraphStyle], width: float) -> Table:
    data = [[Paragraph(_inline(c), styles["cell"]) for c in row] for row in rows]
    columns = max(len(r) for r in data)
    for row in data:
        row.extend([Paragraph("", styles["cell"])] * (columns - len(row)))
    table = Table(data, colWidths=[width / columns] * columns, repeatRows=1, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.4, _RULE),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F9FAFB")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    return table


def _flowables(
    markdown: str, styles: dict[str, ParagraphStyle], width: float
) -> list[Any]:
    story: list[Any] = []
    pending: list[list[str]] = []

    def flush() -> None:
        if pending:
            story.append(_table(list(pending), styles, width))
            story.append(Spacer(1, 5))
            pending.clear()

    for raw in markdown.splitlines():
        line = raw.rstrip()
        if line.startswith("|"):
            if not _ALIGN_ROW.match(line):
                pending.append(_cells(line))
            continue
        flush()
        if not line:
            story.append(Spacer(1, 4))
        elif line.startswith("# "):
            story.append(Paragraph(_inline(line[2:]), styles["h1"]))
            story.append(Spacer(1, 4))
        elif line.startswith("## "):
            story.append(Paragraph(_inline(line[3:]), styles["h2"]))
        elif line.startswith("- "):
            story.append(
                Paragraph(_inline(line[2:]), styles["body"], bulletText="\u2022")
            )
        elif line.startswith("**"):
            story.append(Paragraph(_inline(line), styles["verdict"]))
        else:
            story.append(Paragraph(_inline(line), styles["body"]))
    flush()
    return story


def render_certificate_pdf(cert: dict[str, Any]) -> bytes:
    """Render the signed certificate as a paginated, hash-stamped audit PDF."""
    if not isinstance(cert, dict) or not cert:
        raise ValueError("a built migration certificate is required")

    verdict = cert.get("verdict") if isinstance(cert.get("verdict"), dict) else {}
    headline = str(verdict.get("headline") or "UNKNOWN")
    digest = str(cert.get("content_sha256") or "")
    job = cert.get("job") if isinstance(cert.get("job"), dict) else {}
    job_id = str(job.get("job_id") or "")

    styles = _styles()
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=16 * mm,
        bottomMargin=18 * mm,
        title=f"Migration Certificate {job_id}".strip(),
        author="DataFlow",
        subject=headline,
        # Byte-identical output for the same signed body: an auditor must be
        # able to re-render and diff, which a wall-clock timestamp would break.
        invariant=1,
    )
    width = doc.width

    def decorate(canvas: Any, _doc: Any) -> None:
        canvas.saveState()
        canvas.setStrokeColor(_RULE)
        canvas.line(doc.leftMargin, 14 * mm, doc.leftMargin + width, 14 * mm)
        canvas.setFont("Helvetica", 6.5)
        canvas.setFillColor(_MUTED)
        canvas.drawString(doc.leftMargin, 10 * mm, f"SHA-256 {digest}")
        label = f"Page {canvas.getPageNumber()}  ·  {headline}"
        canvas.setFillColor(_PROVEN if headline == "MIGRATION PROVEN" else _OPEN)
        canvas.drawRightString(doc.leftMargin + width, 10 * mm, label)
        canvas.restoreState()

    story = _flowables(render_certificate_markdown(cert), styles, width)
    doc.build(story, onFirstPage=decorate, onLaterPages=decorate)
    return buffer.getvalue()
