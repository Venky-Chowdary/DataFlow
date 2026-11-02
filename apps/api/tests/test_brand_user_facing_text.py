"""Datawrap brand: no user-facing text says the old product name.

The owner's report: "the old 'dataflow' name leaks into customer ... screens".
The product is Datawrap. This scans every source of text a person reads:

* API messages (every non-docstring string literal under ``src/``,
  ``services/`` and ``connectors/`` — errors, refusals, certificate text);
* downloadable file names (``Content-Disposition``) and e-mail sender defaults;
* the MCP server identity and every MCP tool description;
* the web app's text (``apps/web/src``) and the in-app help corpus.

Some old-brand tokens are NOT presentation and stay, each listed below with
the reason. Names of objects the product writes into customer databases are
governed by ``services.brand`` (see ``test_brand_object_names_e2e.py``), and
``DATAFLOW_*`` operator variables keep working through ``getenv_brand``.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

API = Path(__file__).resolve().parents[1]
WEB_SRC = API.parent / "web" / "src"

#: The product name as prose: "DataFlow", "Dataflow" (any non-identifier use).
BRAND_PROSE = re.compile(r"Data[Ff]low")

#: Allowed old-brand tokens, each with why it is not presentation.
ALLOWED = [
    # Operator environment variables: read DATAWRAP_* first, DATAFLOW_* still
    # works (services/brand_env.getenv_brand). Renaming them breaks deploys.
    re.compile(r"\bDATAFLOW_[A-Z0-9_]+"),
    # Another vendor's product, named when a user asks about it.
    re.compile(r"(?i)(google\s+)?cloud\s+dataflow|google\s+cloud\s+dataflow|(Pub/Sub|Datawrap) is not (Cloud )?Dataflow"),
    # A SQLAlchemy dialect class registered under this name (code identifier).
    re.compile(r"DataFlowBigQueryDialect"),
    # Internal module path cited as a source (apps/cli/dataflow_cli).
    re.compile(r"dataflow_cli"),
    # Weaviate class this product creates in the customer's Weaviate: an
    # object name, governed by services.brand like every other artifact.
    re.compile(r"\bDataflowChunk\b"),
]


def _strip_allowed(text: str) -> str:
    for pat in ALLOWED:
        text = pat.sub("", text)
    return text


def _string_literals(path: Path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = getattr(node, "body", None) or []
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                docstrings.add(id(body[0].value))
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if id(node) not in docstrings:
                yield node.lineno, node.value


#: The brand module defines the old names (to recognise old artifacts); its
#: strings are the inventory, not text shown to anyone.
BRAND_MODULES = {"services/brand.py", "services/brand_env.py", "services/brand_artifacts.py"}


def _api_sources():
    for top in ("src", "services", "connectors"):
        for path in sorted((API / top).rglob("*.py")):
            if "__pycache__" in path.parts or str(path.relative_to(API)) in BRAND_MODULES:
                continue
            yield path


def test_api_messages_name_the_product_datawrap():
    hits = []
    for path in _api_sources():
        for line, value in _string_literals(path):
            if BRAND_PROSE.search(_strip_allowed(value)):
                hits.append(f"{path.relative_to(API)}:{line}: {value[:120]!r}")
    assert not hits, "old product name in user-facing API text:\n" + "\n".join(hits)


def test_downloads_and_mail_senders_use_the_new_name():
    """File names a person saves and the From address they receive."""
    download = re.compile(r"(?i)(filename=|attachment;)")
    sender = re.compile(r"(?i)(?<![:/\w])dataflow@")
    hits = []
    for path in _api_sources():
        for line, value in _string_literals(path):
            text = _strip_allowed(value)
            if (download.search(text) and re.search(r"(?i)dataflow", text)) or sender.search(text):
                hits.append(f"{path.relative_to(API)}:{line}: {value[:120]!r}")
    assert not hits, "\n".join(hits)


def test_mcp_server_identity_and_tool_descriptions_say_datawrap():
    from services.mcp_protocol import SERVER_INFO
    from src.ai.copilot.tools import TOOL_DEFINITIONS

    assert SERVER_INFO["name"] == "datawrap"
    assert "Datawrap" in SERVER_INFO["title"]
    text = _strip_allowed(json.dumps(TOOL_DEFINITIONS, ensure_ascii=False))
    leaks = sorted({m.group(0) for m in re.finditer(r"(?i).{0,40}dataflow.{0,40}", text)})
    assert not leaks, leaks


def test_mcp_manifest_names_datawrap():
    import asyncio

    from src.routers.mcp_router import mcp_manifest

    class _Req:
        base_url = "http://localhost/"
        headers: dict = {}

    try:
        manifest = asyncio.run(mcp_manifest(_Req()))
    except Exception:  # pragma: no cover - manifest needs app state
        src = (API / "src" / "routers" / "mcp_router.py").read_text(encoding="utf-8")
        assert '"name": "dataflow"' not in src
        return
    assert manifest["name"] == "datawrap"


# ---------------------------------------------------------------------------
# Web app text
# ---------------------------------------------------------------------------

#: Web tokens that are code, not text a person reads.
WEB_ALLOWED = [
    re.compile(r"@dataflow/design-system"),  # npm workspace package name
    re.compile(r"\bdataflow(Rps|_ratio)\b|\bc\.dataflow_ratio\b"),  # data keys
    # QA r12 #4b: the comparison/benchmark keys, the JSON-LD id, the export
    # path and the legacy window variable no longer ship; they are not allowed.
    # BigQuery default dataset: a persistent object in the customer's project.
    # The API still writes there when no dataset is given (services.brand).
    re.compile(r"bigquery:\s*\"dataflow\""),
    re.compile(r"placeholder:\s*\"dataflow\"\s*\}"),
    re.compile(r"placeholder=\"dataflow\""),
    # Default destination database the transfer form pre-fills (an object in
    # the customer's system; see services.brand for the owner's call).
    re.compile(r"\"dataflow_test\""),
]


def _web_sources():
    for path in sorted(WEB_SRC.rglob("*")):
        if path.suffix not in {".ts", ".tsx"} or ".test." in path.name:
            continue
        yield path


def _web_text_lines(path: Path):
    in_block = False
    for n, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if in_block:
            if "*/" in line:
                in_block = False
            continue
        if line.startswith("/*") and "*/" not in line:
            in_block = True
            continue
        if line.startswith(("//", "*", "/*")):
            continue
        yield n, raw.split(" // ")[0]


def test_web_ui_text_says_datawrap():
    hits = []
    for path in _web_sources():
        for n, line in _web_text_lines(path):
            text = _strip_allowed(line)
            for pat in WEB_ALLOWED:
                text = pat.sub("", text)
            if re.search(r"(?i)dataflow", text):
                hits.append(f"{path.relative_to(WEB_SRC)}:{n}: {line.strip()[:140]}")
    assert not hits, "old product name in web text:\n" + "\n".join(hits)


def test_in_app_help_corpus_says_datawrap():
    corpus = (API / "src" / "ai" / "rag" / "help_corpus.json").read_text(encoding="utf-8")
    leaks = sorted({m.group(0) for m in re.finditer(r"(?i).{0,40}dataflow.{0,40}", _strip_allowed(corpus))})
    assert not leaks, leaks
