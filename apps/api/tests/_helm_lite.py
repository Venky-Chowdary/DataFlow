"""Render this repository's Helm chart without the ``helm`` binary.

A small evaluator for the part of Go templates / Sprig the chart uses:
``define``/``include``, ``if``/``else if``/``else``, ``range``/``else``,
``with``, field paths, pipelines, whitespace trimming, and the functions in
``_FUNCS``. An action it does not understand raises ``HelmLiteError`` — the
chart test then fails and says so, rather than rendering something wrong.
It exists so the chart's invariants are checked on a runner with no ``helm``;
where ``helm`` is installed the same checks also run on its real output.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml


class HelmLiteError(Exception):
    pass


class ChartFail(HelmLiteError):
    """The chart itself refused the values (``fail`` / ``required``)."""


_ACTION = re.compile(r"\{\{(-?)\s*(.*?)\s*(-?)\}\}", re.S)
_TOKEN = re.compile(r'\s*("(?:[^"\\]|\\.)*"|\(|\)|\||[^\s()|]+)')


def _truthy(value: Any) -> bool:
    return bool(value) and value != 0


def _to_yaml(value: Any) -> str:
    if value is None or value == {} or value == []:
        return "{}" if value == {} else ("[]" if value == [] else "null")
    return yaml.safe_dump(value, default_flow_style=False, sort_keys=False).rstrip("\n")


def _indent(n: int, text: str) -> str:
    pad = " " * int(n)
    return "\n".join(pad + line if line else line for line in str(text).split("\n"))


def _scalar(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _required(message: str, value: Any) -> Any:
    if value is None or value == "":
        raise ChartFail(message)
    return value


def _fail(message: str) -> Any:
    raise ChartFail(message)


_FUNCS: dict[str, Any] = {
    "quote": lambda v: '"' + _scalar(v).replace("\\", "\\\\").replace('"', '\\"') + '"',
    "default": lambda d, v=None: v if _truthy(v) else d,
    "toYaml": _to_yaml,
    "nindent": lambda n, text: "\n" + _indent(n, text),
    "indent": _indent,
    "int": lambda v: int(v or 0),
    "add": lambda *a: sum(int(x) for x in a),
    "lt": lambda a, b: a < b,
    "gt": lambda a, b: a > b,
    "eq": lambda a, *rest: any(a == b for b in rest),
    "ne": lambda a, b: a != b,
    "not": lambda v: not _truthy(v),
    "and": lambda *a: next((x for x in a if not _truthy(x)), a[-1]),
    "or": lambda *a: next((x for x in a if _truthy(x)), a[-1]),
    "required": _required,
    "fail": _fail,
}


class _Renderer:
    def __init__(self, root: dict[str, Any], defines: dict[str, list]) -> None:
        self.root = root
        self.defines = defines

    # -- expressions ---------------------------------------------------
    def _field(self, path: str, dot: Any) -> Any:
        value = self.root if path.startswith("$") else dot
        for part in [p for p in path.lstrip("$").split(".") if p]:
            value = value.get(part) if isinstance(value, dict) else None
        return value

    def _atom(self, tokens: list[str], dot: Any) -> Any:
        tok = tokens.pop(0)
        if tok == "(":
            value = self._pipeline(tokens, dot, closing=True)
            return value
        if tok.startswith('"'):
            return bytes(tok[1:-1], "utf-8").decode("unicode_escape")
        if re.fullmatch(r"-?\d+", tok):
            return int(tok)
        if tok in ("true", "false"):
            return tok == "true"
        if tok == "." or tok.startswith(".") or tok.startswith("$"):
            return self._field(tok, dot)
        raise HelmLiteError(f"unsupported token {tok!r}")

    def _command(self, tokens: list[str], dot: Any, piped: list[Any]) -> Any:
        head = tokens[0]
        if head == "include":
            tokens.pop(0)
            name = self._atom(tokens, dot)
            scope = self._atom(tokens, dot)
            return self.render_nodes(self.defines[name], scope)
        if head in _FUNCS:
            tokens.pop(0)
            args: list[Any] = []
            while tokens and tokens[0] not in ("|", ")"):
                args.append(self._atom(tokens, dot))
            return _FUNCS[head](*args, *piped)
        if re.fullmatch(r"[A-Za-z_]\w*", head):
            raise HelmLiteError(f"unsupported function {head!r}")
        value = self._atom(tokens, dot)
        if tokens and tokens[0] not in ("|", ")"):
            raise HelmLiteError(f"unexpected {tokens[0]!r} after a value")
        return value

    def _pipeline(self, tokens: list[str], dot: Any, closing: bool = False) -> Any:
        value = self._command(tokens, dot, [])
        while tokens and tokens[0] == "|":
            tokens.pop(0)
            value = self._command(tokens, dot, [value])
        if closing:
            if not tokens or tokens.pop(0) != ")":
                raise HelmLiteError("missing )")
        return value

    def evaluate(self, expr: str, dot: Any) -> Any:
        tokens = _TOKEN.findall(expr)
        value = self._pipeline(tokens, dot)
        if tokens:
            raise HelmLiteError(f"trailing tokens in {expr!r}: {tokens}")
        return value

    # -- nodes ---------------------------------------------------------
    def render_nodes(self, nodes: list, dot: Any) -> str:
        out: list[str] = []
        for node in nodes:
            kind = node[0]
            if kind == "text":
                out.append(node[1])
            elif kind == "expr":
                out.append(_scalar(self.evaluate(node[1], dot)))
            elif kind == "if":
                for cond, body in node[1]:
                    if cond is None or _truthy(self.evaluate(cond, dot)):
                        out.append(self.render_nodes(body, dot))
                        break
            elif kind == "with":
                value = self.evaluate(node[1], dot)
                if _truthy(value):
                    out.append(self.render_nodes(node[2], value))
                else:
                    out.append(self.render_nodes(node[3], dot))
            elif kind == "range":
                items = self.evaluate(node[1], dot) or []
                if items:
                    for item in items:
                        out.append(self.render_nodes(node[2], item))
                else:
                    out.append(self.render_nodes(node[3], dot))
        return "".join(out)


def _lex(source: str) -> list[tuple[str, str]]:
    """Text and action tokens, with ``{{-`` / ``-}}`` whitespace trimming applied."""
    parts: list[tuple[str, str]] = []
    pos = 0
    trim_next = False
    for match in _ACTION.finditer(source):
        text = source[pos : match.start()]
        if trim_next:
            text = text.lstrip()
        if match.group(1):
            text = text.rstrip()
        if text:
            parts.append(("text", text))
        body = match.group(2)
        if not body.startswith("/*"):
            parts.append(("action", body))
        trim_next = bool(match.group(3))
        pos = match.end()
    tail = source[pos:]
    if trim_next:
        tail = tail.lstrip()
    if tail:
        parts.append(("text", tail))
    return parts


def _parse(tokens: list[tuple[str, str]], defines: dict[str, list], stop: tuple[str, ...] = ()) -> tuple[list, str]:
    nodes: list = []
    while tokens:
        kind, body = tokens.pop(0)
        if kind == "text":
            nodes.append(("text", body))
            continue
        word = body.split(None, 1)[0]
        rest = body[len(word) :].strip()
        if word in stop or (word == "else" and "else" in stop):
            return nodes, body
        if word == "define":
            name = rest.strip().strip('"')
            block, _ = _parse(tokens, defines, ("end",))
            defines[name] = block
        elif word == "if":
            branches: list[tuple[str | None, list]] = []
            cond: str | None = rest
            while True:
                block, ended = _parse(tokens, defines, ("end", "else"))
                branches.append((cond, block))
                if ended == "end":
                    break
                after = ended[len("else") :].strip()
                if after.startswith("if "):
                    cond = after[3:].strip()
                else:
                    cond = None
            nodes.append(("if", branches))
        elif word in ("with", "range"):
            block, ended = _parse(tokens, defines, ("end", "else"))
            other: list = []
            if ended != "end":
                other, _ = _parse(tokens, defines, ("end",))
            nodes.append((word, rest, block, other))
        elif word in ("end", "else"):
            raise HelmLiteError(f"unexpected {{{{ {body} }}}}")
        else:
            nodes.append(("expr", body))
    return nodes, ""


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def render_chart(
    chart_dir: Path, *, overrides: dict[str, Any] | None = None, release: str = "dataflow"
) -> list[dict[str, Any]]:
    """Every Kubernetes object the chart renders for these values."""
    chart = yaml.safe_load((chart_dir / "Chart.yaml").read_text(encoding="utf-8"))
    values = yaml.safe_load((chart_dir / "values.yaml").read_text(encoding="utf-8"))
    values = _deep_merge(values, overrides or {})
    root = {
        "Values": values,
        "Release": {"Name": release, "Service": "Helm", "Namespace": "default"},
        "Chart": {"Name": chart["name"], "Version": chart["version"], "AppVersion": chart.get("appVersion")},
    }
    defines: dict[str, list] = {}
    parsed: dict[str, list] = {}
    templates = sorted((chart_dir / "templates").iterdir())
    for path in templates:
        nodes, _ = _parse(_lex(path.read_text(encoding="utf-8")), defines)
        if path.suffix in (".yaml", ".yml"):
            parsed[path.name] = nodes
    renderer = _Renderer(root, defines)
    objects: list[dict[str, Any]] = []
    for name, nodes in parsed.items():
        text = renderer.render_nodes(nodes, root)
        try:
            for doc in yaml.safe_load_all(text):
                if isinstance(doc, dict):
                    doc["__template__"] = name
                    objects.append(doc)
        except yaml.YAMLError as exc:
            raise HelmLiteError(f"{name} does not render to valid YAML: {exc}\n{text}") from exc
    return objects
