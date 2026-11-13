#!/usr/bin/env python3
"""Generate the environment-flag table in ``docs/operate/configuration.md``.

The table is produced from the code so it cannot drift: every flag the runtime
Python reads is found by walking the syntax tree, never by importing the app.

Usage (from the repository root)::

    python docs/tools/gen_config_reference.py            # rewrite the table
    python docs/tools/gen_config_reference.py --check    # exit 1 when stale

What counts as "a flag the code reads":

* ``os.getenv(NAME)``, ``os.environ.get(NAME)``, ``os.environ[NAME]`` and
  ``NAME in os.environ``;
* the brand helpers in ``services/brand_env.py`` (``getenv_brand*``), which try
  ``DATAWRAP_<suffix>`` first and ``DATAFLOW_<suffix>`` second;
* any local helper that passes one of its own parameters on to one of the
  above (``_env_int("X", 5)`` style wrappers), followed transitively;
* names reached through a module constant or a literal tuple that is looped
  over (``for key in ("DATAWRAP_ENV", "DATAFLOW_ENV"): os.getenv(key)``).

Reads whose name is built at run time (f-strings, dictionary look-ups) cannot
be listed by name; they are reported under "Reads the generator cannot name".

Descriptions come from ``docs/tools/config_descriptions.json`` when a flag has
a curated entry. Otherwise the cell shows which function reads the flag and the
first line of that function's docstring, so nothing in the table is invented.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET = REPO_ROOT / "docs" / "operate" / "configuration.md"
DESCRIPTIONS = Path(__file__).resolve().parent / "config_descriptions.json"

BEGIN = "<!-- BEGIN GENERATED: config-flags (docs/tools/gen_config_reference.py) -->"
END = "<!-- END GENERATED: config-flags -->"

# Runtime Python only. Tests, benchmarks and one-off scripts are out of scope:
# an operator never sets their flags on a running deployment.
SCAN_ROOTS = (
    "apps/api/src",
    "apps/api/services",
    "apps/api/connectors",
    "apps/api/main.py",
    "apps/api/registry.py",
    "apps/cli/dataflow_cli",
    "packages/preflight/src",
    "packages/ml/src",
)
SKIP_PARTS = {"tests", "__pycache__", "node_modules", "benchmarks"}

BRAND_PREFIXES = ("DATAWRAP_", "DATAFLOW_")
BRAND_READERS = {"getenv_brand", "getenv_brand_str", "getenv_brand_bool"}
OS_READERS = {"os.getenv", "os.environ.get", "environ.get", "getenv"}
ENV_NAME = re.compile(r"^[A-Z][A-Z0-9_]{1,}$")
# Callables that take a product-prefixed literal for a reason other than reading it.
NOT_READERS = {"startswith", "endswith", "len", "setdefault", "pop", "removeprefix", "delenv", "setenv", "append"}


@dataclass
class Flag:
    """One environment flag, keyed by its suffix (or full name when unbranded)."""

    key: str
    brand_read: bool = False  # read through getenv_brand: both prefixes work
    literal_names: set[str] = field(default_factory=set)
    defaults: set[str] = field(default_factory=set)
    files: set[str] = field(default_factory=set)
    contexts: list[tuple[str, str, str]] = field(default_factory=list)

    def accepted_names(self) -> list[str]:
        if self.brand_read:
            extra = sorted(n for n in self.literal_names if not n.startswith(BRAND_PREFIXES))
            return [f"DATAWRAP_{self.key}", f"DATAFLOW_{self.key}", *extra]
        return sorted(self.literal_names)

    def exact_only_names(self) -> list[str]:
        """Prefixed names some reader uses directly, bypassing the brand helper."""
        if not self.brand_read:
            return []
        return sorted(n for n in self.literal_names if n.startswith(BRAND_PREFIXES))


def _dotted(node: ast.AST) -> str:
    if isinstance(node, ast.Attribute):
        return f"{_dotted(node.value)}.{node.attr}"
    if isinstance(node, ast.Name):
        return node.id
    return "?"


def _iter_files() -> list[Path]:
    out: list[Path] = []
    for rel in SCAN_ROOTS:
        base = REPO_ROOT / rel
        if base.is_file():
            out.append(base)
            continue
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("*.py")):
            if SKIP_PARTS.intersection(path.relative_to(REPO_ROOT).parts):
                continue
            out.append(path)
    return out


def _const_str(node: ast.AST | None) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _str_elements(node: ast.AST | None) -> list[str] | None:
    """Literal strings of a tuple/list/set, or None when any element is not one."""
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        vals = [_const_str(e) for e in node.elts]
        if vals and all(v is not None for v in vals):
            return [v for v in vals if v is not None]
    return None


class _Module:
    """Per-file facts needed to resolve a flag-name expression."""

    def __init__(self, path: Path, tree: ast.Module) -> None:
        self.path = path
        self.rel = path.relative_to(REPO_ROOT).as_posix()
        self.tree = tree
        self.str_consts: dict[str, str] = {}
        self.num_consts: dict[str, str] = {}
        self.seq_consts: dict[str, list[str]] = {}
        self.functions: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
        self.parents: dict[ast.AST, ast.AST] = {}
        for parent in ast.walk(tree):
            for child in ast.iter_child_nodes(parent):
                self.parents[child] = parent
            if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef)):
                self.functions.append(parent)
        for stmt in tree.body:
            targets: list[ast.expr] = []
            value: ast.AST | None = None
            if isinstance(stmt, ast.Assign):
                targets, value = stmt.targets, stmt.value
            elif isinstance(stmt, ast.AnnAssign) and stmt.value is not None:
                targets, value = [stmt.target], stmt.value
            for tgt in targets:
                if not isinstance(tgt, ast.Name):
                    continue
                s = _const_str(value)
                if s is not None:
                    self.str_consts[tgt.id] = s
                if (
                    isinstance(value, ast.Constant)
                    and isinstance(value.value, (int, float))
                    and not isinstance(value.value, bool)
                ):
                    self.num_consts[tgt.id] = str(value.value)
                seq = _str_elements(value)
                if seq is not None:
                    self.seq_consts[tgt.id] = seq

    def enclosing_function(self, node: ast.AST) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
        cur = self.parents.get(node)
        while cur is not None:
            if isinstance(cur, (ast.FunctionDef, ast.AsyncFunctionDef)):
                return cur
            cur = self.parents.get(cur)
        return None

    def loop_values(self, node: ast.AST, name: str) -> list[str] | None:
        """Literal values a loop variable ``name`` takes around ``node``."""
        cur = self.parents.get(node)
        while cur is not None:
            iters: list[tuple[ast.AST, ast.AST]] = []
            if isinstance(cur, (ast.For, ast.AsyncFor)):
                iters.append((cur.target, cur.iter))
            if isinstance(cur, (ast.ListComp, ast.SetComp, ast.GeneratorExp, ast.DictComp)):
                iters.extend((g.target, g.iter) for g in cur.generators)
            for target, it in iters:
                if isinstance(target, ast.Name) and target.id == name:
                    seq = _str_elements(it)
                    if seq is None and isinstance(it, ast.Name):
                        seq = self.seq_consts.get(it.id)
                    if seq is not None:
                        return seq
            cur = self.parents.get(cur)
        return None


def _resolve_names(mod: _Module, call: ast.AST, arg: ast.AST) -> list[str] | None:
    """Flag names an argument expression can evaluate to, or None if dynamic."""
    s = _const_str(arg)
    if s is not None:
        return [s]
    if isinstance(arg, ast.Name):
        if arg.id in mod.str_consts:
            return [mod.str_consts[arg.id]]
        return mod.loop_values(call, arg.id)
    return None


def _default_text(call: ast.Call, default_index: int, mod: _Module | None = None) -> str:
    node: ast.AST | None = None
    if len(call.args) > default_index:
        node = call.args[default_index]
    for kw in call.keywords:
        if kw.arg == "default":
            node = kw.value
    if node is None:
        return "unset"
    if isinstance(node, ast.Constant):
        if node.value is None:
            return "unset"
        if node.value == "":
            return '""'
        return str(node.value)
    if isinstance(node, ast.Name) and mod is not None:
        if node.id in mod.num_consts:
            return mod.num_consts[node.id]
        if node.id in mod.str_consts:
            return mod.str_consts[node.id] or '""'
    if isinstance(node, ast.Call) and _dotted(node.func) == "str" and len(node.args) == 1:
        inner = node.args[0]
        if isinstance(inner, ast.Constant):
            return str(inner.value)
        if isinstance(inner, ast.Name) and mod is not None and inner.id in mod.num_consts:
            return mod.num_consts[inner.id]
    return "computed"


def _wrapper_param_index(
    fn: ast.FunctionDef | ast.AsyncFunctionDef, readers: dict[str, tuple[bool, int]]
) -> tuple[bool, int] | None:
    """If ``fn`` forwards one of its parameters as a flag name, say which one.

    Returns ``(brand_read, parameter_index)``.
    """
    params = [a.arg for a in fn.args.posonlyargs + fn.args.args]
    if not params:
        return None
    for node in ast.walk(fn):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        name = _dotted(node.func)
        short = name.rsplit(".", 1)[-1]
        info = readers.get(name) or readers.get(short)
        if info is None:
            continue
        brand, idx = info
        if len(node.args) <= idx:
            continue
        arg = node.args[idx]
        if isinstance(arg, ast.Name) and arg.id in params:
            return brand, params.index(arg.id)
    return None


def _imported_readers(mod: _Module, known: dict[str, tuple[bool, int]]) -> dict[str, tuple[bool, int]]:
    """Readers a module imports by name (``from x import getenv_brand as _g``)."""
    out: dict[str, tuple[bool, int]] = {}
    for node in ast.walk(mod.tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        for alias in node.names:
            if alias.name in known and alias.name not in {"get", "getenv"}:
                out[alias.asname or alias.name] = known[alias.name]
    return out


def _record_settings_class(mod: _Module, cls: ast.ClassDef, record) -> None:  # type: ignore[no-untyped-def]
    """pydantic ``BaseSettings`` fields: ``Field(default=..., validation_alias="NAME")``."""
    for node in ast.walk(cls):
        if not isinstance(node, ast.Call) or _dotted(node.func).rsplit(".", 1)[-1] != "Field":
            continue
        alias = default = None
        for kw in node.keywords:
            if kw.arg in {"validation_alias", "alias", "env"}:
                alias = _const_str(kw.value)
            if kw.arg == "default" and isinstance(kw.value, ast.Constant):
                default = kw.value.value
        if alias:
            shown = "unset" if default is None else ('""' if default == "" else str(default))
            record(mod, node, alias, False, shown)


def collect() -> tuple[dict[str, Flag], list[str]]:
    """Return ``(flags_by_key, dynamic_read_sites)``."""
    modules: list[_Module] = []
    for path in _iter_files():
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        modules.append(_Module(path, tree))

    # name -> (brand_read, index of the flag-name argument)
    base: dict[str, tuple[bool, int]] = {name: (True, 0) for name in BRAND_READERS}
    base.update({name: (False, 0) for name in OS_READERS})

    flags: dict[str, Flag] = {}
    dynamic: set[str] = set()

    def record(mod: _Module, node: ast.AST, raw: str, brand: bool, default: str) -> None:
        if not ENV_NAME.match(raw):
            return
        key = raw
        for prefix in BRAND_PREFIXES:
            if raw.startswith(prefix) and len(raw) > len(prefix):
                key = raw[len(prefix) :]
        flag = flags.setdefault(key, Flag(key=key))
        if brand:
            flag.brand_read = True
        else:
            flag.literal_names.add(raw)
        flag.defaults.add(default)
        flag.files.add(mod.rel)
        fn = mod.enclosing_function(node)
        doc = ""
        fn_name = "module level"
        if fn is not None:
            fn_name = f"{fn.name}()"
            doc = (ast.get_docstring(fn) or "").strip().split("\n", 1)[0].strip()
        if not doc:
            doc = (ast.get_docstring(mod.tree) or "").strip().split("\n", 1)[0].strip()
        flag.contexts.append((mod.rel, fn_name, doc))

    # Pass 1: wrappers defined in each module, found to a fixed point so that
    # wrappers of wrappers count. Pass 2 lets a module use a wrapper it imports.
    local_readers: dict[str, dict[str, tuple[bool, int]]] = {}
    exported: dict[str, tuple[bool, int]] = {}
    for mod in modules:
        readers = dict(base)
        readers.update(_imported_readers(mod, base))
        changed = True
        while changed:
            changed = False
            for fn in mod.functions:
                if fn.name in readers or fn.name in BRAND_READERS:
                    continue
                info = _wrapper_param_index(fn, readers)
                if info is not None:
                    readers[fn.name] = info
                    exported.setdefault(fn.name, info)
                    changed = True
        local_readers[mod.rel] = readers

    for mod in modules:
        readers = dict(local_readers[mod.rel])
        for alias, info in _imported_readers(mod, {**base, **exported}).items():
            readers.setdefault(alias, info)
        wrapper_names = {name for name in readers if name not in base}

        for cls in ast.walk(mod.tree):
            if isinstance(cls, ast.ClassDef) and any("BaseSettings" in _dotted(b) for b in cls.bases):
                _record_settings_class(mod, cls, record)

        for node in ast.walk(mod.tree):
            # os.environ["X"] / "X" in os.environ
            if isinstance(node, ast.Subscript) and _dotted(node.value) in {"os.environ", "environ"}:
                if isinstance(node.ctx, ast.Load):
                    name = _const_str(node.slice)
                    if name:
                        record(mod, node, name, False, "required")
                continue
            if isinstance(node, ast.Compare) and len(node.ops) == 1 and isinstance(node.ops[0], (ast.In, ast.NotIn)):
                if _dotted(node.comparators[0]) in {"os.environ", "environ"}:
                    name = _const_str(node.left)
                    if name:
                        record(mod, node, name, False, "unset")
                continue
            if not isinstance(node, ast.Call) or not node.args:
                continue
            fn_name = _dotted(node.func)
            short = fn_name.rsplit(".", 1)[-1]
            info = readers.get(fn_name)
            if info is None and short in readers and short not in {"get", "getenv"}:
                info = readers[short]
            if info is None and short == "getenv":
                info = (False, 0)  # ``__import__("os").getenv(...)`` and similar
            if info is None and short in BRAND_READERS:
                info = (True, 0)
            if info is None:
                # A callable handed in as an argument (``getter("DATAFLOW_X")``):
                # the product-prefixed literal is still the name that is read.
                literal = _const_str(node.args[0])
                if (
                    literal
                    and literal.startswith(BRAND_PREFIXES)
                    and short not in NOT_READERS
                    and ENV_NAME.match(literal)
                ):
                    record(mod, node, literal, False, _default_text(node, 1, mod))
                continue
            brand, idx = info
            if len(node.args) <= idx:
                continue
            encl = mod.enclosing_function(node)
            if encl is not None and encl.name in wrapper_names:
                # The forwarding call inside a wrapper: its callers are recorded instead.
                arg0 = node.args[idx]
                if isinstance(arg0, ast.Name) and arg0.id in {
                    a.arg for a in encl.args.posonlyargs + encl.args.args
                }:
                    continue
            names = _resolve_names(mod, node, node.args[idx])
            if names is None:
                is_helper_module = mod.rel.endswith("services/brand_env.py")
                if (fn_name in base or short in BRAND_READERS) and not is_helper_module:
                    where = encl.name + "()" if encl is not None else "module level"
                    dynamic.add(f"{mod.rel} — {where}")
                continue
            default = _default_text(node, idx + 1, mod)
            for raw in names:
                record(mod, node, raw, brand, default)

    return flags, sorted(dynamic)


def _load_descriptions() -> dict[str, str]:
    if not DESCRIPTIONS.is_file():
        return {}
    data = json.loads(DESCRIPTIONS.read_text(encoding="utf-8"))
    return {str(k): str(v) for k, v in data.items() if not str(k).startswith("_")}


def _cell(text: str) -> str:
    # Docstrings use reStructuredText double backticks; Markdown wants one.
    return text.replace("``", "`").replace("|", "\\|").replace("\n", " ").strip()


def _describe(flag: Flag, curated: dict[str, str]) -> str:
    text = _describe_base(flag, curated)
    exact = flag.exact_only_names()
    if exact:
        names = ", ".join(f"`{n}`" for n in exact)
        text = f"{text} Some readers accept only {names}.".strip()
    return text


def _describe_base(flag: Flag, curated: dict[str, str]) -> str:
    if flag.key in curated:
        return curated[flag.key]
    seen: list[str] = []
    for _rel, fn_name, doc in sorted(flag.contexts):
        doc = doc.rstrip(".")
        if len(doc) > 110:
            doc = doc[:107].rstrip() + "..."
        where = "Read at module level" if fn_name == "module level" else f"Read by `{fn_name}`"
        text = where + (f": {doc}" if doc else "")
        if text not in seen:
            seen.append(text)
    return seen[0] + (" (and other readers)" if len(seen) > 1 else "") if seen else ""


_SECRET_NAME = re.compile(r"(SECRET|PASSWORD|TOKEN|PEPPER|_KEY$|^KEY$|API_KEY|PRIVATE)")


def _default_cell(flag: Flag) -> str:
    if _SECRET_NAME.search(flag.key) and (flag.defaults - {"unset", '""', "required", "computed"}):
        # Never print a credential-shaped default, even a development one.
        return "set in code (not shown)"
    vals = sorted(flag.defaults - {"unset"}) or ["unset"]
    if len(vals) > 3:
        return "differs by reader"
    return " / ".join(f"`{v}`" if v not in {"unset", "required", "computed"} else v for v in vals)


def _files_cell(flag: Flag) -> str:
    files = sorted(flag.files)
    shown = ", ".join(f"`{f}`" for f in files[:2])
    if len(files) > 2:
        shown += f" +{len(files) - 2} more"
    return shown


def render_table(flags: dict[str, Flag], dynamic: list[str]) -> str:
    curated = _load_descriptions()
    branded = [f for f in flags.values() if f.brand_read]
    legacy_only = [
        f for f in flags.values() if not f.brand_read and all(n.startswith(BRAND_PREFIXES) for n in f.literal_names)
    ]
    plain = [f for f in flags.values() if not f.brand_read and f not in legacy_only]

    lines: list[str] = []
    lines.append(
        f"{len(flags)} flags: {len(branded)} accept both prefixes, "
        f"{len(legacy_only)} are read under one exact product-prefixed name only, "
        f"{len(plain)} are unprefixed."
    )
    lines.append("")

    def section(title: str, intro: str, rows: list[Flag], show_names: bool) -> None:
        lines.append(f"### {title}")
        lines.append("")
        lines.append(intro)
        lines.append("")
        head = "| Flag | Default | What it does | Read in |"
        lines.append(head)
        lines.append("|---|---|---|---|")
        for flag in sorted(rows, key=lambda f: f.key):
            label = f"`{flag.key}`"
            if show_names:
                label = "<br>".join(f"`{n}`" for n in flag.accepted_names())
            else:
                extra = [n for n in flag.accepted_names() if not n.startswith(BRAND_PREFIXES)]
                if extra:
                    label += "<br>also unprefixed " + ", ".join(f"`{n}`" for n in extra)
            lines.append(
                f"| {label} | {_cell(_default_cell(flag))} | {_cell(_describe(flag, curated))} "
                f"| {_cell(_files_cell(flag))} |"
            )
        lines.append("")

    section(
        "Flags that accept both prefixes",
        "Set `DATAWRAP_<flag>` (preferred) or `DATAFLOW_<flag>`. The table shows the part after the prefix.",
        branded,
        show_names=False,
    )
    section(
        "Flags read under one exact name",
        "These are read directly, without the brand helper, so only the name shown works. "
        "Setting the other prefix has no effect.",
        legacy_only,
        show_names=True,
    )
    section(
        "Unprefixed variables",
        "Standard or third-party names (cloud SDKs, database URLs, platform variables), read as shown.",
        plain,
        show_names=True,
    )
    lines.append("### Reads the generator cannot name")
    lines.append("")
    lines.append(
        "These functions read a variable whose name is built at run time, "
        "so the name does not appear in the tables above."
    )
    lines.append("")
    for site in dynamic:
        rel, _, where = site.partition(" — ")
        lines.append(f"- `{rel}` — `{where}`")
    lines.append("")
    return "\n".join(lines)


def table_flag_names(markdown: str) -> set[str]:
    """Flag keys listed in a rendered table (used by the drift test)."""
    start = markdown.index(BEGIN)
    end = markdown.index(END)
    keys: set[str] = set()
    for line in markdown[start:end].splitlines():
        if not line.startswith("| `"):
            continue
        first = line.split("|")[1]
        for name in re.findall(r"`([A-Z][A-Z0-9_]+)`", first):
            for prefix in BRAND_PREFIXES:
                if name.startswith(prefix) and len(name) > len(prefix):
                    name = name[len(prefix) :]
            keys.add(name)
    return keys


def build_document(existing: str) -> str:
    flags, dynamic = collect()
    table = render_table(flags, dynamic)
    if BEGIN not in existing or END not in existing:
        raise SystemExit(f"{TARGET} is missing the generated-block markers")
    head, _, rest = existing.partition(BEGIN)
    _, _, tail = rest.partition(END)
    return f"{head}{BEGIN}\n\n{table}\n{END}{tail}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--check", action="store_true", help="exit 1 if the table is stale")
    args = parser.parse_args(argv)
    existing = TARGET.read_text(encoding="utf-8")
    updated = build_document(existing)
    if args.check:
        if updated != existing:
            print("docs/operate/configuration.md is stale; run docs/tools/gen_config_reference.py")
            return 1
        return 0
    TARGET.write_text(updated, encoding="utf-8")
    print(f"wrote {TARGET.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
