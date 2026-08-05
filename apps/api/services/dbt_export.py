"""Export Datawrap transform projects as dbt-compatible sources/models hooks.

This is a **complement** hook for warehouse ELT — not a dbt Cloud product.
Operators who already run dbt can import the generated files; Datawrap stays
the Map→Validate→Execute assurance path for the load itself.
"""

from __future__ import annotations

from typing import Any

from services.transform_models import TransformModel, extract_sources
from services.transform_store import TransformProject


def _yaml_quote(value: str) -> str:
    escaped = (value or "").replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _materialization_config(model: TransformModel) -> str:
    mat = model.materialization or "view"
    lines = [f"{{{{ config(materialized={_yaml_quote(mat)}"]
    if mat == "incremental" and model.unique_key:
        lines[0] += f", unique_key={_yaml_quote(model.unique_key)}"
        if model.incremental_strategy:
            lines[0] += f", incremental_strategy={_yaml_quote(model.incremental_strategy)}"
    lines[0] += ") }}"
    return lines[0]


def _model_sql(model: TransformModel) -> str:
    header = _materialization_config(model)
    body = (model.sql or "").strip()
    desc = (model.description or "").strip()
    comment = f"-- {desc}\n" if desc else ""
    return f"{comment}{header}\n\n{body}\n"


def _sources_yml(project: TransformProject, source_name: str) -> str:
    tables: list[str] = []
    for model in project.models:
        for src in extract_sources(model.sql):
            if src not in tables:
                tables.append(src)
    if not tables:
        # Honest empty stub — operator wires landed tables later.
        return (
            "version: 2\n\n"
            "sources: []\n"
            "# No {{ source('…') }} refs found in models. "
            "Add sources after the Datawrap load lands.\n"
        )
    lines = [
        "version: 2",
        "",
        "sources:",
        f"  - name: {source_name}",
        f"    description: Landed tables from Datawrap project {_yaml_quote(project.name)}",
        "    tables:",
    ]
    schema = (project.schema or "").strip()
    for table in tables:
        lines.append(f"      - name: {table}")
        if schema:
            lines.append(f"        description: Expected in schema `{schema}` after transfer")
    lines.append("")
    return "\n".join(lines)


def _schema_yml(project: TransformProject) -> str:
    lines = ["version: 2", "", "models:"]
    for model in project.models:
        lines.append(f"  - name: {model.name}")
        if model.description:
            lines.append(f"    description: {_yaml_quote(model.description)}")
        if model.tags:
            tag_list = ", ".join(_yaml_quote(t) for t in model.tags)
            lines.append(f"    config:\n      tags: [{tag_list}]")
        if model.tests:
            lines.append("    columns:")
            by_col: dict[str, list[Any]] = {}
            for t in model.tests:
                col = t.column or "_model"
                by_col.setdefault(col, []).append(t)
            for col, tests in by_col.items():
                if col == "_model":
                    continue
                lines.append(f"      - name: {col}")
                lines.append("        tests:")
                for t in tests:
                    if t.test_type == "accepted_values":
                        vals = ", ".join(_yaml_quote(v) for v in t.values)
                        lines.append("          - accepted_values:")
                        lines.append(f"              values: [{vals}]")
                    elif t.test_type == "relationships":
                        lines.append("          - relationships:")
                        lines.append(f"              to: ref('{t.to_model}')")
                        lines.append(f"              field: {t.to_column}")
                    else:
                        lines.append(f"          - {t.test_type}")
    lines.append("")
    return "\n".join(lines)


def export_dbt_files(project: TransformProject) -> dict[str, Any]:
    """Return a portable dbt starter pack for a transform project.

    Files are strings (path → content). Callers may zip or download as JSON.
    """
    source_name = "datawrap_landed"
    files: dict[str, str] = {
        "dbt_project.yml": (
            f"name: datawrap_{_safe_project_slug(project.name)}\n"
            "version: 1.0.0\n"
            "config-version: 2\n"
            "profile: datawrap_export\n"
            "model-paths: [\"models\"]\n"
            "seed-paths: [\"seeds\"]\n"
            "models:\n"
            f"  datawrap_{_safe_project_slug(project.name)}:\n"
            "    +materialized: view\n"
        ),
        "models/sources.yml": _sources_yml(project, source_name),
        "models/schema.yml": _schema_yml(project),
        "seeds/.gitkeep": "",
        "README_DATAWRAP.md": (
            "# Datawrap → dbt export\n\n"
            "This pack is a **complement hook**, not a managed dbt product.\n\n"
            "- Use Datawrap for Map → Validate (G1–G9) → Execute proof of the load.\n"
            "- Import these files into your dbt project for post-load models.\n"
            "- Do not treat this export as Gate-8 reconciliation evidence.\n"
        ),
    }
    for model in project.models:
        files[f"models/{model.name}.sql"] = _model_sql(model)

    return {
        "project_id": project.id,
        "project_name": project.name,
        "file_count": len(files),
        "files": files,
        "honesty": {
            "is_dbt_cloud": False,
            "is_managed_elt": False,
            "purpose": "Complement warehouse ELT with sources/models hooks after a governed load",
        },
    }


def _safe_project_slug(name: str) -> str:
    raw = "".join(ch if ch.isalnum() or ch == "_" else "_" for ch in (name or "project").lower())
    slug = raw.strip("_") or "project"
    return slug[:48]
