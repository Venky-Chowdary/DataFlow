"""Clonable Transform Library templates — the Quickstart/Mapplet-template layer.

A template is a recipe definition the operator clones into their workspace and
then edits; cloning stamps nothing provenance-wise back at the template —
templates are a starting point, not a tracked dependency. Two starters ship in
P0: the cleansing+masking pair cited across ELT docs, and a type-safety recipe
that demonstrates safe-cast to quarantine.
"""

from __future__ import annotations

from typing import Any

from services.transform_recipe import RecipeStep, TransformRecipe

TEMPLATES: dict[str, dict[str, Any]] = {
    "standardise-email-hash-pii": {
        "name": "Standardise Email + Hash PII",
        "description": (
            "Trim + lowercase an email column, then SHA-256-hash it so the "
            "destination never stores the raw identifier. Clone, then edit the "
            "column names for your source."
        ),
        "steps": [
            {"column": "email", "op": "trim"},
            {"column": "email", "op": "lower"},
            {"column": "email", "op": "hash_pii"},
        ],
        "input_columns": ["email"],
        "output_columns": ["email"],
    },
    "trim-title-case-names": {
        "name": "Trim & Title-Case Names",
        "description": (
            "Collapse stray whitespace and title-case person-name columns "
            "(first_name, last_name by default) — the standardisation step "
            "every customer-360 pipeline repeats."
        ),
        "steps": [
            {"column": "first_name", "op": "trim"},
            {"column": "first_name", "op": "case", "options": {"mode": "title"}},
            {"column": "last_name", "op": "trim"},
            {"column": "last_name", "op": "case", "options": {"mode": "title"}},
        ],
        "input_columns": ["first_name", "last_name"],
        "output_columns": ["first_name", "last_name"],
    },
    "safe-cast-numerics": {
        "name": "Safe Cast + Null-Safe Numerics",
        "description": (
            "Coalesce a blank amount to a parameterised default, then cast to "
            "decimal — a row that still cannot parse refuses/quarantines via "
            "the transfer's normal CAST path, never silently nulls."
        ),
        "steps": [
            {
                "column": "amount",
                "op": "default_if_null",
                "options": {"value": ":default_amount"},
            },
            {"column": "amount", "op": "decimal"},
        ],
        "parameters": {
            "default_amount": {
                "default": "0",
                "description": "Value written when amount is blank",
            }
        },
        "input_columns": ["amount"],
        "output_columns": ["amount"],
    },
    "dedupe-keep-latest": {
        "name": "Dedupe Keep Latest",
        "description": (
            "One row per primary key, the winner decided by an ordering column "
            "(updated_at by default) — the survivorship rule an incremental "
            "dedupe load writes. Clone, then set the key and ordering columns."
        ),
        "steps": [
            {
                "op": "dedupe_rows",
                "options": {
                    "keys": [":pk_columns"],
                    "keep": "last",
                    "order_by": ":order_column",
                    "order_dir": "desc",
                },
            }
        ],
        "parameters": {
            "pk_columns": {
                "default": ["id"],
                "description": "Primary-key column list — one row survives per key",
            },
            "order_column": {
                "default": "updated_at",
                "description": "Column that decides which duplicate wins",
            },
        },
        "input_columns": ["id", "updated_at"],
        "output_columns": ["id", "updated_at"],
    },
    "scd2-history": {
        "name": "SCD-2 History (surrogate + effective dates)",
        "description": (
            "Type-2 slowly-changing-dimension shape: a surrogate key, the "
            "effective-from stamp filled from a version column, and a current "
            "flag on the incoming rows. Closing the previous version's range is "
            "destination-side history logic — this recipe prepares the row set "
            "the merge applies. Clone, then name the columns."
        ),
        "steps": [
            {"op": "surrogate_key", "options": {"to": ":sk_column", "start": 1}},
            {
                "op": "derive_column",
                "options": {
                    "to": ":effective_from_column",
                    "expression": ":version_column",
                },
            },
            {
                "op": "derive_column",
                "options": {"to": ":is_current_column", "expression": "'true'"},
            },
        ],
        "parameters": {
            "sk_column": {
                "default": "sk",
                "description": "Surrogate-key column the merge addresses",
            },
            "effective_from_column": {
                "default": "effective_from",
                "description": "Column holding the version's effective-from stamp",
            },
            "version_column": {
                "default": "updated_at",
                "description": "Source column supplying the version timestamp",
            },
            "is_current_column": {
                "default": "is_current",
                "description": "Current-version flag column",
            },
        },
        "input_columns": ["id", "updated_at"],
        "output_columns": ["id", "updated_at", "sk", "effective_from", "is_current"],
    },
}


def list_templates() -> list[dict[str, Any]]:
    return [
        {"key": key, "name": spec["name"], "description": spec["description"]}
        for key, spec in TEMPLATES.items()
    ]


def clone_template(key: str, workspace_id: str = "") -> TransformRecipe | None:
    spec = TEMPLATES.get(key)
    if spec is None:
        return None
    recipe = TransformRecipe(
        name=spec["name"],
        description=spec["description"],
        steps=[RecipeStep.from_dict(s) for s in spec["steps"]],
        parameters={
            str(k): dict(v or {})
            for k, v in (spec.get("parameters") or {}).items()
        },
        input_columns=tuple(str(c) for c in spec.get("input_columns") or ()),
        output_columns=tuple(str(c) for c in spec.get("output_columns") or ()),
        workspace_id=workspace_id,
    )
    return recipe
