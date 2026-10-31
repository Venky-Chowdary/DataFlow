"""The embedding columns of a pgvector typed table (owner decision 1, QA D209).

A new pgvector table is a typed table by default: every mapped source column
with its real type, written and verified exactly like PostgreSQL, plus

* ``embedding vector(n)`` — the vector of each row's embedded text, and
* ``embedding_text TEXT`` — only when the text is not already a column of the
  row (a declared ``text_template``).

The embedded text follows the one declared rule of
:func:`services.pgvector_layout.embedded_text_rule` (the same rule the
vector-document layout uses): the template, else ``content_column`` (SQL NULL
or '' embeds nothing: the embedding stays NULL), else precomputed vectors in
``embedding_column``, else the mapped row as canonical JSON.

The embedding cells are appended to each write batch as one more mapped
column, so the PostgreSQL writer creates and fills them in the same statement
as the row. They are never part of the Map, so Gate-8 still proves every
source column cell by cell and never reads a column the source lacks.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

logger = logging.getLogger(__name__)

EMBEDDING_COLUMN = "embedding"
EMBEDDING_FALLBACK_COLUMN = "embedding_vector"
TEXT_COLUMN = "embedding_text"
_EMBED_HEADER = "__df_pgvector_embedding__"
_TEXT_HEADER = "__df_pgvector_embedding_text__"

# Models whose dimension is fixed and well known (the plan must name it).
_KNOWN_DIMENSIONS = {
    "all-minilm-l6-v2": 384,
    "all-minilm-l12-v2": 384,
    "all-mpnet-base-v2": 768,
    "bge-small-en-v1.5": 384,
    "bge-base-en-v1.5": 768,
}


def _targets(mappings: list[dict[str, Any]] | None) -> list[str]:
    return [
        str(m.get("target_column") or m.get("target") or m.get("source") or "").strip()
        for m in mappings or []
        if str(m.get("target_column") or m.get("target") or m.get("source") or "").strip()
    ]


def embedding_column_names(
    mappings: list[dict[str, Any]] | None, options: Mapping[str, Any] | None
) -> tuple[str, str | None]:
    """``(embedding column, embedded-text column or None)`` for these mappings."""
    taken = {t.casefold() for t in _targets(mappings)}
    embed = EMBEDDING_COLUMN if EMBEDDING_COLUMN not in taken else EMBEDDING_FALLBACK_COLUMN
    text = None
    if (options or {}).get("text_template") is not None and TEXT_COLUMN not in taken:
        text = TEXT_COLUMN
    return embed, text


def embedding_dimension(options: Mapping[str, Any] | None) -> int | None:
    from services.brand_env import getenv_brand
    from services.vector_sync import vector_dimension_hint

    opts = dict(options or {})
    hint = vector_dimension_hint(opts.get("embedding_model"), opts)
    if hint:
        return hint
    model = str(
        opts.get("embedding_model")
        or getenv_brand("EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    )
    return _KNOWN_DIMENSIONS.get(model.rsplit("/", 1)[-1].strip().lower())


def embedding_storage(options: Mapping[str, Any] | None) -> str:
    storage = str((options or {}).get("vector_storage") or "vector").strip().lower()
    return storage if storage in {"vector", "halfvec"} else "vector"


def contract_columns(
    mappings: list[dict[str, Any]] | None,
    options: Mapping[str, Any] | None,
    existing_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """The embedding columns the typed table will have, after the mapped ones.

    An existing typed table keeps its shape: only embedding columns it already
    has are listed (and filled).
    """
    embed, text = embedding_column_names(mappings, options)
    dim = embedding_dimension(options)
    storage = embedding_storage(options)
    wanted: list[dict[str, Any]] = [
        {"name": embed, "type": f"{storage}({dim})" if dim else storage, "role": "embedding"}
    ]
    if text:
        wanted.append({"name": text, "type": "text", "role": "embedded_text"})
    if existing_columns is None:
        return wanted
    have = {str(c).casefold() for c in existing_columns}
    return [c for c in wanted if c["name"].casefold() in have]


def _row_text(
    record: Mapping[str, Any],
    rule: Mapping[str, Any],
    options: Mapping[str, Any],
    fields: list[str],
) -> str:
    from services.vectorization import _document_contract_text

    if rule.get("kind") == "template":
        from services.value_serializer import is_reader_null_cell
        from services.vector_template import render_record_template

        present = {k: v for k, v in record.items() if not is_reader_null_cell(v)}
        return render_record_template(str(rule["template"]), present)
    exclude = {str(c) for c in (options.get("exclude_pii_columns") or []) if c}
    return _document_contract_text(
        record,
        str(options.get("content_column") or "").strip() or None,
        None,
        exclude,
        fields,
    )


def augment_typed_batch(
    cfg: Mapping[str, Any],
    headers: list[str],
    data_rows: list[list[Any]],
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    *,
    create_table: bool,
    dest_column_types: Mapping[str, Any] | None,
) -> tuple[list[str], list[list[Any]], list[dict[str, Any]], dict[str, str]]:
    """Append the embedding (and embedded-text) cells to one write batch."""
    from services.pgvector_layout import document_options, embedded_text_rule

    options = document_options(cfg)
    live = [str(c) for c in (dest_column_types or {})]
    # An existing table keeps its shape: only embedding columns it has are filled.
    columns = contract_columns(mappings, options, live or None)
    if not columns:
        return headers, data_rows, mappings, column_types
    rule = embedded_text_rule(options)
    fields = _targets(mappings)
    source_of = {
        str(m.get("source") or "").strip(): str(m.get("target") or m.get("source") or "").strip()
        for m in mappings
    }
    index = {h: i for i, h in enumerate(headers)}
    records: list[dict[str, Any]] = []
    for row in data_rows:
        rec: dict[str, Any] = {}
        for src, tgt in source_of.items():
            i = index.get(src)
            if i is not None and i < len(row) and tgt:
                rec[tgt] = row[i]
        records.append(rec)

    vectors: list[list[float] | None] = [None] * len(records)
    texts: list[str] = [""] * len(records)
    if rule.get("kind") == "embedding_column":
        from services.vector_embedding import coerce_embedding

        col = str(rule.get("column") or "")
        for i, rec in enumerate(records):
            vec, err = coerce_embedding(rec.get(col))
            vectors[i] = None if err else vec
    else:
        from services.embedding_providers import provider_extra_from_options
        from services.vectorization import embed

        texts = [_row_text(rec, rule, options, fields) for rec in records]
        todo = [i for i, t in enumerate(texts) if t]
        if todo:
            got = embed(
                [texts[i] for i in todo],
                model=options.get("embedding_model"),
                embedding_extra=provider_extra_from_options(dict(cfg)),
            )
            for i, vec in zip(todo, got):
                vectors[i] = list(vec)

    embed_col = columns[0]
    if embed_col["role"] == "embedding" and "(" not in embed_col["type"]:
        dim = next((len(v) for v in vectors if v), None)
        if dim:
            embed_col = {**embed_col, "type": f"{embed_col['type']}({dim})"}

    out_headers = list(headers)
    out_mappings = list(mappings)
    out_types = dict(column_types)
    extra_cells: list[list[Any]] = [[] for _ in data_rows]
    for col in columns:
        if col["role"] == "embedding":
            col = embed_col
            header = _EMBED_HEADER
            cells = [
                "[" + ",".join(repr(float(x)) for x in v) + "]" if v else None
                for v in vectors
            ]
        else:
            header = _TEXT_HEADER
            cells = [t or None for t in texts]
        out_headers.append(header)
        out_mappings.append(
            {"source": header, "target": col["name"], "target_type": col["type"]}
        )
        out_types[header] = col["type"]
        for i, cell in enumerate(cells):
            extra_cells[i].append(cell)
    out_rows = [list(row) + extra for row, extra in zip(data_rows, extra_cells)]
    return out_headers, out_rows, out_mappings, out_types


def materialize_batch(
    headers: list[str],
    data_rows: list[list[Any]] | None,
    records: list[dict[str, Any]] | None,
    source_spool: Any,
) -> tuple[list[str], list[list[Any]]]:
    """One matrix for the batch, whichever form the reader handed over.

    The embedding needs each row, so a spool or records hand-off is read into
    the batch matrix here (one write batch, not the whole source).
    """
    rows = list(data_rows or [])
    if rows:
        return list(headers), rows
    if source_spool is not None and hasattr(source_spool, "iter_rows"):
        return list(getattr(source_spool, "headers", None) or headers), list(source_spool.iter_rows())
    if records:
        from src.transfer.adapters import records_to_matrix

        return records_to_matrix(list(records), list(headers))
    return list(headers), rows


def augment_writer_kwargs(
    common: dict[str, Any], cfg: Mapping[str, Any], dest_extra: Mapping[str, Any] | None
) -> None:
    """The adapter write path's version of :func:`augment_typed_batch` (in place).

    That path may hand the writer ``records`` or a spool instead of a matrix;
    the embedding needs the row, so records become a matrix here. A spool is
    left alone and logged: the stream path, which every database run takes,
    never builds one for this writer.
    """
    if common.get("source_spool") is not None:
        logger.warning("pgvector typed table: spooled adapter write carries no embedding")
        return
    headers = list(common.get("headers") or [])
    rows = list(common.get("data_rows") or [])
    if not rows and common.get("records"):
        from src.transfer.adapters import records_to_matrix

        headers, rows = records_to_matrix(list(common["records"]), headers)
        common["records"] = None
    extra = dict(dest_extra or {})
    live = extra.get("schema_types") or extra.get("destination_column_types") or {}
    (
        common["headers"],
        common["data_rows"],
        common["mappings"],
        common["column_types"],
    ) = augment_typed_batch(
        cfg,
        headers,
        rows,
        list(common.get("mappings") or []),
        dict(common.get("column_types") or {}),
        create_table=True,
        dest_column_types=live,
    )
