"""Document model: parse (strict), validate (every rule), hash (canonical)."""

from __future__ import annotations

import copy
import dataclasses
import re

import pytest

from services.document_model import (
    DEFAULT_MAX_DOC_BYTES,
    DEFAULT_MAX_ELEMENTS,
    ERROR,
    WARNING,
    DocumentModel,
    DocumentModelError,
    SourceTable,
    has_errors,
    model_hash,
    parse_model,
    validate_model,
)
from services.foreign_key_metadata import ForeignKey, ForeignKeys

# --- realistic fixture: customers -> addresses, orders -> order_items, countries


def _schemas() -> dict:
    return {
        "public.customers": {
            "columns": {
                "customer_id": "integer",
                "name": "varchar(200)",
                "email": "varchar(320)",
                "country_code": "char(2)",
                "created_at": "timestamp",
            },
            "primary_key": ["customer_id"],
            "unique_keys": [["email"]],
        },
        "public.addresses": {
            "columns": {
                "address_id": "integer",
                "customer_id": "integer",
                "street": "text",
                "city": "varchar(100)",
                "zip": "varchar(10)",
            },
            "primary_key": ["address_id"],
        },
        "public.orders": {
            "columns": {
                "order_id": "bigint",
                "customer_id": "integer",
                "ordered_at": "timestamp",
                "total": "numeric(12,2)",
                "external_ref": "uuid",
            },
            "primary_key": ["order_id"],
        },
        "public.order_items": {
            "columns": {
                "order_id": "bigint",
                "line_no": "integer",
                "sku": "varchar(40)",
                "qty": "integer",
                "price": "numeric(12,2)",
            },
            "primary_key": ["order_id", "line_no"],
        },
        "public.countries": {
            "columns": {"code": "char(2)", "name": "varchar(100)"},
            "primary_key": ["code"],
        },
        "public.tags": {
            "columns": {"customer_id": "integer", "tag": "varchar(30)", "weight": "real"},
            "primary_key": [],
        },
    }


def _fk(name, cols, table, ref_cols, schema="public"):
    return ForeignKey(
        name=name,
        columns=list(cols),
        referenced_schema=schema,
        referenced_table=table,
        referenced_columns=list(ref_cols),
    )


def _fks() -> dict:
    return {
        "public.addresses": [_fk("addr_cust", ["customer_id"], "customers", ["customer_id"])],
        "public.orders": [_fk("ord_cust", ["customer_id"], "customers", ["customer_id"])],
        "public.order_items": [_fk("item_ord", ["order_id"], "orders", ["order_id"])],
        "public.customers": [_fk("cust_country", ["country_code"], "countries", ["code"])],
    }


def _model() -> dict:
    return {
        "version": 1,
        "collection": "customers",
        "label": "Customer 360",
        "root": {
            "source": "public.customers",
            "key": ["customer_id"],
            "id": {"mode": "key"},
            "fields": [
                {"source": "name", "path": "name"},
                {"source": "email", "path": "contact.email", "label": "E-mail"},
                {"source": "created_at", "path": "created_at"},
            ],
        },
        "relationships": [
            {
                "id": "addresses",
                "parent": "root",
                "source": "public.addresses",
                "join": [{"parent": "customer_id", "child": "customer_id"}],
                "join_origin": "fk",
                "rule": "embed_array",
                "path": "addresses",
                "child_key": ["address_id"],
                "fields": [
                    {"source": "street", "path": "street"},
                    {"source": "city", "path": "city"},
                    {"source": "zip", "path": "zip"},
                ],
            },
            {
                "id": "orders",
                "parent": "root",
                "source": "public.orders",
                "join": [{"parent": "customer_id", "child": "customer_id"}],
                "join_origin": "fk",
                "rule": "embed_array",
                "path": "orders",
                "child_key": ["order_id"],
                "fields": [
                    {"source": "order_id", "path": "order_id"},
                    {"source": "ordered_at", "path": "ordered_at"},
                    {"source": "total", "path": "total"},
                ],
                "options": {"order_by": [["ordered_at", "desc"]]},
            },
            {
                "id": "items",
                "parent": "orders",
                "source": "public.order_items",
                "join": [{"parent": "order_id", "child": "order_id"}],
                "join_origin": "fk",
                "rule": "embed_array",
                "path": "items",
                "child_key": ["order_id", "line_no"],
                "fields": [
                    {"source": "sku", "path": "sku"},
                    {"source": "qty", "path": "qty"},
                    {"source": "price", "path": "price"},
                ],
            },
            {
                "id": "country",
                "parent": "root",
                "source": "public.countries",
                "join": [{"parent": "country_code", "child": "code"}],
                "join_origin": "fk",
                "rule": "embed_object",
                "path": "country",
                "fields": [
                    {"source": "code", "path": "code"},
                    {"source": "name", "path": "name"},
                ],
            },
        ],
        "limits": {"max_depth": 4},
    }


def _rel(model: dict, rel_id: str) -> dict:
    return next(r for r in model["relationships"] if r["id"] == rel_id)


def _codes(issues, severity=None):
    return [i.code for i in issues if severity is None or i.severity == severity]


def _validate(model=None, schemas=None, fks=None, **kw):
    return validate_model(
        _model() if model is None else model,
        _schemas() if schemas is None else schemas,
        _fks() if fks is None else fks,
        **kw,
    )


# --- parse -----------------------------------------------------------------


def test_realistic_model_parses_with_defaults():
    model = parse_model(_model())
    assert isinstance(model, DocumentModel)
    assert model.collection == "customers"
    assert model.root.key == ("customer_id",)
    assert model.root.id.mode == "key"
    assert [r.id for r in model.relationships] == ["addresses", "orders", "items", "country"]
    orders = model.relationships[1]
    assert orders.options.order_by == (("ordered_at", "desc"),)
    assert orders.options.max_elements == DEFAULT_MAX_ELEMENTS == 10000
    assert orders.options.drop_join_keys is True
    assert orders.options.on_orphan == "quarantine"
    assert orders.options.reference_shape == "id"
    assert orders.options.primitive is False
    assert orders.options.merge_into_parent is False
    assert model.limits.max_depth == 4
    assert model.limits.max_doc_bytes == DEFAULT_MAX_DOC_BYTES == 16252928
    assert model.limits.on_oversize == "quarantine"


def test_parse_round_trips_through_to_dict():
    model = parse_model(_model())
    again = parse_model(model.to_dict())
    assert again == model
    assert again.to_dict() == model.to_dict()


def test_minimal_model_fills_every_default():
    model = parse_model(
        {"collection": "c", "root": {"source": "t", "key": ["id"]}}
    )
    assert model.version == 1
    assert model.relationships == ()
    assert model.root.fields == ()
    assert model.root.id.mode == "key"
    rel = parse_model(
        {
            "collection": "c",
            "root": {"source": "t", "key": ["id"]},
            "relationships": [
                {"id": "r", "parent": "root", "source": "u", "rule": "embed_array"}
            ],
        }
    ).relationships[0]
    assert rel.join_origin == "declared"
    assert rel.path == ""
    assert rel.child_key == ()


def test_field_path_defaults_to_the_source_column():
    model = parse_model(
        {"collection": "c", "root": {"source": "t", "key": ["id"], "fields": [{"source": "name"}]}}
    )
    assert model.root.fields[0].path == "name"


def test_composite_id_mode_is_the_key_mode():
    # decisions.md #1: a composite key becomes an ordered sub-document _id;
    # that is what "key" does with several columns, so both spell one model.
    raw = _model()
    raw["root"]["id"] = {"mode": "composite"}
    assert parse_model(raw).root.id.mode == "key"
    assert model_hash(raw) == model_hash(_model())


@pytest.mark.parametrize(
    "mutate, where, key",
    [
        (lambda m: m.update(colour="red"), "model", "colour"),
        (lambda m: m["root"].update(filter={}), "root", "filter"),
        (lambda m: m["root"]["id"].update(kind="x"), "root.id", "kind"),
        (lambda m: m["root"]["fields"][0].update(type="text"), "root.fields[0]", "type"),
        (lambda m: m["relationships"][1].update(on="x"), "relationships[1]", "on"),
        (lambda m: m["relationships"][1]["join"][0].update(op="="), "relationships[1].join[0]", "op"),
        (lambda m: m["relationships"][1]["options"].update(sort="x"), "relationships[1].options", "sort"),
        (lambda m: m["limits"].update(max_elements=5), "limits", "max_elements"),
    ],
)
def test_unknown_keys_are_refused_by_name(mutate, where, key):
    raw = _model()
    mutate(raw)
    with pytest.raises(DocumentModelError) as exc:
        parse_model(raw)
    assert exc.value.code == "unknown_key"
    assert exc.value.where == where
    assert repr(key) in str(exc.value)


@pytest.mark.parametrize(
    "mutate, code, where",
    [
        (lambda m: m.pop("collection"), "missing_key", "model.collection"),
        (lambda m: m.pop("root"), "missing_key", "model.root"),
        (lambda m: m["root"].pop("source"), "missing_key", "root.source"),
        (lambda m: m["root"]["fields"][0].pop("source"), "missing_key", "root.fields[0].source"),
        (lambda m: m["relationships"][0].pop("id"), "missing_key", "relationships[0].id"),
        (lambda m: m["relationships"][0].pop("rule"), "missing_key", "relationships[0].rule"),
        (lambda m: m["relationships"][0].pop("parent"), "missing_key", "relationships[0].parent"),
        (lambda m: m["relationships"][0].pop("source"), "missing_key", "relationships[0].source"),
        (lambda m: m.update(version=2), "unsupported_version", "model.version"),
        (lambda m: m.update(collection=""), "wrong_type", "model.collection"),
        (lambda m: m.update(relationships={}), "wrong_type", "model.relationships"),
        (lambda m: m["root"].update(key="customer_id"), "wrong_type", "root.key"),
        (lambda m: m["root"].update(key=[""]), "wrong_type", "root.key[0]"),
        (lambda m: m["root"]["id"].update(mode="uuid"), "bad_value", "root.id.mode"),
        (lambda m: m["relationships"][0].update(rule="nest"), "bad_value", "relationships[0].rule"),
        (lambda m: m["relationships"][0].update(join_origin="guess"), "bad_value", "relationships[0].join_origin"),
        (lambda m: m["relationships"][0].update(join=[{"parent": "a"}]), "missing_key", "relationships[0].join[0].child"),
        (lambda m: m["relationships"][0].update(options={"max_elements": 0}), "bad_value", "relationships[0].options.max_elements"),
        (lambda m: m["relationships"][0].update(options={"max_elements": "10"}), "wrong_type", "relationships[0].options.max_elements"),
        (lambda m: m["relationships"][0].update(options={"max_elements": True}), "wrong_type", "relationships[0].options.max_elements"),
        (lambda m: m["relationships"][0].update(options={"drop_join_keys": "yes"}), "wrong_type", "relationships[0].options.drop_join_keys"),
        (lambda m: m["relationships"][0].update(options={"order_by": [["city", "up"]]}), "bad_value", "relationships[0].options.order_by[0]"),
        (lambda m: m["relationships"][0].update(options={"order_by": ["city"]}), "wrong_type", "relationships[0].options.order_by[0]"),
        (lambda m: m["relationships"][0].update(options={"reference_shape": "dbref"}), "bad_value", "relationships[0].options.reference_shape"),
        (lambda m: m["relationships"][0].update(options={"on_orphan": "drop"}), "bad_value", "relationships[0].options.on_orphan"),
        (lambda m: m["limits"].update(on_oversize="truncate"), "bad_value", "limits.on_oversize"),
        (lambda m: m["limits"].update(max_depth=0), "bad_value", "limits.max_depth"),
        (lambda m: m["limits"].update(max_doc_bytes=-1), "bad_value", "limits.max_doc_bytes"),
    ],
)
def test_malformed_models_are_refused_with_a_named_error(mutate, code, where):
    raw = _model()
    mutate(raw)
    with pytest.raises(DocumentModelError) as exc:
        parse_model(raw)
    assert (exc.value.code, exc.value.where) == (code, where)
    assert str(exc.value)
    assert "DataFlow" not in str(exc.value)


def test_non_object_model_is_refused():
    for bad in (None, [], "model", 3):
        with pytest.raises(DocumentModelError) as exc:
            parse_model(bad)
        assert exc.value.code == "model_not_object"


def test_parse_does_not_mutate_its_input():
    raw = _model()
    before = copy.deepcopy(raw)
    parse_model(raw)
    assert raw == before


# --- hash ------------------------------------------------------------------


def test_hash_is_short_hex_and_stable():
    digest = model_hash(_model())
    assert re.fullmatch(r"[0-9a-f]{16}", digest)
    assert model_hash(parse_model(_model())) == digest
    assert model_hash(parse_model(_model()).to_dict()) == digest


def test_hash_ignores_key_order_labels_and_spelled_out_defaults():
    raw = _model()
    reordered = dict(reversed(list(raw.items())))
    assert model_hash(reordered) == model_hash(raw)

    unlabeled = _model()
    unlabeled.pop("label")
    unlabeled["root"]["fields"][1].pop("label")
    _rel(unlabeled, "orders")["label"] = "Purchase history"
    assert model_hash(unlabeled) == model_hash(raw)

    explicit = _model()
    _rel(explicit, "orders")["options"].update(
        max_elements=10000, drop_join_keys=True, on_orphan="quarantine"
    )
    explicit["limits"].update(max_doc_bytes=16252928, on_oversize="quarantine")
    assert model_hash(explicit) == model_hash(raw)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda m: _rel(m, "orders").update(path="purchases"),
        lambda m: _rel(m, "country").update(rule="reference"),
        lambda m: _rel(m, "orders")["options"].update(max_elements=50),
        lambda m: _rel(m, "orders")["options"].update(order_by=[["ordered_at", "asc"]]),
        lambda m: m["root"]["fields"].append({"source": "country_code", "path": "cc"}),
        lambda m: m["root"].update(id={"mode": "objectid"}),
        lambda m: m.update(collection="clients"),
        lambda m: m["limits"].update(on_oversize="fail"),
    ],
)
def test_hash_changes_with_every_semantic_change(mutate):
    raw = _model()
    mutate(raw)
    assert model_hash(raw) != model_hash(_model())


def test_no_model_has_no_hash():
    assert model_hash(None) == ""
    assert model_hash({}) == ""


def test_hash_of_a_malformed_model_raises():
    with pytest.raises(DocumentModelError):
        model_hash({"collection": "c"})


# --- validate: the realistic model is clean --------------------------------


def test_realistic_model_has_no_issues():
    assert _validate() == []


def test_validate_accepts_a_parsed_model_and_structured_inputs():
    schemas = {name: SourceTable.from_dict(raw) for name, raw in _schemas().items()}
    assert validate_model(parse_model(_model()), schemas, _fks()) == []


def test_validate_accepts_measured_foreign_key_sets():
    measured = [
        ForeignKeys(
            dialect="postgresql",
            status="measured",
            schema="public",
            table=owner.split(".")[1],
            items=items,
        )
        for owner, items in _fks().items()
    ]
    assert _validate(fks=measured) == []


def test_validate_accepts_foreign_keys_as_plain_dicts():
    fks = {
        owner: [fk.to_dict() for fk in items] for owner, items in _fks().items()
    }
    assert _validate(fks=fks) == []


def test_validate_reports_a_malformed_model_as_one_issue():
    raw = _model()
    raw["colour"] = "red"
    issues = _validate(raw)
    assert [(i.code, i.severity, i.path) for i in issues] == [("unknown_key", ERROR, "model")]


def test_issue_shape_and_voice():
    raw = _model()
    _rel(raw, "orders")["join"] = [{"parent": "customer_id", "child": "missing_col"}]
    issues = _validate(raw)
    assert issues
    for issue in issues:
        d = issue.to_dict()
        assert set(d) == {"code", "severity", "path", "message"}
        assert d["severity"] in {ERROR, WARNING}
        assert d["message"] and "DataFlow" not in d["message"]
    assert has_errors(issues)
    assert not has_errors([i for i in issues if i.severity == WARNING])


# --- validate: tree, recursion, depth --------------------------------------


def test_parent_must_exist():
    raw = _model()
    _rel(raw, "items")["parent"] = "baskets"
    issues = _validate(raw)
    assert _codes(issues) == ["parent_unknown"]
    assert issues[0].path == "relationships[2].parent"


def test_parent_must_come_earlier():
    raw = _model()
    items = raw["relationships"].pop(2)
    raw["relationships"].insert(0, items)
    assert "parent_not_earlier" in _codes(_validate(raw), ERROR)


def test_relationship_ids_are_unique_and_not_root():
    raw = _model()
    _rel(raw, "country")["id"] = "orders"
    assert "duplicate_relationship_id" in _codes(_validate(raw), ERROR)
    raw = _model()
    _rel(raw, "country")["id"] = "root"
    assert "relationship_id_reserved" in _codes(_validate(raw), ERROR)


def test_duplicate_ids_keep_separate_element_namespaces():
    raw = _model()
    raw["relationships"].append(
        _tags(id="addresses", fields=[{"source": "tag", "path": "street"}], options={})
    )
    # Both arrays keep their own elements ("street" in each is fine); only the
    # duplicate id is reported (plus the tags array's missing child key).
    assert _codes(_validate(raw)) == ["duplicate_relationship_id", "child_key_missing"]


def test_same_source_twice_on_an_ancestor_chain_is_recursion():
    raw = _model()
    raw["relationships"].append(
        {
            "id": "reorders",
            "parent": "items",
            "source": "public.orders",
            "join": [{"parent": "order_id", "child": "order_id"}],
            "rule": "embed_object",
            "path": "order",
        }
    )
    issues = _validate(raw)
    assert "recursive_source" in _codes(issues, ERROR)


def test_root_source_counts_as_an_ancestor():
    raw = _model()
    raw["relationships"].append(
        {
            "id": "referrer",
            "parent": "root",
            "source": "public.customers",
            "join": [{"parent": "customer_id", "child": "customer_id"}],
            "rule": "embed_object",
            "path": "referrer",
        }
    )
    assert "recursive_source" in _codes(_validate(raw), ERROR)


def test_same_source_on_sibling_branches_is_not_recursion():
    raw = _model()
    raw["relationships"].append(
        {
            "id": "billing_country",
            "parent": "addresses",
            "source": "public.countries",
            "join": [{"parent": "city", "child": "name"}],
            "rule": "reference",
            "path": "country_name",
        }
    )
    # countries appears under root and under addresses: different chains.
    assert "recursive_source" not in _codes(_validate(raw))


def test_depth_is_capped_by_max_depth():
    raw = _model()
    raw["limits"]["max_depth"] = 1
    issues = _validate(raw)
    assert _codes(issues) == ["too_deep"]
    assert issues[0].path == "relationships[2]"
    raw["limits"]["max_depth"] = 2
    assert _validate(raw) == []


def test_max_depth_has_a_hard_cap_of_ten():
    raw = _model()
    raw["limits"]["max_depth"] = 10
    assert _validate(raw) == []
    raw["limits"]["max_depth"] = 11
    assert _codes(_validate(raw)) == ["max_depth_over_cap"]


def test_children_only_nest_under_embedded_parents():
    raw = _model()
    _rel(raw, "orders").update(rule="collection", path="")
    assert "parent_not_embedded" in _codes(_validate(raw), ERROR)


# --- validate: sources, columns, join types --------------------------------


def test_unknown_sources_are_named():
    raw = _model()
    raw["root"]["source"] = "public.clients"
    assert "root_source_unknown" in _codes(_validate(raw), ERROR)
    raw = _model()
    _rel(raw, "items")["source"] = "public.lines"
    assert "source_unknown" in _codes(_validate(raw), ERROR)


@pytest.mark.parametrize(
    "mutate, path",
    [
        (lambda m: m["root"].update(key=["cust_no"]), "root.key[0]"),
        (lambda m: m["root"]["fields"][0].update(source="full_name"), "root.fields[0].source"),
        (lambda m: _rel(m, "orders")["join"][0].update(parent="cust"), "relationships[1].join[0].parent"),
        (lambda m: _rel(m, "orders")["join"][0].update(child="cust"), "relationships[1].join[0].child"),
        (lambda m: _rel(m, "orders").update(child_key=["order_no"]), "relationships[1].child_key[0]"),
        (lambda m: _rel(m, "orders")["fields"][0].update(source="order_no"), "relationships[1].fields[0].source"),
        (lambda m: _rel(m, "orders")["options"].update(order_by=[["placed", "asc"]]), "relationships[1].options.order_by[0]"),
    ],
)
def test_every_column_reference_must_exist(mutate, path):
    raw = _model()
    mutate(raw)
    issues = [i for i in _validate(raw) if i.code == "column_unknown"]
    assert [i.path for i in issues] == [path]
    assert issues[0].severity == ERROR


def test_join_is_required():
    raw = _model()
    _rel(raw, "orders")["join"] = []
    assert "join_missing" in _codes(_validate(raw), ERROR)


def test_incompatible_join_types_are_refused():
    schemas = _schemas()
    schemas["public.orders"]["columns"]["customer_id"] = "varchar(20)"
    issues = _validate(schemas=schemas)
    assert _codes(issues) == ["join_type_mismatch"]
    assert issues[0].severity == ERROR
    assert issues[0].path == "relationships[1].join[0]"


def test_numeric_families_join_cleanly():
    schemas = _schemas()
    schemas["public.orders"]["columns"]["customer_id"] = "numeric(10,0)"
    schemas["public.addresses"]["columns"]["customer_id"] = "bigint"
    assert _validate(schemas=schemas) == []


def test_text_like_joins_of_different_kinds_warn():
    schemas = _schemas()
    schemas["public.countries"]["columns"]["code"] = "uuid"
    issues = _validate(schemas=schemas)
    assert _codes(issues) == ["join_type_lossy"]
    assert issues[0].severity == WARNING


def test_float_join_keys_warn():
    schemas = _schemas()
    schemas["public.orders"]["columns"]["customer_id"] = "double precision"
    assert _codes(_validate(schemas=schemas)) == ["join_type_lossy"]


def test_unknown_types_are_not_compared():
    schemas = _schemas()
    schemas["public.orders"]["columns"]["customer_id"] = ""
    assert _validate(schemas=schemas) == []


def test_fk_origin_needs_a_matching_foreign_key():
    fks = _fks()
    fks.pop("public.orders")
    issues = _validate(fks=fks)
    assert _codes(issues) == ["join_fk_missing"]
    raw = _model()
    _rel(raw, "orders")["join_origin"] = "declared"
    # declared join: the parent's primary key is the proof, no FK needed.
    assert _validate(raw, fks=fks) == []


def test_fk_in_either_direction_satisfies_fk_origin():
    # country: customers.country_code -> countries.code (parent references child)
    raw = _model()
    assert "join_fk_missing" not in _codes(_validate(raw))


def test_fk_must_match_every_join_pair():
    raw = _model()
    _rel(raw, "items")["join"].append({"parent": "customer_id", "child": "line_no"})
    assert "join_fk_missing" in _codes(_validate(raw), ERROR)


def test_schema_less_table_names_match_qualified_foreign_keys():
    schemas = {k.split(".")[1]: v for k, v in _schemas().items()}
    raw = _model()
    raw["root"]["source"] = "customers"
    for rel in raw["relationships"]:
        rel["source"] = rel["source"].split(".")[1]
    fks = {k.split(".")[1]: v for k, v in _fks().items()}
    assert validate_model(raw, schemas, fks) == []


# --- validate: embed_array -------------------------------------------------


def test_embed_array_child_must_be_the_many_side():
    # countries is the "one" side of customers.country_code: embedding it as
    # an array under customers would copy each country into many customers.
    raw = _model()
    _rel(raw, "country").update(rule="embed_array", child_key=["code"])
    issues = _validate(raw)
    assert _codes(issues) == ["embed_array_parent_not_unique"]
    assert issues[0].severity == ERROR
    assert "embed_object" in issues[0].message


def test_embed_array_parent_uniqueness_from_a_sample_only_warns():
    schemas = _schemas()
    schemas["public.customers"]["primary_key"] = []
    schemas["public.customers"]["sampled_unique"] = [["customer_id"]]
    fks = _fks()
    fks.pop("public.addresses")
    fks.pop("public.orders")
    raw = _model()
    for rel_id in ("addresses", "orders"):
        _rel(raw, rel_id)["join_origin"] = "declared"
    codes = _codes(_validate(raw, schemas, fks))
    assert codes.count("embed_array_parent_unique_by_sample_only") == 2
    assert "root_key_unique_by_sample_only" in codes
    assert not has_errors(_validate(raw, schemas, fks))


def test_embed_array_parent_uniqueness_from_a_foreign_key_only_warns():
    schemas = _schemas()
    schemas["public.orders"]["primary_key"] = []
    # items' FK onto orders.order_id is the only sign order_id is unique on
    # orders. SQLite and MySQL accept a foreign key to non-unique columns, so
    # it is a warning, both for items' parent and for orders' child_key.
    issues = _validate(schemas=schemas)
    assert [(i.code, i.severity, i.path) for i in issues] == [
        ("uniqueness_unproven", WARNING, "relationships[1].child_key"),
        ("uniqueness_unproven", WARNING, "relationships[2].join"),
    ]
    declared = _schemas()
    declared["public.orders"]["primary_key"] = []
    declared["public.orders"]["unique_keys"] = [["order_id"]]
    assert _validate(schemas=declared) == []
    fks = _fks()
    fks.pop("public.order_items")
    raw = _model()
    _rel(raw, "items")["join_origin"] = "declared"
    codes = _codes(_validate(raw, schemas, fks), ERROR)
    assert codes == ["child_key_not_unique", "embed_array_parent_not_unique"]


def test_embed_array_without_child_key_is_count_only():
    raw = _model()
    _rel(raw, "addresses")["child_key"] = []
    issues = _validate(raw)
    assert _codes(issues) == ["child_key_missing"]
    assert issues[0].severity == WARNING
    assert "count" in issues[0].message.lower()


def test_embed_array_without_child_key_is_refused_for_cdc():
    raw = _model()
    _rel(raw, "addresses")["child_key"] = []
    issues = _validate(raw, cdc=True)
    assert _codes(issues) == ["child_key_missing"]
    assert issues[0].severity == ERROR
    assert _validate(cdc=True) == []


def test_child_key_must_be_unique():
    raw = _model()
    _rel(raw, "items")["child_key"] = ["sku"]
    issues = _validate(raw)
    assert _codes(issues) == ["child_key_not_unique"]
    assert issues[0].severity == ERROR


def test_child_key_unique_by_sample_only_warns():
    schemas = _schemas()
    schemas["public.order_items"]["sampled_unique"] = [["sku"]]
    raw = _model()
    _rel(raw, "items")["child_key"] = ["sku"]
    issues = _validate(raw, schemas)
    assert _codes(issues) == ["child_key_unique_by_sample_only"]
    assert issues[0].severity == WARNING


def test_child_key_wider_than_a_unique_key_is_unique():
    raw = _model()
    _rel(raw, "items")["child_key"] = ["order_id", "line_no", "sku"]
    assert _validate(raw) == []


# --- validate: embed_object, reference, collection --------------------------


def test_embed_object_needs_proof_the_child_is_unique():
    schemas = _schemas()
    schemas["public.countries"]["primary_key"] = []
    fks = _fks()
    fks.pop("public.customers")
    raw = _model()
    _rel(raw, "country")["join_origin"] = "declared"
    issues = _validate(raw, schemas, fks)
    assert _codes(issues) == ["embed_object_not_unique"]
    assert issues[0].severity == ERROR


def test_embed_object_uniqueness_from_a_sample_only_warns():
    schemas = _schemas()
    schemas["public.countries"]["primary_key"] = []
    schemas["public.countries"]["sampled_unique"] = [["code"]]
    fks = _fks()
    fks.pop("public.customers")
    raw = _model()
    _rel(raw, "country")["join_origin"] = "declared"
    issues = _validate(raw, schemas, fks)
    assert _codes(issues) == ["embed_object_unique_by_sample_only"]
    assert issues[0].severity == WARNING


def test_embed_object_uniqueness_from_the_parents_foreign_key_only_warns():
    schemas = _schemas()
    schemas["public.countries"]["primary_key"] = []
    assert _codes(_validate(schemas=schemas), WARNING) == ["uniqueness_unproven"]
    assert not has_errors(_validate(schemas=schemas))


def test_embed_object_of_the_many_side_is_refused():
    raw = _model()
    _rel(raw, "orders").update(rule="embed_object", child_key=[])
    _rel(raw, "orders")["options"] = {}
    _rel(raw, "items")["parent"] = "orders"
    assert "embed_object_not_unique" in _codes(_validate(raw), ERROR)


def test_reference_by_id_needs_the_same_proof_as_embed_object():
    raw = _model()
    _rel(raw, "country").update(rule="reference", fields=[])
    assert _validate(raw) == []
    schemas = _schemas()
    schemas["public.countries"]["primary_key"] = []
    fks = _fks()
    fks.pop("public.customers")
    _rel(raw, "country")["join_origin"] = "declared"
    assert _codes(_validate(raw, schemas, fks)) == ["reference_not_unique"]


def test_reference_ids_array_needs_a_child_key_and_a_unique_parent():
    raw = _model()
    _rel(raw, "addresses").update(rule="reference", fields=[], options={"reference_shape": "ids_array"})
    assert _validate(raw) == []
    _rel(raw, "addresses")["child_key"] = []
    assert _codes(_validate(raw)) == ["reference_ids_need_child_key"]
    schemas = _schemas()
    schemas["public.customers"]["primary_key"] = []
    schemas["public.customers"]["unique_keys"] = []
    fks = _fks()
    fks.pop("public.addresses")
    fks.pop("public.orders")
    _rel(raw, "addresses").update(child_key=["address_id"], join_origin="declared")
    _rel(raw, "orders")["join_origin"] = "declared"
    codes = _codes(_validate(raw, schemas, fks), ERROR)
    assert "reference_parent_not_unique" in codes
    assert "embed_array_parent_not_unique" in codes


def test_collection_rule_needs_no_path():
    raw = _model()
    _rel(raw, "addresses").update(rule="collection", path="")
    assert _validate(raw) == []


def test_embedded_relationships_need_a_path():
    for rule in ("embed_array", "embed_object", "reference"):
        raw = _model()
        _rel(raw, "country").update(rule=rule, path="", child_key=["code"])
        assert "path_missing" in _codes(_validate(raw), ERROR), rule


# --- validate: primitive and merge_into_parent ------------------------------


def _tags(**over) -> dict:
    rel = {
        "id": "tags",
        "parent": "root",
        "source": "public.tags",
        "join": [{"parent": "customer_id", "child": "customer_id"}],
        "rule": "embed_array",
        "path": "tags",
        "fields": [{"source": "tag"}],
        "options": {"primitive": True},
    }
    rel.update(over)
    return rel


def test_primitive_array_with_one_field():
    raw = _model()
    raw["relationships"].append(_tags())
    assert _codes(_validate(raw)) == ["child_key_missing"]


def test_primitive_needs_exactly_one_field():
    raw = _model()
    raw["relationships"].append(_tags(fields=[{"source": "tag"}, {"source": "weight"}]))
    assert "primitive_needs_one_field" in _codes(_validate(raw), ERROR)
    raw = _model()
    # No fields listed: every non-join column (tag, weight) would be used.
    raw["relationships"].append(_tags(fields=[]))
    assert "primitive_needs_one_field" in _codes(_validate(raw), ERROR)


def test_primitive_is_only_for_embedded_values():
    raw = _model()
    _rel(raw, "country").update(rule="reference", fields=[{"source": "name"}])
    _rel(raw, "country")["options"] = {"primitive": True}
    assert "primitive_not_supported" in _codes(_validate(raw), ERROR)


def test_primitive_embed_object_is_a_single_value():
    raw = _model()
    _rel(raw, "country").update(fields=[{"source": "name"}], options={"primitive": True})
    assert _validate(raw) == []


def test_children_cannot_nest_under_a_primitive():
    raw = _model()
    _rel(raw, "orders")["options"] = {"primitive": True}
    _rel(raw, "orders")["fields"] = [{"source": "total"}]
    assert "parent_not_embedded" in _codes(_validate(raw), ERROR)


def test_merge_into_parent_places_fields_beside_the_parents():
    raw = _model()
    _rel(raw, "country").update(
        path="", fields=[{"source": "name", "path": "country_name"}], options={"merge_into_parent": True}
    )
    assert _validate(raw) == []


def test_merge_into_parent_must_not_collide():
    raw = _model()
    _rel(raw, "country").update(
        path="", fields=[{"source": "name", "path": "name"}], options={"merge_into_parent": True}
    )
    issues = _validate(raw)
    assert _codes(issues) == ["merge_into_parent_collision"]
    assert issues[0].path == "relationships[3].fields[0].path"


def test_merge_into_parent_expands_fields_and_drops_join_keys():
    raw = _model()
    _rel(raw, "country").update(path="", fields=[], options={"merge_into_parent": True})
    # countries' columns are code and name; code is the join key and is
    # dropped, name collides with the root's own "name".
    assert _codes(_validate(raw)) == ["merge_into_parent_collision"]
    _rel(raw, "country")["options"]["drop_join_keys"] = False
    raw["root"]["fields"][0]["path"] = "full_name"
    assert _validate(raw) == []
    raw["root"]["fields"].append({"source": "country_code", "path": "code"})
    assert _codes(_validate(raw)) == ["merge_into_parent_collision"]


def test_merge_into_parent_collides_with_a_parent_sub_document():
    raw = _model()
    _rel(raw, "country").update(
        path="", fields=[{"source": "name", "path": "contact"}], options={"merge_into_parent": True}
    )
    assert _codes(_validate(raw)) == ["merge_into_parent_collision"]


def test_merge_into_parent_is_for_embed_object_only():
    raw = _model()
    _rel(raw, "addresses").update(path="", options={"merge_into_parent": True})
    assert "merge_into_parent_not_object" in _codes(_validate(raw), ERROR)
    raw = _model()
    _rel(raw, "country").update(
        path="", fields=[{"source": "name"}], options={"merge_into_parent": True, "primitive": True}
    )
    assert "merge_into_parent_not_object" in _codes(_validate(raw), ERROR)


def test_merge_into_parent_takes_no_path():
    raw = _model()
    _rel(raw, "country").update(
        fields=[{"source": "name", "path": "country_name"}], options={"merge_into_parent": True}
    )
    assert _codes(_validate(raw)) == ["merge_into_parent_has_path"]


# --- validate: paths ---------------------------------------------------------


@pytest.mark.parametrize(
    "mutate, code, path",
    [
        (lambda m: m["root"]["fields"][1].update(path="contact..email"), "path_empty_segment", "root.fields[1].path"),
        (lambda m: m["root"]["fields"][1].update(path="$email"), "path_dollar_segment", "root.fields[1].path"),
        (lambda m: m["root"]["fields"][1].update(path="_id"), "id_path_reserved", "root.fields[1].path"),
        (lambda m: m["root"]["fields"][1].update(path="_id.email"), "id_path_reserved", "root.fields[1].path"),
        (lambda m: _rel(m, "orders").update(path="_id"), "id_path_reserved", "relationships[1].path"),
        (lambda m: _rel(m, "items")["fields"][0].update(path="_id"), "path_id_not_root", "relationships[2].fields[0].path"),
        (lambda m: _rel(m, "items").update(path="a.$b"), "path_dollar_segment", "relationships[2].path"),
        (lambda m: m["root"]["fields"][0].update(path="orders"), "path_duplicate", "relationships[1].path"),
        (lambda m: m["root"]["fields"][0].update(path="orders.count"), "path_array_collision", "root.fields[0].path"),
        (lambda m: m["root"]["fields"][0].update(path="contact"), "path_prefix_collision", "root.fields[1].path"),
        (lambda m: _rel(m, "items")["fields"][1].update(path="sku"), "path_duplicate", "relationships[2].fields[1].path"),
        (lambda m: m["root"]["fields"][0].update(path="country.name"), "path_duplicate", "relationships[3].fields[1].path"),
    ],
)
def test_path_rules(mutate, code, path):
    raw = _model()
    mutate(raw)
    issues = _validate(raw)
    assert [(i.code, i.path) for i in issues] == [(code, path)]
    assert issues[0].severity == ERROR


def test_element_paths_live_in_their_own_namespace():
    raw = _model()
    # "total" in an order element does not collide with a root "total".
    raw["root"]["fields"].append({"source": "country_code", "path": "total"})
    assert _validate(raw) == []


def test_reference_ids_array_is_an_array_path():
    raw = _model()
    _rel(raw, "addresses").update(rule="reference", fields=[], options={"reference_shape": "ids_array"})
    raw["root"]["fields"].append({"source": "country_code", "path": "addresses.cc"})
    assert _codes(_validate(raw)) == ["path_array_collision"]


# --- validate: root, collection, limits, option hygiene ---------------------


def test_root_key_is_required_and_unique():
    raw = _model()
    raw["root"]["key"] = []
    assert _codes(_validate(raw)) == ["root_key_missing"]
    raw = _model()
    raw["root"]["key"] = ["name"]
    issues = _validate(raw)
    assert "root_key_not_unique" in _codes(issues, ERROR)


def test_composite_root_key_is_unique_through_a_unique_constraint():
    raw = _model()
    raw["root"]["key"] = ["email", "name"]
    # email alone is unique, so (email, name) is too; the children still join
    # on customer_id, which stays the primary key.
    assert _validate(raw) == []


def test_objectid_mode_still_needs_a_unique_key():
    # decisions.md #1: the ObjectId option keeps a unique index on the key.
    raw = _model()
    raw["root"]["id"] = {"mode": "objectid"}
    assert _validate(raw) == []
    raw["root"]["key"] = ["name"]
    assert "root_key_not_unique" in _codes(_validate(raw), ERROR)


@pytest.mark.parametrize("name", ["$customers", "system.users", "a\x00b", "has$dollar"])
def test_collection_name_rules(name):
    raw = _model()
    raw["collection"] = name
    assert _codes(_validate(raw)) == ["collection_name_invalid"]


def test_max_doc_bytes_cannot_exceed_mongodb_limit():
    raw = _model()
    raw["limits"]["max_doc_bytes"] = 16 * 1024 * 1024
    assert _validate(raw) == []
    raw["limits"]["max_doc_bytes"] = 16 * 1024 * 1024 + 1
    assert _codes(_validate(raw)) == ["max_doc_bytes_over_limit"]


def test_order_by_outside_an_array_is_ignored_with_a_warning():
    raw = _model()
    _rel(raw, "country")["options"] = {"order_by": [["name", "asc"]]}
    issues = _validate(raw)
    assert _codes(issues) == ["option_ignored"]
    assert issues[0].severity == WARNING
    assert issues[0].path == "relationships[3].options.order_by"


# --- review of PR 417: parse strictness (M1, M2, L4, L5) ---------------------


@pytest.mark.parametrize("rule", [None, "missing"])
def test_a_null_or_missing_rule_is_refused_like_any_required_key(rule):
    # M1: a null rule used to parse as "" and validate clean, with a hash of
    # its own, so an approval of a rule-less model passed Execute.
    raw = _model()
    if rule == "missing":
        _rel(raw, "country").pop("rule")
    else:
        _rel(raw, "country")["rule"] = rule
    with pytest.raises(DocumentModelError) as exc:
        parse_model(raw)
    assert (exc.value.code, exc.value.where) == ("missing_key", "relationships[3].rule")
    with pytest.raises(DocumentModelError):
        model_hash(raw)
    assert [(i.code, i.path) for i in _validate(raw)] == [
        ("missing_key", "relationships[3].rule")
    ]


_SURROGATE = "x\ud800"


@pytest.mark.parametrize(
    "mutate, where",
    [
        (lambda m: m.update(collection=_SURROGATE), "model.collection"),
        (lambda m: m.update(label=_SURROGATE), "model.label"),
        (lambda m: m["root"].update(source=_SURROGATE), "root.source"),
        (lambda m: m["root"].update(key=[_SURROGATE]), "root.key[0]"),
        (lambda m: m["root"]["fields"][0].update(source=_SURROGATE), "root.fields[0].source"),
        (lambda m: m["root"]["fields"][0].update(path=_SURROGATE), "root.fields[0].path"),
        (lambda m: m["root"]["fields"][0].update(label=_SURROGATE), "root.fields[0].label"),
        (lambda m: m["relationships"][1].update(id=_SURROGATE), "relationships[1].id"),
        (lambda m: m["relationships"][1].update(parent=_SURROGATE), "relationships[1].parent"),
        (lambda m: m["relationships"][1].update(source=_SURROGATE), "relationships[1].source"),
        (lambda m: m["relationships"][1].update(path=_SURROGATE), "relationships[1].path"),
        (lambda m: m["relationships"][1].update(label=_SURROGATE), "relationships[1].label"),
        (lambda m: m["relationships"][1].update(child_key=[_SURROGATE]), "relationships[1].child_key[0]"),
        (
            lambda m: m["relationships"][1]["join"][0].update(child=_SURROGATE),
            "relationships[1].join[0].child",
        ),
        (
            lambda m: m["relationships"][1]["fields"][0].update(path=_SURROGATE),
            "relationships[1].fields[0].path",
        ),
        (
            lambda m: m["relationships"][1]["options"].update(order_by=[[_SURROGATE, "asc"]]),
            "relationships[1].options.order_by[0]",
        ),
    ],
)
def test_text_that_is_not_valid_utf8_is_refused_where_it_is(mutate, where):
    # M2: a lone surrogate cannot be written as BSON (or hashed); it used to
    # crash model_hash, validate_model and the approval check.
    raw = _model()
    mutate(raw)
    with pytest.raises(DocumentModelError) as exc:
        parse_model(raw)
    assert (exc.value.code, exc.value.where) == ("bad_value", where)
    assert "UTF-8" in str(exc.value)
    with pytest.raises(DocumentModelError):
        model_hash(raw)
    assert [(i.code, i.path) for i in _validate(raw)] == [("bad_value", where)]


def test_a_parsed_model_built_by_hand_is_held_to_the_same_rules():
    # validate_model and model_hash accept a DocumentModel; one built in code
    # must not slip past the parser's rules.
    model = parse_model(_model())
    bad = DocumentModel(collection=_SURROGATE, root=model.root)
    with pytest.raises(DocumentModelError) as exc:
        model_hash(bad)
    assert exc.value.code == "bad_value"
    assert _codes(_validate(bad)) == ["bad_value"]


@pytest.mark.parametrize(
    "mutate, where",
    [
        (lambda m: m["root"].update(key=["customer_id", "customer_id"]), "root.key[1]"),
        (
            lambda m: _rel(m, "items").update(child_key=["order_id", "line_no", "order_id"]),
            "relationships[2].child_key[2]",
        ),
    ],
)
def test_a_key_cannot_name_a_column_twice(mutate, where):
    # L4
    raw = _model()
    mutate(raw)
    with pytest.raises(DocumentModelError) as exc:
        parse_model(raw)
    assert (exc.value.code, exc.value.where) == ("duplicate_key_column", where)
    assert _codes(_validate(raw)) == ["duplicate_key_column"]


@pytest.mark.parametrize(
    "table, where",
    [
        (None, ""),
        ("customers", ""),
        ({"columns": "customer_id"}, "columns"),
        ({"columns": {"customer_id": 5}}, "columns"),
        ({"columns": [1, 2]}, "columns"),
        ({"columns": {"customer_id": "integer"}, "primary_key": "customer_id"}, "primary_key"),
        ({"columns": {"customer_id": "integer"}, "primary_key": [1]}, "primary_key"),
        ({"columns": {"customer_id": "integer"}, "unique_keys": ["customer_id"]}, "unique_keys"),
        ({"columns": {"customer_id": "integer"}, "unique_keys": [[]]}, "unique_keys"),
        ({"columns": {"customer_id": "integer"}, "unique_keys": "customer_id"}, "unique_keys"),
        ({"columns": {"customer_id": "integer"}, "sampled_unique": ["customer_id"]}, "sampled_unique"),
    ],
)
def test_source_table_shapes_are_read_strictly(table, where):
    # L5: None crashed, a string primary key was split into characters and a
    # flat unique_keys list became one key per character.
    with pytest.raises(DocumentModelError) as exc:
        SourceTable.from_dict(table)
    assert exc.value.code == "bad_source_schema"
    assert exc.value.where == where
    schemas = _schemas()
    schemas["public.customers"] = table
    issues = _validate(schemas=schemas)
    bad = [i for i in issues if i.code == "bad_source_schema"]
    assert len(bad) == 1 and bad[0].severity == ERROR
    assert bad[0].path.startswith("source_schemas['public.customers']")
    # The table is not reported as missing too: one finding, one cause.
    assert "root_source_unknown" not in _codes(issues)


def test_a_flat_unique_key_list_is_never_a_proof():
    # unique_keys=["ab"] used to read as the key ("a", "b") and prove it.
    model = {"collection": "x", "root": {"source": "t", "key": ["a", "b"]}}
    table = {"columns": {"a": "int", "b": "int", "ab": "int"}, "unique_keys": ["ab"]}
    issues = validate_model(model, {"t": table})
    assert has_errors(issues)
    assert _codes(issues) == ["bad_source_schema"]


def test_well_formed_source_tables_still_read():
    table = SourceTable.from_dict(
        {
            "columns": {"a": "int", "b": None},
            "primary_key": ("a",),
            "unique_keys": [("a", "b")],
            "sampled_unique": [],
        }
    )
    assert table.columns == {"a": "int", "b": ""}
    assert table.primary_key == ("a",)
    assert table.unique_keys == (("a", "b"),)
    assert SourceTable.from_dict({"columns": ["a", "b"]}).columns == {"a": "", "b": ""}
    assert SourceTable.from_dict({}).columns == {}


def test_source_schemas_that_are_not_a_mapping_are_one_issue():
    issues = validate_model(_model(), [("public.customers", {})], _fks())
    assert [(i.code, i.path) for i in issues] == [("bad_source_schema", "source_schemas")]


# --- review of PR 417: validation semantics (L1, L2, L3, L6) ------------------


def _employees() -> tuple[dict, dict]:
    schemas = {
        "public.employees": {
            "columns": {"employee_id": "integer", "manager_id": "integer", "name": "text"},
            "primary_key": ["employee_id"],
        }
    }
    model = {
        "collection": "employees",
        "root": {"source": "public.employees", "key": ["employee_id"]},
        "relationships": [
            {
                "id": "manager",
                "parent": "root",
                "source": "public.employees",
                "join": [{"parent": "manager_id", "child": "employee_id"}],
                "rule": "reference",
                "path": "manager",
            }
        ],
    }
    return model, schemas


@pytest.mark.parametrize("rule", ["reference", "collection"])
def test_a_self_reference_is_not_recursion(rule):
    # L1: employees.manager_id -> employees.id is the textbook self
    # reference; only embedding a table inside itself recurses.
    model, schemas = _employees()
    model["relationships"][0]["rule"] = rule
    if rule == "collection":
        model["relationships"][0]["path"] = ""
    assert validate_model(model, schemas) == []


@pytest.mark.parametrize(
    "rule, options",
    [
        ("embed_object", {}),
        ("embed_object", {"merge_into_parent": True}),
        ("embed_array", {}),
    ],
)
def test_embedding_a_table_inside_itself_is_still_recursion(rule, options):
    model, schemas = _employees()
    rel = model["relationships"][0]
    rel.update(rule=rule, options=options, fields=[{"source": "name", "path": "manager_name"}])
    if options.get("merge_into_parent"):
        rel["path"] = ""
    if rule == "embed_array":
        rel["child_key"] = ["employee_id"]
    assert "recursive_source" in _codes(validate_model(model, schemas), ERROR)


def _profiles_schema() -> dict:
    schemas = _schemas()
    schemas["public.profiles"] = {
        "columns": {"customer_id": "integer", "bio": "text"},
        "primary_key": ["customer_id"],
    }
    schemas["public.regions"] = {
        "columns": {"code": "char(2)", "region": "text"},
        "primary_key": ["code"],
    }
    return schemas


def _profile(**over) -> dict:
    rel = {
        "id": "profile",
        "parent": "root",
        "source": "public.profiles",
        "join": [{"parent": "customer_id", "child": "customer_id"}],
        "rule": "embed_object",
        "path": "profile",
        "fields": [{"source": "bio", "path": "bio"}],
    }
    rel.update(over)
    return rel


def test_two_embedded_objects_cannot_share_a_path():
    # L2: country at "country" and a profile at "country" both validated
    # clean, so one document would merge two tables' rows into one object.
    raw = _model()
    raw["relationships"].append(_profile(path="country"))
    issues = _validate(raw, _profiles_schema())
    assert [(i.code, i.path) for i in issues] == [("path_collision", "relationships[4].path")]
    assert issues[0].severity == ERROR
    assert "'country'" in issues[0].message


def test_nothing_else_writes_inside_an_embedded_object():
    raw = _model()
    raw["root"]["fields"].append({"source": "country_code", "path": "country.iso"})
    issues = _validate(raw)
    assert [(i.code, i.path) for i in issues] == [("path_collision", "root.fields[3].path")]
    raw = _model()
    raw["relationships"].append(_profile(path="country.profile"))
    issues = _validate(raw, _profiles_schema())
    assert [(i.code, i.path) for i in issues] == [("path_collision", "relationships[4].path")]
    raw = _model()
    raw["relationships"].append(
        {
            "id": "tags",
            "parent": "root",
            "source": "public.tags",
            "join": [{"parent": "customer_id", "child": "customer_id"}],
            "rule": "embed_array",
            "path": "country.tags",
            "child_key": [],
            "fields": [{"source": "tag"}],
            "options": {"primitive": True},
        }
    )
    assert ("path_collision", "relationships[4].path") in [
        (i.code, i.path) for i in _validate(raw)
    ]


def test_a_merged_object_keeps_its_own_collision_rule():
    raw = _model()
    raw["relationships"].append(
        _profile(path="", fields=[{"source": "bio", "path": "country.bio"}], options={"merge_into_parent": True})
    )
    issues = _validate(raw, _profiles_schema())
    assert [(i.code, i.path) for i in issues] == [
        ("merge_into_parent_collision", "relationships[4].fields[0].path")
    ]


def test_an_embedded_object_owns_what_nests_inside_it():
    # The object's own fields and the relationships nested under it may
    # write inside its path; that is how an object is built.
    raw = _model()
    raw["relationships"].append(
        {
            "id": "region",
            "parent": "country",
            "source": "public.regions",
            "join": [{"parent": "code", "child": "code"}],
            "rule": "embed_object",
            "path": "region",
            "fields": [{"source": "region", "path": "name"}],
        }
    )
    raw["relationships"].append(
        _profile(
            parent="country",
            source="public.profiles",
            join=[{"parent": "code", "child": "bio"}],
            path="",
            fields=[{"source": "bio", "path": "motto"}],
            options={"merge_into_parent": True},
        )
    )
    issues = _validate(raw, _profiles_schema())
    assert "path_collision" not in _codes(issues)
    assert "merge_into_parent_collision" not in _codes(issues)


@pytest.mark.parametrize("name", [".customers", "customers.", "a..b", "."])
def test_collection_names_with_empty_parts_are_refused(name):
    # L3: pymongo refuses these; they used to validate clean.
    raw = _model()
    raw["collection"] = name
    assert _codes(_validate(raw)) == ["collection_name_invalid"]


def test_collection_names_with_dots_inside_are_fine():
    raw = _model()
    raw["collection"] = "sales.customers"
    assert _validate(raw) == []


def test_the_namespace_limit_counts_the_target_database():
    # L3: MongoDB limits "<database>.<collection>" to 255 bytes.
    raw = _model()
    raw["collection"] = "c" * 251
    issues = _validate(raw, database="shop")
    assert [(i.code, i.severity, i.path) for i in issues] == [
        ("collection_namespace_too_long", ERROR, "collection")
    ]
    assert "shop" in issues[0].message
    raw["collection"] = "c" * (255 - len("shop."))
    assert _validate(raw, database="shop") == []


def test_without_a_database_a_long_name_warns_that_the_database_counts():
    raw = _model()
    raw["collection"] = "c" * 200
    issues = _validate(raw)
    assert [(i.code, i.severity) for i in issues] == [("namespace_length_unchecked", WARNING)]
    assert "database" in issues[0].message
    # Short names fit with any database name (at most 63 bytes): no warning.
    raw["collection"] = "c" * 191
    assert _validate(raw) == []
    raw["collection"] = "c" * 256
    assert _codes(_validate(raw)) == ["collection_name_invalid"]


def test_a_foreign_key_alone_does_not_prove_uniqueness():
    # L6: SQLite and MySQL InnoDB accept a foreign key to columns that are
    # not unique, so a foreign key proves nothing unless the referenced
    # columns are also a declared primary or unique key.
    schemas = _schemas()
    schemas["public.countries"]["primary_key"] = []
    issues = _validate(schemas=schemas)
    assert [(i.code, i.severity, i.path) for i in issues] == [
        ("uniqueness_unproven", WARNING, "relationships[3].join")
    ]
    assert "foreign key" in issues[0].message
    schemas["public.countries"]["unique_keys"] = [["code"]]
    assert _validate(schemas=schemas) == []


# --- review of PR 417: limits and cost (M3) -----------------------------------


def _chain(n: int, rule: str = "embed_object") -> tuple[dict, dict]:
    """``n`` relationships, each nested under the one before."""
    tables = {
        f"t{i}": {"columns": {"id": "integer", "pid": "integer", "v": "text"}, "primary_key": ["id"]}
        for i in range(n + 1)
    }
    rels = [
        {
            "id": f"r{i}",
            "parent": "root" if i == 1 else f"r{i - 1}",
            "source": f"t{i}",
            "rule": rule,
            "path": "p",
            "child_key": ["id"],
            # Each row points at one child row by its primary key.
            "join": [{"parent": "pid", "child": "id"}],
        }
        for i in range(1, n + 1)
    ]
    model = {"collection": "c", "root": {"source": "t0", "key": ["id"]}, "relationships": rels}
    return model, tables


def test_at_most_a_thousand_relationships():
    model, _tables = _chain(1000)
    assert len(parse_model(model).relationships) == 1000
    model, _tables = _chain(1001)
    with pytest.raises(DocumentModelError) as exc:
        parse_model(model)
    assert (exc.value.code, exc.value.where) == ("too_many_relationships", "model.relationships")
    assert "1000" in str(exc.value) or "1,000" in str(exc.value)


@pytest.mark.parametrize(
    "mutate, where",
    [
        (lambda m, p: m["root"]["fields"][0].update(path=p), "root.fields[0].path"),
        (lambda m, p: m["relationships"][1].update(path=p), "relationships[1].path"),
        (lambda m, p: m["relationships"][1]["fields"][0].update(path=p), "relationships[1].fields[0].path"),
        (lambda m, p: m["root"]["fields"].append({"source": p}), "root.fields[3].path"),
    ],
)
def test_paths_are_at_most_1024_utf8_bytes(mutate, where):
    raw = _model()
    mutate(raw, "a" * 1024)
    parse_model(raw)
    raw = _model()
    # 513 characters, 1026 bytes: the limit is in bytes, as BSON stores it.
    mutate(raw, "é" * 513)
    with pytest.raises(DocumentModelError) as exc:
        parse_model(raw)
    assert (exc.value.code, exc.value.where) == ("path_too_long", where)


def test_too_deep_is_reported_once_and_nothing_below_is_placed():
    # M3: validation kept descending past the depth cap, reporting every
    # level again and placing ever longer paths.
    model, tables = _chain(8)
    model["limits"] = {"max_depth": 3}
    issues = validate_model(model, tables)
    assert [(i.code, i.path) for i in issues] == [("too_deep", "relationships[3]")]
    # Every other rule still applies below the cap.
    model["relationships"][6]["join"] = [{"parent": "id", "child": "missing"}]
    assert ("column_unknown", "relationships[6].join[0].child") in [
        (i.code, i.path) for i in validate_model(model, tables)
    ]


def _timed(fn) -> float:
    import time

    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def test_a_deep_chain_validates_quickly():
    # M3: an embed_object chain of 1,000 took ~4 s and of 2,000 ~30 s
    # (cubic). Bounds are generous for slow CI; the fixed code takes a few
    # hundredths of a second.
    from services.document_model_validate import run_checks

    model, tables = _chain(1000)
    assert _timed(lambda: validate_model(model, tables)) < 1.0
    # Above the parse limit, the validator's own cost is still linear: build
    # the 2,000-deep chain in code and hand it to the rules directly.
    model, tables = _chain(2000)
    template = parse_model(_chain(1)[0])
    chained = dataclasses.replace(
        template,
        relationships=tuple(
            dataclasses.replace(
                template.relationships[0], id=r["id"], parent=r["parent"], source=r["source"]
            )
            for r in model["relationships"]
        ),
    )
    issues: list = []
    assert _timed(lambda: issues.extend(run_checks(chained, tables, None))) < 1.0
    assert _codes(issues) == ["too_deep"]


def test_a_thousand_sibling_relationships_validate_quickly():
    tables = {"root_t": {"columns": {"id": "integer"}, "primary_key": ["id"]}}
    rels = []
    for i in range(1000):
        tables[f"public.t{i}"] = {
            "columns": {"id": "integer", "v": "text"},
            "primary_key": ["id"],
        }
        rels.append(
            {
                "id": f"r{i}",
                "parent": "root",
                "source": f"public.t{i}",
                "rule": "embed_object",
                "path": f"p{i}",
                "join": [{"parent": "id", "child": "id"}],
            }
        )
    model = {"collection": "c", "root": {"source": "root_t", "key": ["id"]}, "relationships": rels}
    issues: list = []
    assert _timed(lambda: issues.extend(validate_model(model, tables))) < 3.0
    assert issues == []


# --- re-review of PR 417: BSON nesting, repeated columns, database name -------


def _deep(n: int, leaf: str = "x") -> str:
    """A dotted path of ``n`` segments."""
    return ".".join(["d"] * (n - 1) + [leaf])


@pytest.mark.parametrize(
    "mutate, where, ok, too_deep",
    [
        # A top-level path of n segments is n levels deep.
        (lambda m, n: m["root"]["fields"].append({"source": "name", "path": _deep(n)}), "root.fields[3].path", 100, 101),
        # embed_object: its fields sit one level below its path.
        (lambda m, n: _rel(m, "country")["fields"][0].update(path=_deep(n - 1)), "relationships[3].fields[0].path", 100, 101),
        # embed_array adds two levels (the array and the element document).
        (lambda m, n: _rel(m, "addresses")["fields"][0].update(path=_deep(n - 2)), "relationships[0].fields[0].path", 100, 101),
        # ... at every level: items' elements sit inside orders' elements.
        (lambda m, n: _rel(m, "items")["fields"][0].update(path=_deep(n - 4)), "relationships[2].fields[0].path", 100, 101),
    ],
)
def test_mongodb_nesting_limit_of_100_levels(mutate, where, ok, too_deep):
    # Re-review: a 101-segment root field validated clean, though MongoDB
    # refuses documents nested more than 100 levels.
    raw = _model()
    mutate(raw, ok)
    assert _validate(raw) == []
    raw = _model()
    mutate(raw, too_deep)
    issues = _validate(raw)
    assert [(i.code, i.severity, i.path) for i in issues] == [("nesting_too_deep", ERROR, where)]
    assert "100" in issues[0].message


def test_merged_fields_add_no_level():
    raw = _model()
    _rel(raw, "country").update(
        path="", fields=[{"source": "name", "path": _deep(100, "cn")}], options={"merge_into_parent": True}
    )
    assert _validate(raw) == []
    _rel(raw, "country")["fields"][0]["path"] = _deep(101, "cn")
    assert _codes(_validate(raw)) == ["nesting_too_deep"]


def test_an_object_path_with_no_room_for_its_fields_is_one_finding():
    raw = _model()
    _rel(raw, "country")["path"] = _deep(100, "country")
    assert [(i.code, i.path) for i in _validate(raw)] == [("nesting_too_deep", "relationships[3].path")]


def test_a_512_segment_path_is_refused_quickly():
    raw = _model()
    raw["root"]["fields"].append({"source": "name", "path": _deep(512)})
    assert _codes(_validate(raw)) == ["nesting_too_deep"]


def test_wide_models_with_deep_paths_validate_quickly():
    # Re-review: collision and ownership checks were O(segments^2) per path
    # (100 objects x 100 fields x 300 segments took ~37 s). Paths are now
    # capped at 100 levels and prefixes are walked without re-joining.
    cols = {f"c{j}": "text" for j in range(50)}
    cols["id"] = "integer"
    tables = {"p": {"columns": cols, "primary_key": ["id"]}}
    rels = []
    for i in range(1000):
        tables[f"k{i}"] = {"columns": cols, "primary_key": ["id"]}
        array = i % 2 == 0
        rels.append(
            {
                "id": f"r{i}",
                "parent": "root",
                "source": f"k{i}",
                "rule": "embed_array" if array else "embed_object",
                "path": f"o{i}",
                "child_key": ["id"] if array else [],
                "join": [{"parent": "id", "child": "id"}],
                # 100 levels exactly: the deepest a document may go.
                "fields": [
                    {"source": f"c{j}", "path": _deep(98 if array else 99, f"f{j}")}
                    for j in range(50)
                ],
            }
        )
    model = {
        "collection": "c",
        "root": {
            "source": "p",
            "key": ["id"],
            "fields": [{"source": f"c{j}", "path": _deep(99, f"r{j}")} for j in range(50)],
        },
        "relationships": rels,
    }
    issues: list = []
    assert _timed(lambda: issues.extend(validate_model(model, tables))) < 20.0
    assert issues == []


@pytest.mark.parametrize(
    "mutate, code, where",
    [
        (
            lambda m: _rel(m, "orders")["options"].update(order_by=[["ordered_at", "desc"], ["ordered_at", "asc"]]),
            "duplicate_order_by_column",
            "relationships[1].options.order_by[1]",
        ),
        (
            lambda m: _rel(m, "orders")["join"].append({"parent": "customer_id", "child": "customer_id"}),
            "duplicate_join_pair",
            "relationships[1].join[1]",
        ),
    ],
)
def test_repeated_columns_outside_keys_are_refused(mutate, code, where):
    raw = _model()
    mutate(raw)
    with pytest.raises(DocumentModelError) as exc:
        parse_model(raw)
    assert (exc.value.code, exc.value.where) == (code, where)
    assert _codes(_validate(raw)) == [code]


@pytest.mark.parametrize(
    "database",
    ["a.b", "a$b", "a/b", "a\\b", 'a"b', "a b", "a*b", "a<b", "a>b", "a:b", "a|b", "a?b", "a\x00b",
     " ", "   ", "d" * 64, "\ud800"],
)
def test_the_target_database_name_follows_mongodb_rules(database):
    # Re-review: these passed, and a whitespace-only name was stripped to
    # nothing, which skipped the namespace check.
    issues = _validate(database=database)
    assert [(i.code, i.severity, i.path) for i in issues] == [("database_name_invalid", ERROR, "database")]
    issues[0].message.encode("utf-8")


def test_valid_database_names_pass():
    for database in ("shop", "d" * 63, "Sales_2026-eu", "café"):
        assert _validate(database=database) == []
    assert _validate(database="") == [] and _validate(database=None) == []


def test_paths_far_over_the_nesting_limit_are_refused_quickly():
    # Re-review probe (perf_obj 100 100 300 took ~37 s): many objects whose
    # fields are 300 levels deep. Each is now one finding, found quickly.
    cols = {f"c{j}": "text" for j in range(30)}
    cols["id"] = "integer"
    tables = {"p": {"columns": cols, "primary_key": ["id"]}}
    deep = ".".join(["s"] * 300)
    rels = []
    for i in range(100):
        tables[f"k{i}"] = {"columns": cols, "primary_key": ["id"]}
        rels.append(
            {
                "id": f"r{i}",
                "parent": "root",
                "source": f"k{i}",
                "rule": "embed_object",
                "path": f"o{i}",
                "join": [{"parent": "id", "child": "id"}],
                "fields": [{"source": f"c{j}", "path": f"{deep}.f{j}"} for j in range(30)],
            }
        )
    model = {"collection": "c", "root": {"source": "p", "key": ["id"]}, "relationships": rels}
    issues: list = []
    assert _timed(lambda: issues.extend(validate_model(model, tables))) < 2.0
    assert set(_codes(issues)) == {"nesting_too_deep"}
    assert len(issues) == 100 * 30
