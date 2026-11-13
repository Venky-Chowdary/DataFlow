"""Document model: parse (strict), validate (every rule), hash (canonical)."""

from __future__ import annotations

import copy
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


def test_embed_array_parent_uniqueness_from_a_foreign_key():
    schemas = _schemas()
    schemas["public.orders"]["primary_key"] = []
    # A foreign key can only reference a unique key, so items' FK onto
    # orders.order_id proves order_id unique on orders: it serves both as the
    # parent proof for items and as the child_key proof for orders.
    assert _validate(schemas=schemas) == []
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


def test_embed_object_uniqueness_from_the_parents_foreign_key():
    schemas = _schemas()
    schemas["public.countries"]["primary_key"] = []
    assert _validate(schemas=schemas) == []


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
