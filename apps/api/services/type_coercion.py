from __future__ import annotations
from typing import Any, Mapping

from services import type_system as _ts
from services.dest_dialect_facts import dest_has_no_uuid_type
from services.document_instant import document_instant_wire_preserved
from services.source_engine_scope import active_source_engine, bind_source_engine
from services.type_system import (
    LOGICAL_ARRAY,
    LOGICAL_BINARY,
    LOGICAL_BOOLEAN,
    LOGICAL_DATE,
    LOGICAL_DATETIME,
    LOGICAL_DECIMAL,
    LOGICAL_FLOAT,
    LOGICAL_INTEGER,
    LOGICAL_JSON,
    LOGICAL_MAP,
    LOGICAL_OBJECTID,
    LOGICAL_STRING,
    LOGICAL_STRUCT,
    LOGICAL_TEXT,
    LOGICAL_TIME,
    LOGICAL_UUID,
    LOGICAL_VECTOR,
    PRECISION_COLLAPSE_PAIRS,
    _normalize_dest_db,
    accent_polarity_invent,
    bfile_locator_would_collapse,
    bignumeric_capacity_would_invent,
    binary_width_would_narrow,
    bitstring_opaque_bytes_collapse,
    bitstring_pad_polarity_loss,
    bitstring_width_would_narrow,
    blocking_string_width_narrowing,
    bounded_string_sink_would_truncate,
    case_fold_polarity_invent,
    code_page_sink_would_collapse,
    date_to_tz_aware_invent,
    decfloat_domain_would_collapse,
    decimal_fixed_point_would_collapse_to_text,
    decimal_params_would_narrow,
    dest_string_length_is_unenforced,
    document_domain_would_collapse,
    document_domain_would_invent,
    enum_domain_would_collapse,
    enum_set_domain_would_reject,
    fixed_width_pad_polarity_loss,
    float_mantissa_would_narrow,
    generated_always_overwrite_risk,
    geography_contract_would_collapse,
    identity_domain_would_invent,
    identity_polarity_would_collapse,
    integer_width_would_narrow,
    interval_family_would_collapse,
    interval_precision_would_narrow,
    is_bitstring_carrier,
    is_dialect_native_document_wire,
    is_fixed_width_char_carrier,
    is_nested_document_collapse,
    is_nested_shape_collapse,
    is_timezone_polarity_loss,
    is_unlimited_string_carrier,
    kana_fold_polarity_invent,
    long_raw_locator_would_collapse,
    money_domain_would_collapse,
    national_charset_would_collapse,
    national_charset_would_invent,
    nested_struct_fields_incompatible,
    nested_to_native_document_wire_preserved,
    normalize_logical_type,
    objectid_would_collapse,
    oracle_char_byte_polarity_loss,
    oracle_long_numeric_invent,
    parse_numeric_precision_scale,
    parse_string_carrier_width,
    rowversion_would_collapse_to_temporal,
    set_to_array_polarity_preserved,
    smalldatetime_domain_would_invent,
    specialty_carrier_base,
    specialty_carrier_would_collapse,
    specialty_domain_would_invent,
    specialty_polarity_mismatch,
    specialty_wire_preserves_value,
    sql_variant_would_collapse,
    string_width_would_narrow,
    temporal_precision_would_narrow,
    time_timezone_polarity_loss,
    timezone_aware_would_collapse_to_string,
    unsigned_integer_would_overflow,
    unsigned_signed_polarity_invent,
    uuid_carrier_is_dialect_equivalent,
    uuid_exact_wire_carrier,
    uuid_would_collapse,
    vector_dim_mismatch,
    vector_encoding_would_collapse,
    vector_to_array_wire_preserved,
    width_fold_polarity_invent,
    year_domain_would_collapse,
)


def _blocking_width(
    source: str | None,
    target: str | None,
    source_type: str,
    target_type: str,
    *,
    dest_db: str,
    proven: Mapping[tuple[str, str], Any] | None,
) -> bool:
    if source is None or target is None:
        return string_width_would_narrow(source_type, target_type, dest_db=dest_db)
    return blocking_string_width_narrowing(
        source,
        target,
        source_type,
        target_type,
        dest_db=dest_db,
        proven=proven,
    )


def _width_proven(
    source: str | None,
    target: str | None,
    proven: Mapping[tuple[str, str], Any] | None,
) -> bool:
    return (
        source is not None
        and target is not None
        and (str(source).casefold(), str(target).casefold()) in (proven or {})
    )


def is_precision_collapse_coercion(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
    dest_table_exists: bool | None = None,
    population: Any = None,
    source: str | None = None,
    target: str | None = None,
    proven: Mapping[tuple[str, str], Any] | None = None,
) -> bool:
    """True when source→target collapses precision even if samples appear clean.

    Used by DDL compatibility, schema-drift, G3, and integrity sample soft-pass so
    Airbyte-style "head sample coerced → green" never hides float→decimal /
    datetime→date / timestamptz→timestamp_ntz / DECIMAL(p,s) / VARCHAR width /
    BINARY width / UNSIGNED→signed overflow / ENUM·SET domain shrink /
    INTERVAL YM↔DS / GEOGRAPHY SRID·polarity / GENERATED ALWAYS overwrite /
    TIME·TIMESTAMP fractional-second (p) narrow.

    ``dest_db`` threads dialect defaults (document wires, bare TIMESTAMP FSP).
    """
    dest_db = _normalize_dest_db(dest_db) if dest_db else ""
    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type)
    if document_instant_wire_preserved(
        source_type, target_type, dest_db=dest_db, population=population
    ):
        # A document store's ``date`` keeps the time of day; only sub-millisecond
        # precision is lost, and a source holding that much (declared, or an
        # engine default, and not measured to whole milliseconds) is excluded
        # so it still reports below. Without this the (datetime, date) pair read
        # as dropping the clock and demoted every timestamp mapping.
        return False
    if _ts._dynamodb_number_wire_preserves(source_type, target_type, dest_db=dest_db):
        # AttributeValue N is an unbounded decimal, not DECIMAL(p,s). A float
        # lands as Decimal(str(value)) — the engine's only numeric carrier.
        return False
    if (src, tgt) in PRECISION_COLLAPSE_PAIRS:
        return True
    if is_timezone_polarity_loss(source_type, target_type, dest_db=dest_db):
        return True
    if time_timezone_polarity_loss(source_type, target_type):
        return True
    if timezone_aware_would_collapse_to_string(
        source_type, target_type, dest_db=dest_db
    ):
        return True
    if long_raw_locator_would_collapse(source_type, target_type):
        return True
    if decfloat_domain_would_collapse(source_type, target_type):
        return True
    if bignumeric_capacity_would_invent(
        source_type,
        target_type,
        dest_db=dest_db,
        dest_table_exists=dest_table_exists,
    ):
        return True
    if decimal_fixed_point_would_collapse_to_text(
        source_type, target_type, dest_db=dest_db
    ):
        return True
    if smalldatetime_domain_would_invent(source_type, target_type):
        return True
    if decimal_params_would_narrow(source_type, target_type, dest_db=dest_db):
        return True
    if _blocking_width(
        source,
        target,
        source_type,
        target_type,
        dest_db=dest_db,
        proven=proven,
    ):
        return True
    if bounded_string_sink_would_truncate(
        source_type, target_type, dest_db=dest_db
    ) and not _width_proven(source, target, proven):
        return True
    if national_charset_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if national_charset_would_invent(source_type, target_type, dest_db=dest_db):
        return True
    if code_page_sink_would_collapse(
        source_type, target_type, dest_db=dest_db, population=population
    ):
        return True
    if fixed_width_pad_polarity_loss(source_type, target_type, dest_db=dest_db):
        return True
    if binary_width_would_narrow(source_type, target_type):
        return True
    if bitstring_width_would_narrow(source_type, target_type):
        return True
    if unsigned_integer_would_overflow(source_type, target_type):
        return True
    if unsigned_signed_polarity_invent(source_type, target_type):
        return True
    if integer_width_would_narrow(source_type, target_type, dest_db=dest_db):
        return True
    if float_mantissa_would_narrow(source_type, target_type, dest_db=dest_db):
        return True
    if year_domain_would_collapse(source_type, target_type):
        return True
    if money_domain_would_collapse(source_type, target_type):
        return True
    if bitstring_opaque_bytes_collapse(source_type, target_type):
        return True
    if enum_set_domain_would_reject(source_type, target_type):
        return True
    if enum_domain_would_collapse(source_type, target_type):
        return True
    if interval_family_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if interval_precision_would_narrow(source_type, target_type):
        return True
    if bitstring_pad_polarity_loss(source_type, target_type):
        return True
    if oracle_char_byte_polarity_loss(source_type, target_type):
        return True
    if oracle_long_numeric_invent(source_type, target_type):
        return True
    if geography_contract_would_collapse(source_type, target_type):
        return True
    if specialty_carrier_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if specialty_domain_would_invent(
        source_type,
        target_type,
        dest_db=dest_db,
        dest_table_exists=dest_table_exists,
    ):
        return True
    if specialty_polarity_mismatch(source_type, target_type):
        return True
    if case_fold_polarity_invent(source_type, target_type):
        return True
    if accent_polarity_invent(source_type, target_type):
        return True
    if width_fold_polarity_invent(source_type, target_type):
        return True
    if kana_fold_polarity_invent(source_type, target_type):
        return True
    if date_to_tz_aware_invent(source_type, target_type):
        return True
    if vector_dim_mismatch(source_type, target_type):
        return True
    if vector_encoding_would_collapse(source_type, target_type):
        return True
    if rowversion_would_collapse_to_temporal(source_type, target_type):
        return True
    if sql_variant_would_collapse(source_type, target_type):
        return True
    # ObjectId → VARCHAR(24)/BINARY(12) is the industry create-new wire — not lossy.
    if (
        normalize_logical_type(source_type) == LOGICAL_OBJECTID
        and specialty_wire_preserves_value("OBJECTID", target_type)
    ):
        return False
    if uuid_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if objectid_would_collapse(source_type, target_type):
        return True
    if generated_always_overwrite_risk(target_type):
        return True
    if identity_polarity_would_collapse(source_type, target_type):
        return True
    if identity_domain_would_invent(source_type, target_type):
        return True
    if bfile_locator_would_collapse(source_type, target_type):
        return True
    if document_domain_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if document_domain_would_invent(
        source_type,
        target_type,
        dest_db=dest_db,
        dest_table_exists=dest_table_exists,
    ):
        return True
    if temporal_precision_would_narrow(
        source_type, target_type, dest_db=dest_db, population=population
    ):
        return True
    return False


def assess_bson_affinity(
    source_type: str,
    target_type: str,
    *,
    destination_db_type: str = "",
) -> list[dict]:
    """Schemaless G3 semantic contract — type affinity when DDL is absent.

    MongoDB / DynamoDB / Redis do not enforce column DDL, but declared (or
    inferred) carriers still have BSON/AttributeValue affinity. ObjectId↛NUMBER
    and DECIMAL↛INTEGER must surface before write — never silent SKIP-as-green.
    """
    risks: list[dict] = []
    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type) if (target_type or "").strip() else src
    db = (destination_db_type or "").strip().lower()

    hard = {
        (LOGICAL_OBJECTID, LOGICAL_INTEGER),
        (LOGICAL_OBJECTID, LOGICAL_DECIMAL),
        (LOGICAL_OBJECTID, LOGICAL_FLOAT),
        (LOGICAL_OBJECTID, LOGICAL_BOOLEAN),
        (LOGICAL_UUID, LOGICAL_INTEGER),
        (LOGICAL_UUID, LOGICAL_DECIMAL),
        (LOGICAL_BINARY, LOGICAL_INTEGER),
        (LOGICAL_BINARY, LOGICAL_DECIMAL),
        (LOGICAL_JSON, LOGICAL_INTEGER),
        (LOGICAL_JSON, LOGICAL_DECIMAL),
        (LOGICAL_STRUCT, LOGICAL_INTEGER),
        (LOGICAL_ARRAY, LOGICAL_INTEGER),
    }
    if (src, tgt) in hard:
        risks.append({
            "kind": "bson_affinity_block",
            "severity": "block",
            "message": (
                f"Schemaless affinity: {source_type or src} → {target_type or tgt}"
                + (f" on {db}" if db else "")
                + " invents a numeric/scalar domain the source never had."
            ),
        })
        return risks

    soft = {
        (LOGICAL_DECIMAL, LOGICAL_INTEGER),
        (LOGICAL_FLOAT, LOGICAL_INTEGER),
        (LOGICAL_FLOAT, LOGICAL_DECIMAL),
        (LOGICAL_DECIMAL, LOGICAL_FLOAT),
        (LOGICAL_DATETIME, LOGICAL_DATE),
        (LOGICAL_STRING, LOGICAL_INTEGER),
        (LOGICAL_STRING, LOGICAL_DECIMAL),
        (LOGICAL_STRING, LOGICAL_BOOLEAN),
        (LOGICAL_TEXT, LOGICAL_INTEGER),
        (LOGICAL_TEXT, LOGICAL_DECIMAL),
        # Specialty / domain → open string invents no validation at schemaless sinks.
        (LOGICAL_UUID, LOGICAL_STRING),
        (LOGICAL_UUID, LOGICAL_TEXT),
        # ObjectId → unbounded TEXT/STRING hex wire is value-preserving (not soft risk).
        (LOGICAL_BINARY, LOGICAL_STRING),
        (LOGICAL_BINARY, LOGICAL_TEXT),
        (LOGICAL_JSON, LOGICAL_STRING),
        (LOGICAL_JSON, LOGICAL_TEXT),
        (LOGICAL_VECTOR, LOGICAL_ARRAY),
        (LOGICAL_VECTOR, LOGICAL_STRING),
        (LOGICAL_VECTOR, LOGICAL_TEXT),
    }
    specialty_to_open = (
        specialty_carrier_base(source_type) is not None
        and normalize_logical_type(target_type or "") in {
            LOGICAL_STRING, LOGICAL_TEXT, LOGICAL_JSON, LOGICAL_ARRAY,
        }
        and specialty_carrier_base(target_type) is None
        and not specialty_wire_preserves_value(
            specialty_carrier_base(source_type) or "", target_type or ""
        )
    )
    # An offset-bearing datetime onto a document store's single instant carrier
    # keeps the instant; the (datetime, date) pair is the token's name only.
    instant_kept = document_instant_wire_preserved(
        source_type, target_type or source_type, dest_db=db
    )
    if (
        ((src, tgt) in soft and not instant_kept)
        or objectid_would_collapse(source_type, target_type or source_type)
        or specialty_to_open
    ):
        risks.append({
            "kind": "bson_affinity_warn",
            "severity": "warn",
            "message": (
                f"Schemaless affinity risk: {source_type or src} → {target_type or tgt}"
                + (f" on {db}" if db else "")
                + ". No DDL contract — confirm samples before write."
            ),
        })
    return risks


def assess_create_new_type_risk(
    source_type: str,
    target_type: str,
    *,
    destination_db_type: str = "",
    samples: list | None = None,
    source_types_declared: bool = False,
) -> list[dict]:
    """Risk chips for create-new columns before Validate/write.

    Returns a list of ``{kind, severity, message}`` — empty when lossless.

    ``source_types_declared`` is True when the source handed us real DDL. A
    declared DOUBLE already is the IEEE float; its long fractional digits are
    the value, not residue left by an Excel/CSV export, so the
    ``ieee_float_artifact`` chip (which asks the operator to reconsider an
    *invented* FLOAT) does not apply (D161).
    """
    risks: list[dict] = []
    src = (source_type or "").strip() or "VARCHAR"
    tgt = (target_type or "").strip() or src
    db = (destination_db_type or "").strip().lower()

    if samples and not source_types_declared:
        from services.decimal_observe import (
            ieee_float_create_new_risk,
            observe_numeric_samples,
        )

        ieee = ieee_float_create_new_risk(observe_numeric_samples(samples))
        if ieee:
            risks.append(ieee)

    from services.conversion_contract import invents_unproven_capacity

    if invents_unproven_capacity(src, tgt, dest_db=db):
        _sp, src_scale = parse_numeric_precision_scale(src)
        _tp, dest_scale = parse_numeric_precision_scale(tgt)
        declared_scale = 0 if src_scale is None else int(src_scale)
        if _sp is None and src_scale is None and normalize_logical_type(src) == LOGICAL_DECIMAL:
            # A bare NUMERIC declares no bound, so no carrier is "invented"
            # from it: the engine's unbounded exact decimal is the declared
            # carrier, sized by declaration rather than by today's rows (D68).
            # A value past it is refused at write, never rounded silently.
            risks.append({
                "kind": "unbounded_decimal_carrier",
                "severity": "warn",
                "message": (
                    f"Source declares an unbounded {src}; created as {tgt}, the "
                    f"exact decimal carrier for {db or 'this destination'}. A "
                    "value beyond its precision or scale is refused at write, "
                    "never rounded."
                ),
            })
        elif (
            dest_scale is not None
            and int(dest_scale) > declared_scale
            and not any(r.get("kind") == "invented_decimal_scale" for r in risks)
        ):
            # NUMBER(38,10) / DECIMAL(38,15) floors invent zeros after the
            # decimal the source never declared. Trailing zeros do not change
            # the value; CREATE still invented the shape.
            risks.append({
                "kind": "invented_decimal_scale",
                "severity": "warn",
                "message": (
                    f"Create-new {src} → {tgt} invents destination scale "
                    f"{int(dest_scale)} the source never declared. Zeros after "
                    "the decimal do not change the value, but CREATE invented "
                    "that scale. Use a declared typmod or observed samples — "
                    "Accept · Risk Contract before Execute if this shape is intended."
                ),
            })

    # The samples are the measured population: a declared DATETIME(6) whose
    # cells hold whole milliseconds loses nothing on a BSON / Elasticsearch
    # date. Judging the declaration alone stamped a blocking chip G3 then
    # cleared, parking the mapping at 0.70 under the G4 floor where no
    # declared zone or signed contract could lift it (D148/D156).
    if is_precision_collapse_coercion(src, tgt, dest_db=db, population=samples):
        risks.append({
            "kind": "precision_collapse",
            "severity": "block",
            "message": (
                f"Create-new {src} → {tgt} collapses precision"
                + (f" on {db}" if db else "")
                + ". Remap or Accept · Risk Contract before Execute."
            ),
        })
    elif is_lossy_coercion(src, tgt, dest_db=db, population=samples):
        risks.append({
            "kind": "lossy_coercion",
            "severity": "block",
            "message": (
                f"Create-new {src} → {tgt} may lose information"
                + (f" on {db}" if db else "")
                + ". Remap or Accept · Risk Contract before Execute."
            ),
        })

    # Dialect VARCHAR capacity ceilings that create-new often hit.
    src_w = parse_string_carrier_width(src)
    tgt_w = parse_string_carrier_width(tgt)
    dialect_cap = {
        "oracle": 4000,
        "sqlserver": 8000,
        "mssql": 8000,
        "redshift": 65535,
        "mysql": 16383,  # utf8mb4 InnoDB row practical ceiling for VARCHAR
    }.get(db)
    # Unbounded TEXT/CLOB/MAX is the honest create-new escape — no width-cap chip.
    tgt_unlimited = is_unlimited_string_carrier(tgt)
    if (
        dialect_cap
        and (src_w or 0) > dialect_cap
        and not tgt_unlimited
        and (tgt_w is not None and tgt_w <= dialect_cap)
    ):
        risks.append({
            "kind": "varchar_width_cap",
            "severity": "block",
            "message": (
                f"Source width {src_w} exceeds {db} VARCHAR capacity (~{dialect_cap}) "
                f"and create-new stamped bounded {tgt}. Use CLOB/TEXT/MAX or Accept · Risk Contract."
            ),
        })
    if (
        dest_string_length_is_unenforced(db)
        and is_fixed_width_char_carrier(src)
        and normalize_logical_type(tgt) in {LOGICAL_STRING, LOGICAL_TEXT}
    ):
        risks.append({
            "kind": "fixed_width_not_enforced",
            "severity": "warn",
            "message": (
                f"Source {src} declares a fixed width; {db or 'this engine'} stores "
                "every text carrier as untyped TEXT, so the value lands exactly as "
                "read but no padding or length is enforced at the destination."
            ),
        })
    if src_w and tgt_w and tgt_w < src_w:
        risks.append({
            "kind": "varchar_narrow",
            "severity": "block",
            "message": (
                f"Create-new narrows VARCHAR({src_w}) → VARCHAR({tgt_w}). "
                "Widen DDL or Accept · Risk Contract before Execute."
            ),
        })

    if is_timezone_polarity_loss(src, tgt, dest_db=db) or time_timezone_polarity_loss(src, tgt):
        risks.append({
            "kind": "timezone_polarity",
            "severity": "warn",
            "message": f"Create-new drops timezone polarity: {src} → {tgt}.",
        })
    else:
        from services.timezone_policy import (
            MYSQL_TIMESTAMP_RANGE_TEXT,
            instant_range_would_cap,
            samples_outside_instant_range,
        )

        if instant_range_would_cap(src, tgt, dest_db=db):
            # The carrier keeps the instant, so this is not a polarity loss and
            # not a Risk Contract — but its domain is 68 years wide, and a row
            # outside it is rejected at the write, not at Map.
            outside = samples_outside_instant_range(samples)
            risks.append({
                "kind": "instant_range_cap",
                "severity": "block" if outside else "warn",
                "message": (
                    f"Create-new {src} → {tgt} keeps the instant but caps its range "
                    f"to {MYSQL_TIMESTAMP_RANGE_TEXT}"
                    + (
                        f"; sampled value(s) outside it: {', '.join(outside[:3])}. "
                        if outside
                        else ". "
                    )
                    + "Use DATETIME(6) with a UTC-normalize contract for the full "
                    "1000..9999 range, or accept that out-of-range rows quarantine."
                ),
            })
    if uuid_would_collapse(src, tgt, dest_db=db):
        risks.append({
            "kind": "uuid_domain",
            "severity": "warn",
            "message": (
                f"Create-new stores UUID as {tgt}"
                + (f" on {db}" if db else "")
                + " — UUID domain is not enforced at destination."
            ),
        })
    elif uuid_carrier_is_dialect_equivalent(src, tgt, dest_db=db):
        # The dialect has one untyped text carrier, so this is the widest wire it
        # can spell — stated as a note, never a refusal the operator cannot act on.
        risks.append({
            "kind": "uuid_carrier_equivalent",
            "severity": "warn",
            "message": (
                f"Create-new stores UUID as {tgt}"
                + (f" on {db}" if db else "")
                + " — every text carrier on this engine is the same untyped TEXT, "
                "so values round-trip exactly but the UUID domain is not enforced."
            ),
        })
    elif (
        normalize_logical_type(src) == LOGICAL_UUID
        and normalize_logical_type(tgt) != LOGICAL_UUID
        and uuid_exact_wire_carrier(tgt)
    ):
        # Exact CHAR/VARCHAR(36) wire preserves values but invents no UUID type —
        # Accept risk so Map never looks UUID→UUID while CREATE emits string DDL.
        # An engine with no UUID type at all leaves the operator nothing to
        # remap to, so there the chip is stated, not held (D72).
        typeless = dest_has_no_uuid_type(db)
        risks.append({
            "kind": "uuid_carrier_equivalent" if typeless else "uuid_domain",
            "severity": "warn",
            "message": (
                f"Create-new stores UUID as {tgt}"
                + (f" on {db}" if db else "")
                + (
                    " — the engine has no UUID type, so this exact 36-char wire is "
                    "its strictest carrier; values round-trip exactly but the UUID "
                    "domain is not enforced."
                    if typeless
                    else " — exact 36-char wire; UUID domain is not enforced at destination."
                )
            ),
        })
    if objectid_would_collapse(src, tgt):
        risks.append({
            "kind": "objectid_domain",
            "severity": "warn",
            "message": (
                f"Create-new stores ObjectId as {tgt}"
                + (f" on {db}" if db else "")
                + " — ObjectId domain is not enforced at destination."
            ),
        })
    elif (
        normalize_logical_type(src) == LOGICAL_OBJECTID
        and normalize_logical_type(tgt) != LOGICAL_OBJECTID
        and specialty_wire_preserves_value("OBJECTID", tgt)
    ):
        # VARCHAR(24) / BINARY(12) keeps every ObjectId byte. Postgres has no
        # ObjectId type, so this note must not lock Map the way a real collapse
        # does — that lock blocked every Mongo collection, including plain
        # scalars, as an unnamed mapping-confidence root (E1-031).
        risks.append({
            "kind": "objectid_carrier_equivalent",
            "severity": "warn",
            "message": (
                f"Create-new stores ObjectId as {tgt}"
                + (f" on {db}" if db else "")
                + " — hex/binary wire; ObjectId domain is not enforced at destination."
            ),
        })
    return risks


def _offset_text_preserves_instant(
    source_type: str,
    target_type: str,
    population: Any,
) -> bool:
    """Text whose every sample already carries an offset is an instant.

    SQLite stores ``2024-01-01T05:00:00+05:30`` in TEXT. Mapping that column
    onto TIMESTAMPTZ parses the instant; it does not invent one (QA R2-014).
    A naive string still needs ``source_timezone`` and stays lossy.
    """
    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type)
    if src not in {LOGICAL_STRING, LOGICAL_TEXT} or tgt != LOGICAL_DATETIME:
        return False
    if _ts.datetime_timezone_polarity(target_type) not in {"tz", "ltz"}:
        return False
    if not isinstance(population, (list, tuple)) or not population:
        return False
    from services.schema_inference import classify_value

    saw = False
    for raw in population:
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        saw = True
        if classify_value(text) != "TIMESTAMPTZ":
            return False
    return saw


def _measured_text_preserves_scalar(
    source_type: str,
    target_type: str,
    population: Any,
    *,
    dest_db: str = "",
    dest_table_exists: bool | None = None,
) -> bool:
    """True when file text binds onto an existing Postgres number without loss.

    A second full overwrite reads the same CSV as VARCHAR against the INTEGER
    or NUMERIC column the first run created. Unmeasured string→number stays
    lossy. Declared TEXT→NUMBER on other engines, and a create-new stamp,
    stay a collapse even when the sample happens to be numeric.
    """
    if dest_table_exists is not True:
        return False
    if (dest_db or "").strip().lower() not in {"postgresql", "postgres"}:
        return False
    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type)
    if src not in {LOGICAL_STRING, LOGICAL_TEXT}:
        return False
    if tgt not in {LOGICAL_INTEGER, LOGICAL_DECIMAL}:
        return False
    if not isinstance(population, (list, tuple)) or not population:
        return False
    from decimal import Decimal

    from connectors.sql_bind import normalize_sql_bind_value
    from services.transform_engine import decimal_wire_value

    saw = False
    for raw in population:
        if raw is None:
            continue
        text = str(raw).strip()
        if not text or text.lower() in {"null", "none"}:
            continue
        saw = True
        number = decimal_wire_value(text)
        if number is None:
            return False
        if tgt == LOGICAL_INTEGER and number != number.to_integral_value():
            return False
        try:
            bound = normalize_sql_bind_value(text, target_type, engine=dest_db or "")
        except Exception:
            return False
        if isinstance(bound, bool) or not isinstance(bound, (int, Decimal)):
            return False
        if Decimal(bound) != number:
            return False
    return saw


def is_lossy_coercion(
    source_type: str,
    target_type: str,
    *,
    dest_db: str = "",
    source_db: str = "",
    dest_table_exists: bool | None = None,
    population: Any = None,
    source: str | None = None,
    target: str | None = None,
    proven: Mapping[tuple[str, str], Any] | None = None,
) -> bool:
    """True when converting sourceâ†’target may lose precision, fail silently, or
    change the semantic meaning of a value.

    ``source_db`` is the engine the values come from. The charset, timestamp
    and LONG checks read it from :func:`active_source_engine`, which is empty
    outside a transfer, so a caller that knows the engine binds it here: an
    unbound PostgreSQL ``VARCHAR`` → SQL Server ``NVARCHAR`` graded as an
    invented national charset and blocked run 2 into DataFlow's own table (D158).

    The allow-list below captures the widening / reversible conversions that the
    transform engine can perform without losing the original value:

      * any value â†’ string/text/json/array (structural serialization)
      * integer â†’ decimal/string/text/json (integerâ†’float is lossy â€” IEEE mantissa)
      * decimal â†’ string/text/json (decimalâ†’float is lossy)
      * float â†’ string/text/json (floatâ†’decimal and floatâ†’integer are lossy)
      * boolean â†’ string/text/json/integer/decimal/float
      * date â†’ datetime/string/text/json
      * datetime/time â†’ string/text/json
      * json/array â†’ string/text/json/array
      * string/text/uuid/json/array → binary is lossy: base64-decoding
        changes the bytes, so it is not a preserve
      * binary → string/text/json (base64 text of the bytes) is a rendering

    Everything else is considered lossy and should be surfaced in preflight.

    Note ``uuid`` -> string/text/json is value-preserving but is still reported
    lossy: :func:`uuid_would_collapse` treats the lost UUID *domain constraint*
    as an operator-visible collapse rather than silent green.
    """
    engine = (source_db or "").strip().lower()
    if engine and engine != active_source_engine():
        with bind_source_engine(engine):
            return is_lossy_coercion(
                source_type,
                target_type,
                dest_db=dest_db,
                dest_table_exists=dest_table_exists,
                population=population,
                source=source,
                target=target,
                proven=proven,
            )
    dest_db = _normalize_dest_db(dest_db) if dest_db else ""
    src = normalize_logical_type(source_type)
    tgt = normalize_logical_type(target_type)
    if src == tgt:
        # Same logical family can still drop TZ polarity, DECIMAL params,
        # VARCHAR width, UNSIGNED range, STRUCT/MAP fields, or ARRAY elements.
        if is_timezone_polarity_loss(source_type, target_type, dest_db=dest_db):
            return True
        if time_timezone_polarity_loss(source_type, target_type):
            return True
        if timezone_aware_would_collapse_to_string(
            source_type, target_type, dest_db=dest_db
        ):
            return True
        if long_raw_locator_would_collapse(source_type, target_type):
            return True
        if decfloat_domain_would_collapse(source_type, target_type):
            return True
        if bignumeric_capacity_would_invent(
            source_type,
            target_type,
            dest_db=dest_db,
            dest_table_exists=dest_table_exists,
        ):
            return True
        if decimal_fixed_point_would_collapse_to_text(
            source_type, target_type, dest_db=dest_db
        ):
            return True
        if smalldatetime_domain_would_invent(source_type, target_type):
            return True
        if decimal_params_would_narrow(source_type, target_type, dest_db=dest_db):
            return True
        if _blocking_width(
            source,
            target,
            source_type,
            target_type,
            dest_db=dest_db,
            proven=proven,
        ):
            return True
        if bounded_string_sink_would_truncate(
            source_type, target_type, dest_db=dest_db
        ) and not _width_proven(source, target, proven):
            return True
        if national_charset_would_collapse(source_type, target_type, dest_db=dest_db):
            return True
        if national_charset_would_invent(source_type, target_type, dest_db=dest_db):
            return True
        if code_page_sink_would_collapse(
            source_type, target_type, dest_db=dest_db, population=population
        ):
            return True
        if fixed_width_pad_polarity_loss(source_type, target_type, dest_db=dest_db):
            return True
        if bitstring_opaque_bytes_collapse(source_type, target_type):
            return True
        if binary_width_would_narrow(source_type, target_type):
            return True
        if bitstring_width_would_narrow(source_type, target_type):
            return True
        if year_domain_would_collapse(source_type, target_type):
            return True
        if money_domain_would_collapse(source_type, target_type):
            return True
        if unsigned_integer_would_overflow(source_type, target_type):
            return True
        if unsigned_signed_polarity_invent(source_type, target_type):
            return True
        if integer_width_would_narrow(source_type, target_type, dest_db=dest_db):
            return True
        if float_mantissa_would_narrow(source_type, target_type, dest_db=dest_db):
            return True
        if enum_set_domain_would_reject(source_type, target_type):
            return True
        if enum_domain_would_collapse(source_type, target_type):
            return True
        if interval_family_would_collapse(source_type, target_type, dest_db=dest_db):
            return True
        if interval_precision_would_narrow(source_type, target_type):
            return True
        if bitstring_pad_polarity_loss(source_type, target_type):
            return True
        if oracle_char_byte_polarity_loss(source_type, target_type):
            return True
        if oracle_long_numeric_invent(source_type, target_type):
            return True
        if geography_contract_would_collapse(source_type, target_type):
            return True
        if specialty_carrier_would_collapse(source_type, target_type, dest_db=dest_db):
            return True
        if specialty_domain_would_invent(
            source_type,
            target_type,
            dest_db=dest_db,
            dest_table_exists=dest_table_exists,
        ):
            return True
        if specialty_polarity_mismatch(source_type, target_type):
            return True
        if case_fold_polarity_invent(source_type, target_type):
            return True
        if accent_polarity_invent(source_type, target_type):
            return True
        if width_fold_polarity_invent(source_type, target_type):
            return True
        if kana_fold_polarity_invent(source_type, target_type):
            return True
        if vector_dim_mismatch(source_type, target_type):
            return True
        if vector_encoding_would_collapse(source_type, target_type):
            return True
        if rowversion_would_collapse_to_temporal(source_type, target_type):
            return True
        if sql_variant_would_collapse(source_type, target_type):
            return True
        if uuid_would_collapse(source_type, target_type, dest_db=dest_db):
            return True
        if objectid_would_collapse(source_type, target_type):
            return True
        if generated_always_overwrite_risk(target_type):
            return True
        if identity_polarity_would_collapse(source_type, target_type):
            return True
        if identity_domain_would_invent(source_type, target_type):
            return True
        if bfile_locator_would_collapse(source_type, target_type):
            return True
        if temporal_precision_would_narrow(
        source_type, target_type, dest_db=dest_db, population=population
    ):
            return True
        if src == LOGICAL_STRUCT and nested_struct_fields_incompatible(
            source_type, target_type, dest_db=dest_db
        ):
            return True
        if src in {LOGICAL_MAP, LOGICAL_ARRAY} and is_nested_shape_collapse(
            source_type, target_type, dest_db=dest_db
        ):
            return True
        return False

    # MySQL SET â†’ TEXT[] is the intentional multi-value sink (not stringâ†’array invent).
    if set_to_array_polarity_preserved(source_type, target_type):
        return False
    # BIT(n) â†’ VARCHAR(n) create-new is 0/1 digit text â€” not opaque BYTEA packing.
    if is_bitstring_carrier(source_type) and tgt in {LOGICAL_STRING, LOGICAL_TEXT}:
        return False
    # ObjectId â†’ VARCHAR(24)/BINARY(12)/STRING(24) is the industry create-new wire.
    if (
        normalize_logical_type(source_type) == LOGICAL_OBJECTID
        and specialty_wire_preserves_value("OBJECTID", target_type)
    ):
        return False
    # VECTOR(n) â†’ ARRAY<FLOAT> lakehouse create-new wire â€” not embedding invent.
    if vector_to_array_wire_preserved(source_type, target_type, dest_db=dest_db):
        return False
    # Document store ``date`` is an instant, not a calendar day — the time of
    # day survives. Sub-millisecond sources not measured to whole milliseconds
    # are excluded and fall through to temporal_precision_would_narrow, which
    # names the truncation.
    if document_instant_wire_preserved(
        source_type, target_type, dest_db=dest_db, population=population
    ):
        return False
    if _ts._dynamodb_number_wire_preserves(source_type, target_type, dest_db=dest_db):
        # AttributeValue N is an unbounded decimal, not DECIMAL(p,s). Float
        # lands as Decimal(str(value)). A sized DECIMAL(p,s) stamp is excluded
        # by the token check and stays lossy below.
        return False
    # ARRAY/STRUCT/MAP â†’ dialect create-new JSON/VARIANT/CLOB wire â€” representation.
    if nested_to_native_document_wire_preserved(
        source_type, target_type, dest_db=dest_db
    ):
        return False
    # JSON â†’ dialect-native document wire (CLOB/NVARCHAR(MAX)/JSONB/â€¦) â€” not lossy.
    if src == LOGICAL_JSON and is_dialect_native_document_wire(
        target_type, dest_db=dest_db
    ):
        return False
    # SQLite TEXT affinity: UUID / CHAR(36) UUID wire / TEXT are one carrier.
    if uuid_carrier_is_dialect_equivalent(
        source_type, target_type, dest_db=dest_db
    ):
        return False
    # Existing-table string/text → dialect-native JSON/JSONB is a load (parse at
    # write), not create-new document invent — must clear before allow-list fallthrough.
    if (
        dest_table_exists is True
        and src in {LOGICAL_STRING, LOGICAL_TEXT}
        and tgt == LOGICAL_JSON
        and is_dialect_native_document_wire(target_type, dest_db=dest_db)
    ):
        return False
    if _offset_text_preserves_instant(source_type, target_type, population):
        return False
    # Dialect-aware collapse SSOT â€” same rules as G3/probe (never MySQL-default FSP
    # when dest_db is postgresql/redshift, never false-collapse JSONâ†’JSONB).
    if is_precision_collapse_coercion(
        source_type,
        target_type,
        dest_db=dest_db,
        dest_table_exists=dest_table_exists,
        source=source,
        target=target,
        proven=proven,
    ):
        return True
    # Fielded STRUCT/MAP â†’ opaque JSON/VARIANT is intentional on many warehouses
    # (Airbyte V2) but is still a field-DDL collapse â€” treat as lossy so G3 surfaces it.
    if is_nested_document_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if timezone_aware_would_collapse_to_string(
        source_type, target_type, dest_db=dest_db
    ):
        return True
    if long_raw_locator_would_collapse(source_type, target_type):
        return True
    if decfloat_domain_would_collapse(source_type, target_type):
        return True
    if bignumeric_capacity_would_invent(
        source_type,
        target_type,
        dest_db=dest_db,
        dest_table_exists=dest_table_exists,
    ):
        return True
    if decimal_fixed_point_would_collapse_to_text(
        source_type, target_type, dest_db=dest_db
    ):
        return True
    if smalldatetime_domain_would_invent(source_type, target_type):
        return True
    if year_domain_would_collapse(source_type, target_type):
        return True
    if money_domain_would_collapse(source_type, target_type):
        return True
    if bitstring_opaque_bytes_collapse(source_type, target_type):
        return True
    if unsigned_integer_would_overflow(source_type, target_type):
        return True
    if unsigned_signed_polarity_invent(source_type, target_type):
        return True
    if integer_width_would_narrow(source_type, target_type, dest_db=dest_db):
        return True
    if float_mantissa_would_narrow(source_type, target_type, dest_db=dest_db):
        return True
    if _blocking_width(
        source,
        target,
        source_type,
        target_type,
        dest_db=dest_db,
        proven=proven,
    ):
        return True
    if bounded_string_sink_would_truncate(
        source_type, target_type, dest_db=dest_db
    ) and not _width_proven(source, target, proven):
        return True
    if national_charset_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if national_charset_would_invent(source_type, target_type, dest_db=dest_db):
        return True
    if code_page_sink_would_collapse(
        source_type, target_type, dest_db=dest_db, population=population
    ):
        return True
    if fixed_width_pad_polarity_loss(source_type, target_type, dest_db=dest_db):
        return True
    if binary_width_would_narrow(source_type, target_type):
        return True
    if bitstring_width_would_narrow(source_type, target_type):
        return True
    if enum_set_domain_would_reject(source_type, target_type):
        return True
    if enum_domain_would_collapse(source_type, target_type):
        return True
    if interval_family_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if interval_precision_would_narrow(source_type, target_type):
        return True
    if bitstring_pad_polarity_loss(source_type, target_type):
        return True
    if oracle_char_byte_polarity_loss(source_type, target_type):
        return True
    if oracle_long_numeric_invent(source_type, target_type):
        return True
    if geography_contract_would_collapse(source_type, target_type):
        return True
    if specialty_carrier_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if specialty_domain_would_invent(
        source_type,
        target_type,
        dest_db=dest_db,
        dest_table_exists=dest_table_exists,
    ):
        return True
    if specialty_polarity_mismatch(source_type, target_type):
        return True
    if case_fold_polarity_invent(source_type, target_type):
        return True
    if accent_polarity_invent(source_type, target_type):
        return True
    if width_fold_polarity_invent(source_type, target_type):
        return True
    if kana_fold_polarity_invent(source_type, target_type):
        return True
    if date_to_tz_aware_invent(source_type, target_type):
        return True
    if vector_dim_mismatch(source_type, target_type):
        return True
    if vector_encoding_would_collapse(source_type, target_type):
        return True
    if rowversion_would_collapse_to_temporal(source_type, target_type):
        return True
    if sql_variant_would_collapse(source_type, target_type):
        return True
    if uuid_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if objectid_would_collapse(source_type, target_type):
        return True
    if generated_always_overwrite_risk(target_type):
        return True
    if identity_polarity_would_collapse(source_type, target_type):
        return True
    if identity_domain_would_invent(source_type, target_type):
        return True
    if bfile_locator_would_collapse(source_type, target_type):
        return True
    if temporal_precision_would_narrow(
        source_type, target_type, dest_db=dest_db, population=population
    ):
        return True
    if document_domain_would_collapse(source_type, target_type, dest_db=dest_db):
        return True
    if document_domain_would_invent(
        source_type,
        target_type,
        dest_db=dest_db,
        dest_table_exists=dest_table_exists,
    ):
        return True
    # Text that merely uses the base64 alphabet is not a binary payload.
    # Decoding it into a binary column changes the value; that is lossy.
    if tgt == LOGICAL_BINARY and src in {
        LOGICAL_STRING,
        LOGICAL_TEXT,
        LOGICAL_UUID,
        LOGICAL_JSON,
        LOGICAL_ARRAY,
    }:
        return True
    # ARRAYâ†’ARRAY is in the safe allow-list below only when element types widen.
    if src == LOGICAL_ARRAY and tgt == LOGICAL_ARRAY and is_nested_shape_collapse(
        source_type, target_type, dest_db=dest_db
    ):
        return True
    if src == LOGICAL_MAP and tgt == LOGICAL_MAP and is_nested_shape_collapse(
        source_type, target_type, dest_db=dest_db
    ):
        return True

    safe: set[tuple[str, str]] = {
        # text / structural containers are universal sinks
        (LOGICAL_STRING, LOGICAL_TEXT),
        (LOGICAL_TEXT, LOGICAL_STRING),
        # Open string â†’ JSON invents document domain â€” not allow-listed.
        # JSON/VARIANT/SUPER â†’ open string drops document domain â€” not allow-listed.
        (LOGICAL_JSON, LOGICAL_JSON),
        # ARRAYâ†’JSON/text is document collapse (lossy) â€” not allow-listed.
        # JSONâ†’ARRAY invents array domain from a document â€” not allow-listed.
        (LOGICAL_ARRAY, LOGICAL_ARRAY),
        # numeric widening and text renderings
        (LOGICAL_INTEGER, LOGICAL_DECIMAL),
        # integerâ†’float is LOSSY for large ints (IEEE mantissa) â€” not allow-listed
        (LOGICAL_INTEGER, LOGICAL_STRING),
        (LOGICAL_INTEGER, LOGICAL_TEXT),
        (LOGICAL_INTEGER, LOGICAL_JSON),
        (LOGICAL_DECIMAL, LOGICAL_STRING),
        (LOGICAL_DECIMAL, LOGICAL_TEXT),
        (LOGICAL_DECIMAL, LOGICAL_JSON),
        (LOGICAL_FLOAT, LOGICAL_STRING),
        (LOGICAL_FLOAT, LOGICAL_TEXT),
        (LOGICAL_FLOAT, LOGICAL_JSON),
        # floatâ†’decimal is LOSSY (IEEE â†’ fixed-point) â€” not in this allow-list
        # boolean renderings and scalar widenings
        (LOGICAL_BOOLEAN, LOGICAL_STRING),
        (LOGICAL_BOOLEAN, LOGICAL_TEXT),
        (LOGICAL_BOOLEAN, LOGICAL_JSON),
        (LOGICAL_BOOLEAN, LOGICAL_INTEGER),
        (LOGICAL_BOOLEAN, LOGICAL_DECIMAL),
        (LOGICAL_BOOLEAN, LOGICAL_FLOAT),
        # dateâ†’datetime NTZ widening only; DATEâ†’TIMESTAMPTZ invents midnight TZ.
        (LOGICAL_DATE, LOGICAL_DATETIME),
        (LOGICAL_DATE, LOGICAL_STRING),
        (LOGICAL_DATE, LOGICAL_TEXT),
        (LOGICAL_DATE, LOGICAL_JSON),
        (LOGICAL_DATETIME, LOGICAL_STRING),
        (LOGICAL_DATETIME, LOGICAL_TEXT),
        (LOGICAL_DATETIME, LOGICAL_JSON),
        (LOGICAL_TIME, LOGICAL_STRING),
        (LOGICAL_TIME, LOGICAL_TEXT),
        (LOGICAL_TIME, LOGICAL_JSON),
        # uuid renderings (domain still collapses via uuid_would_collapse above)
        (LOGICAL_UUID, LOGICAL_STRING),
        (LOGICAL_UUID, LOGICAL_TEXT),
        (LOGICAL_UUID, LOGICAL_JSON),
        # VECTORâ†’ARRAY drops embedding domain â€” not allow-listed.
        # STRUCT/MAP â†’ text/JSON is nested document collapse (handled above).
        (LOGICAL_STRUCT, LOGICAL_STRUCT),
        (LOGICAL_MAP, LOGICAL_MAP),
    }

    if (src, tgt) in safe:
        # DATEâ†’TIMESTAMPTZ / DATETIMEOFFSET invents an instant â€” not a free widen.
        if date_to_tz_aware_invent(source_type, target_type):
            return True
        if case_fold_polarity_invent(source_type, target_type):
            return True
        if accent_polarity_invent(source_type, target_type):
            return True
        if width_fold_polarity_invent(source_type, target_type):
            return True
        if kana_fold_polarity_invent(source_type, target_type):
            return True
        if national_charset_would_collapse(source_type, target_type, dest_db=dest_db):
            return True
        if national_charset_would_invent(source_type, target_type, dest_db=dest_db):
            return True
        if code_page_sink_would_collapse(
            source_type, target_type, dest_db=dest_db, population=population
        ):
            return True
        if fixed_width_pad_polarity_loss(source_type, target_type, dest_db=dest_db):
            return True
        # INTEGERâ†’VARCHAR(1) / JSONâ†’CHAR(10) â€” bounded sink truncates.
        if bounded_string_sink_would_truncate(
            source_type, target_type, dest_db=dest_db
        ) and not _width_proven(source, target, proven):
            return True
        return False
    if _measured_text_preserves_scalar(
        source_type,
        target_type,
        population,
        dest_db=dest_db,
        dest_table_exists=dest_table_exists,
    ):
        return False
    return True
