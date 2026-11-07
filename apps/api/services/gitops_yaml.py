"""One YAML dumper for every GitOps artifact (schedules, manifests, contracts).

``yaml.safe_dump`` looks representers up by exact type, so an ``int`` subclass
(``bson.int64.Int64``, which MongoDB returns for every integer above 2**31), a
``Decimal`` or a ``Decimal128`` raised ``RepresenterError`` and the export
route answered 500 while the JSON export of the same artifact answered 200
(QA SC11). The stores now hand back plain values, and this dumper is the
second line: it writes exactly what JSON would carry, with exact decimals.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

import yaml


class _ArtifactDumper(yaml.SafeDumper):
    """SafeDumper that knows the value types our stores can hand back."""


def _represent_int(dumper: yaml.SafeDumper, value: int) -> yaml.Node:
    return dumper.represent_int(int(value))


def _represent_decimal(dumper: yaml.SafeDumper, value: Decimal) -> yaml.Node:
    if not value.is_finite():
        # NaN / Infinity: YAML's own spellings, not a crash.
        return dumper.represent_float(float(value))
    if value == value.to_integral_value() and value.as_tuple().exponent >= 0:
        return dumper.represent_int(int(value))
    # The exact digits as a plain float scalar: ``12.30`` stays ``12.30`` in
    # the file and loads back as a number, never as a rounded binary float.
    text = format(value, "f")
    return dumper.represent_scalar("tag:yaml.org,2002:float", text)


def _represent_fallback(dumper: yaml.SafeDumper, value: Any) -> yaml.Node:
    from services.value_serializer import json_default, python_document_from_bson

    if isinstance(value, str):
        return dumper.represent_str(str(value))
    if isinstance(value, dict):
        return dumper.represent_dict(dict(value))
    if isinstance(value, (list, tuple, set, frozenset)):
        return dumper.represent_list(list(value))
    plain = python_document_from_bson(value)
    if plain is not value:
        return dumper.represent_data(plain)
    # ObjectId, UUID, enum members, bytes...: the same text the JSON export writes.
    return dumper.represent_data(json_default(value))


_ArtifactDumper.add_multi_representer(int, _represent_int)
_ArtifactDumper.add_representer(bool, yaml.SafeDumper.represent_bool)
_ArtifactDumper.add_representer(Decimal, _represent_decimal)
_ArtifactDumper.add_representer(datetime, yaml.SafeDumper.represent_datetime)
_ArtifactDumper.add_representer(date, yaml.SafeDumper.represent_date)
_ArtifactDumper.add_multi_representer(object, _represent_fallback)


def dump_artifact_yaml(artifact: Any) -> str:
    """Serialise a GitOps artifact as YAML, keeping key order and block style."""
    return yaml.dump(
        artifact,
        Dumper=_ArtifactDumper,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=False,
    )
