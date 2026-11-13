"""Kafka topics keep the field names earlier runs gave them (G11 re-review M3).

Before G11 the Kafka writer sanitized each Map target (``First Name`` ->
``First_Name``, ``address.city`` -> ``address_city``, cut at 63); since G11 it
produces the name exactly as mapped. On an existing topic that changes the
event shape consumers read (and a composite message key, which is the JSON of
the key fields) with no Validate signal, and a Schema Registry subject that
holds the old names made the write fail with "physical DDL missing".

* Schema Registry subject: its fields are the topic's existing columns. A
  target whose old spelling is a subject field keeps writing that field, as a
  SaaS app's existing field is adopted (``target_name_legacy``).
* No registry, ``key_format=legacy`` on the destination: every target keeps
  its old spelling (the operator's opt-out, like Redis ``key_format``).
* No registry, topic with earlier runs (job history for that topic): a target
  whose spelling changed is refused at Validate (``g23e``) and Execute, with
  the remedies, unless ``legacy_field_policy=write_new`` (recorded on the run).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

KEY_FORMAT = "key_format"
LEGACY = "legacy"

#: Destination options this module reads (Validate merges them into its probe config).
NAMING_OPTIONS = (KEY_FORMAT, "schema_registry_url", "registry_url", "legacy_field_policy")


def _options(cfg: Mapping[str, Any] | None, endpoint: Any = None) -> dict[str, Any]:
    """Destination options: connector config (and its ``extra``), then the endpoint's."""
    out: dict[str, Any] = {}
    for layer in (cfg, endpoint):
        if layer is None:
            continue
        if isinstance(layer, Mapping):
            top = dict(layer)
        else:
            top = dict(getattr(layer, "extra", None) or {})
        nested = top.get("extra") if isinstance(top.get("extra"), Mapping) else {}
        for source in (nested, top):
            for key, value in source.items():
                if value not in (None, "") and key != "extra":
                    out[key] = value
    return out


def kafka_key_format(cfg: Mapping[str, Any] | None, endpoint: Any = None) -> str:
    """``legacy`` when the destination keeps the pre-G11 field names, else ``exact``."""
    raw = str(_options(cfg, endpoint).get(KEY_FORMAT) or "").strip().lower()
    return LEGACY if raw == LEGACY else "exact"


def kafka_registry_url(cfg: Mapping[str, Any] | None, endpoint: Any = None) -> str:
    """The Schema Registry the writer uses (the same precedence as the write)."""
    opts = _options(cfg, endpoint)
    conn = str(opts.get("connection_string") or "")
    return str(
        opts.get("schema_registry_url") or opts.get("registry_url")
        or (conn if "http" in conn else "") or ""
    ).strip()


def registry_subject_fields(url: str, topic: str, *, timeout: float = 5.0) -> list[str] | None:
    """Field names of the topic's latest subject; ``None`` when no subject exists.

    The writer's subjects (``<topic>-value``, then ``<topic>``); any failure
    other than HTTP 404 raises, so a caller never reads it as "no fields".
    """
    from connectors.confluent_schema_registry import (
        SchemaRegistryError,
        fetch_latest_subject_schema,
        schema_map_from_registry_doc,
    )

    for subject in (f"{topic}-value", topic):
        try:
            doc = fetch_latest_subject_schema(url, subject, timeout=timeout)
        except SchemaRegistryError as exc:
            if "http 404" in str(exc).lower():
                continue
            raise
        return [str(name) for name in schema_map_from_registry_doc(doc)]
    return None


def renamed_targets(mappings: Iterable[Any] | None) -> list[tuple[str, str]]:
    """``(target, old spelling)`` for each Map target the writer used to rename."""
    from connectors.destination_naming import _mapped_targets
    from services.target_name_legacy import legacy_spelling

    out: list[tuple[str, str]] = []
    for target in _mapped_targets(mappings):
        old = legacy_spelling("kafka", target)
        if old and old != target and (target, old) not in out:
            out.append((target, old))
    return out


def kafka_existing_fields(
    cfg: Mapping[str, Any] | None, endpoint: Any, topic: str, mappings: Iterable[Any] | None,
    *, timeout: float = 5.0,
) -> list[str]:
    """The fields a topic's targets adopt (see the module docstring)."""
    url = kafka_registry_url(cfg, endpoint)
    if url and topic:
        return list(registry_subject_fields(url, topic, timeout=timeout) or [])
    if kafka_key_format(cfg, endpoint) == LEGACY:
        return [old for _target, old in renamed_targets(mappings)]
    return []


def _same_cluster(job: Mapping[str, Any], connector_id: str, host: str) -> bool:
    theirs_id = str(job.get("dest_connector_id") or "")
    if connector_id and theirs_id:
        return theirs_id == connector_id
    theirs_host = str(((job.get("transfer_request") or {}).get("destination") or {}).get(
        "host") or "")
    return not (host and theirs_host) or theirs_host == host


def topic_has_prior_runs(topic: str, cfg: Mapping[str, Any] | None, endpoint: Any = None) -> bool:
    """Whether a job (in the caller's workspace scope) already wrote rows to ``topic``.

    Raises when the job history cannot be read: callers fail closed.
    """
    from connectors.destination_naming import STREAM_SINKS, dest_kind
    from services.mongodb_service import get_mongodb_service
    from services.workspace_filter import current_workspace_filter

    opts = _options(cfg, endpoint)
    connector_id = str(getattr(endpoint, "connector_id", "") or opts.get("connector_id") or "")
    host = str(getattr(endpoint, "host", "") or opts.get("host") or "")
    jobs = get_mongodb_service().destination_job_history(
        topic, workspace_filter=current_workspace_filter())
    return any(
        dest_kind(str(job.get("destination_type") or "")) in STREAM_SINKS
        and _same_cluster(job, connector_id, host)
        for job in jobs
    )


def kafka_name_verdict(
    mappings: Iterable[Any] | None, cfg: Mapping[str, Any] | None, endpoint: Any, topic: str,
    *, policy: str = "",
) -> tuple[list[dict[str, str]], bool]:
    """``(issues, blocking)`` for targets whose spelling changed on a used topic.

    Nothing to judge with a Schema Registry (its subject is adopted) or with
    ``key_format=legacy`` (the old names are kept).
    """
    from services.target_name_legacy import LEGACY_FIELD_POLICY, WRITE_NEW

    if kafka_registry_url(cfg, endpoint) or kafka_key_format(cfg, endpoint) == LEGACY:
        return [], False
    pairs = renamed_targets(mappings)
    if not pairs or not topic or not topic_has_prior_runs(topic, cfg, endpoint):
        return [], False
    blocking = policy != WRITE_NEW
    issues = []
    for target, old in pairs:
        ack = (
            f" To write {target!r} anyway, set {LEGACY_FIELD_POLICY}={WRITE_NEW!r} on the "
            "destination; the run records it." if blocking else ""
        )
        issues.append({
            "code": "target_topic_legacy_field",
            "target": target,
            "other": old,
            "message": (
                f"Topic {topic!r} has earlier runs that wrote {old!r} for {target!r}; this "
                f"run writes {target!r}, so consumers (and a composite message key) would "
                f"see a renamed field. Set {KEY_FORMAT}={LEGACY!r} on the destination to keep "
                f"the old names, or rename the Map target to {old!r}.{ack}"
            ),
        })
    return issues, blocking


def kafka_ack(issues: Iterable[Mapping[str, str]], topic: str) -> list[str]:
    """What an acknowledged run records: each old name it no longer writes."""
    from services.target_name_legacy import LEGACY_FIELD_POLICY, WRITE_NEW

    return [
        f"{i['other']} written by earlier runs of topic {topic}; this run writes "
        f"{i['target']} ({LEGACY_FIELD_POLICY}={WRITE_NEW})"
        for i in issues
    ]


def with_kafka_name_options(probe_cfg: Any, *extras: Any) -> Any:
    """Validate's probe config carrying the destination's naming options.

    Validate adopts the same Map rows Execute does, so ``key_format`` and the
    Schema Registry the Studio sent in the destination options ride on it.
    """
    options = {
        key: value
        for extra in extras if isinstance(extra, Mapping)
        for key, value in dict(extra).items()
        if key in NAMING_OPTIONS and value not in (None, "")
    }
    if not probe_cfg or not options:
        return probe_cfg
    from services.secret_config import RedactedConfig

    merged = {**dict(probe_cfg), **options}
    return RedactedConfig(merged) if isinstance(probe_cfg, RedactedConfig) else merged


__all__ = [
    "KEY_FORMAT",
    "LEGACY",
    "kafka_ack",
    "kafka_existing_fields",
    "kafka_key_format",
    "kafka_name_verdict",
    "kafka_registry_url",
    "registry_subject_fields",
    "renamed_targets",
    "topic_has_prior_runs",
    "with_kafka_name_options",
]
