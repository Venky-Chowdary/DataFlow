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
* No registry, ``key_format`` unset: a topic whose earlier runs wrote the old
  names (``services.naming_history``: a pre-upgrade job to the same topic,
  workspace and cluster, or the latest stamped run) keeps them by default
  (``key_format=legacy``, the Redis precedent), so an existing or scheduled
  job does not change shape on upgrade. The run records the default and
  Validate shows an info note naming ``key_format=exact``. A new topic (no
  history) gets the exact names. History that cannot be read refuses the run
  until ``key_format`` is set.
* ``key_format=legacy``: every target keeps its old spelling.
* ``key_format=exact`` or ``legacy_field_policy=write_new``: the exact names on
  purpose; on a topic with earlier runs the run records what changed.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

KEY_FORMAT = "key_format"
LEGACY = "legacy"
EXACT = "exact"

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
    """The operator's choice: ``legacy``, ``exact`` (also ``write_new``), or ``""`` (unset)."""
    opts = _options(cfg, endpoint)
    raw = str(opts.get(KEY_FORMAT) or "").strip().lower()
    if raw in (LEGACY, EXACT):
        return raw
    from services.target_name_legacy import LEGACY_FIELD_POLICY, WRITE_NEW

    return EXACT if str(opts.get(LEGACY_FIELD_POLICY) or "").strip() == WRITE_NEW else ""


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


class KafkaHistoryUnverified(RuntimeError):
    """The topic's job history could not be read and no ``key_format`` was set."""


def kafka_naming(
    cfg: Mapping[str, Any] | None, endpoint: Any, topic: str, mappings: Iterable[Any] | None,
    *, workspace_id: str, timeout: float = 5.0,
) -> dict[str, Any]:
    """How this run names the topic's fields.

    ``mode``: ``registry`` (adopt the subject's ``fields``), ``legacy`` (every
    target keeps its old spelling) or ``exact``. ``default`` is True when
    ``legacy`` was chosen because earlier runs of the topic wrote the old
    names (``naming_history``: a pre-upgrade job, or the latest stamped run)
    and no ``key_format`` was set; ``history`` says they did (only looked up
    when a target's spelling differs). ``pairs`` are the operator's
    ``(target, old spelling)``.
    """
    from services.naming_history import LEGACY as HISTORY_LEGACY
    from services.naming_history import object_history

    pairs = renamed_targets(mappings)
    url = kafka_registry_url(cfg, endpoint)
    if url and topic:
        return {"mode": "registry", "fields": list(
            registry_subject_fields(url, topic, timeout=timeout) or []),
            "pairs": pairs, "default": False, "history": False}
    chosen = kafka_key_format(cfg, endpoint)
    out = {"mode": chosen or EXACT, "fields": [], "pairs": pairs, "default": False,
           "history": False}
    if not pairs or not topic or chosen == LEGACY:
        return out
    try:
        out["history"] = object_history(
            "kafka", topic, cfg, endpoint, workspace_id) == HISTORY_LEGACY
    except Exception as exc:  # noqa: BLE001 - fail closed only when nothing was chosen
        if chosen == EXACT:
            out["unverified"] = str(exc)
            return out
        raise KafkaHistoryUnverified(
            f"Could not read the run history of topic {topic!r} ({exc}), so Datawrap "
            "cannot tell whether earlier runs wrote the old field names. Set "
            f"{KEY_FORMAT}={LEGACY!r} to keep them or {KEY_FORMAT}={EXACT!r} to send the "
            "mapped names exactly.") from exc
    if not chosen and out["history"]:
        out.update(mode=LEGACY, default=True)
    return out


def adopted_fields(naming: Mapping[str, Any]) -> list[str]:
    """The fields a run's targets adopt under ``naming``."""
    if naming.get("mode") == "registry":
        return list(naming.get("fields") or [])
    if naming.get("mode") == LEGACY:
        return [old for _target, old in naming.get("pairs") or []]
    return []


def default_legacy_note(naming: Mapping[str, Any], topic: str) -> str:
    """Validate's info note / the run's evidence for the upgrade default."""
    names = ", ".join(f"{t!r} -> {o!r}" for t, o in list(naming.get("pairs") or [])[:5])
    return (
        f"Topic {topic!r} has earlier runs, so it keeps the field names they wrote "
        f"({KEY_FORMAT}=legacy by default): {names}; set {KEY_FORMAT}=exact to send the "
        "mapped names exactly."
    )


def kafka_evidence(naming: Mapping[str, Any], topic: str) -> tuple[list[dict[str, str]], list[str]]:
    """``(warnings, run evidence)`` for a naming decision.

    The upgrade default is recorded; exact names on a topic with earlier runs
    are a warning at Validate and recorded on the run.
    """
    if naming.get("default"):
        return [], [default_legacy_note(naming, topic)]
    if naming.get("mode") != EXACT or not naming.get("pairs"):
        return [], []
    if naming.get("unverified"):
        return [], [f"topic history not verified ({naming['unverified']}); {KEY_FORMAT}=exact"]
    if not naming.get("history"):
        return [], []
    warnings, evidence = [], []
    for target, old in naming["pairs"]:
        warnings.append({
            "code": "target_topic_legacy_field",
            "target": target,
            "other": old,
            "message": (
                f"Topic {topic!r} has earlier runs that wrote {old!r} for {target!r}; with "
                f"{KEY_FORMAT}=exact this run writes {target!r}, so consumers (and a "
                f"composite message key) see a renamed field. Set {KEY_FORMAT}=legacy to "
                "keep the old names."
            ),
        })
        evidence.append(
            f"{old} written by earlier runs of topic {topic}; this run writes {target} "
            f"({KEY_FORMAT}=exact)")
    return warnings, evidence


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
    "EXACT",
    "KEY_FORMAT",
    "LEGACY",
    "KafkaHistoryUnverified",
    "adopted_fields",
    "default_legacy_note",
    "kafka_evidence",
    "kafka_key_format",
    "kafka_naming",
    "kafka_registry_url",
    "registry_subject_fields",
    "renamed_targets",
    "with_kafka_name_options",
]
