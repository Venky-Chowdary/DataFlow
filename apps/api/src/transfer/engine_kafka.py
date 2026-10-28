"""Kafka-source run policy for the transfer engine.

Where a Kafka source reads from and when its consumer-group offsets move:
the engine stamps the route's group and start position on the source endpoint
before anything reads, and commits the group once the job is accepted.
"""

from __future__ import annotations

from typing import Any

from .adapters import resolve_connector_config


def stamp_kafka_source(request: Any) -> None:
    """Stamp a Kafka source with its consumer group and start position.

    D84 — a source without a configured group reads under its route's, so the
    peek, the stream and the accept-time commit all name the same group and two
    pipelines on one connector never share (and rebalance) one group (D87).

    D151 — only an incremental run continues from the group's committed
    offsets. A full refresh re-reads the topic from the start: resuming at the
    position an earlier run committed loaded 0 rows on every run after the
    first.
    """
    from connectors.kafka_reader import (
        KAFKA_ROUTE_GROUP_KEY,
        KAFKA_START_KEY,
        configured_kafka_group_id,
        kafka_route_group_id,
    )
    from services.copy_kafka_common import kafka_family_name
    from services.sync_cursor import (
        requires_incremental,
        resolve_effective_sync_mode,
        resolve_sync_contract,
    )

    source = request.source
    if kafka_family_name(str(source.format or "")) != "kafka":
        return
    extra = dict(source.extra or {})
    contract = resolve_sync_contract(getattr(request, "stream_contracts", None))
    effective = resolve_effective_sync_mode(
        request.sync_mode, contract.sync_mode if contract else None
    )
    extra[KAFKA_START_KEY] = "committed" if requires_incremental(effective) else "earliest"
    if not configured_kafka_group_id(extra):
        extra[KAFKA_ROUTE_GROUP_KEY] = kafka_route_group_id(source, request.destination)
    source.extra = extra


def commit_kafka_offsets_on_accept(request: Any, dest_summary: dict) -> None:
    """D85 — commit Kafka consumer-group offsets once, only on acceptance.

    Called on the success path after reconcile passed and the destination
    committed. During the run, ``stream`` accumulates the per-partition end
    offsets in ``dest_summary['kafka_offsets']`` and never commits; a failed or
    cancelled run leaves the broker position untouched so the next run
    re-reads every uncommitted message (at-least-once).
    """
    kafka_commit = dest_summary.get("kafka_offsets") or {}
    pending = kafka_commit.get("pending_offsets") or []
    if (request.source.format or "") != "kafka" or not pending:
        return
    try:
        from connectors.kafka_reader import commit_kafka_offsets

        commit_kafka_offsets(
            resolve_connector_config(request.source),
            {
                "group_id": kafka_commit.get("group_id"),
                "topic": kafka_commit.get("topic"),
                "pending_offsets": pending,
            },
        )
    except Exception as exc:
        from services.execution_engine_contract import (
            kafka_offset_commit_must_fail_closed,
        )

        raise kafka_offset_commit_must_fail_closed(exc) from exc
    kafka_commit["committed"] = True
    kafka_commit["committed_offsets"] = [dict(item) for item in pending]
    dest_summary["kafka_offsets"] = kafka_commit
