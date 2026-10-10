from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any, Iterator

from connectors.sdk import RecordBatch
from connectors.sdk.declarative.connector import DeclarativeSource
from connectors.sdk.declarative.errors import ConnectorAuthError, TransientExhausted
from tests.connector_certification.cases import build_certification_cases
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer
from tests.connector_certification.harness import CertificationCase, certify


def test_certify_rejects_broken_connector_claims_at_the_related_step(
    tmp_path: Path,
) -> None:
    base = build_certification_cases(tmp_path)["declarative_source"]

    class MutatedSource(DeclarativeSource):
        def __init__(self, config: dict[str, Any], phase: str, secret: str) -> None:
            super().__init__(config)
            self.phase = phase
            self.secret = secret

    class DropsLastPage(MutatedSource):
        def read(self, stream: str, *, state: dict[str, Any] | None = None, offset: int = 0, limit: int = 1000) -> Iterator[RecordBatch]:
            if self.phase != "full_read":
                yield from super().read(stream, state=state, offset=offset, limit=limit)
                return
            batches = list(super().read(stream, state=state, offset=offset, limit=limit))
            if batches:
                yield from batches[:-1]
                last = batches[-1]
                yield RecordBatch(last.stream, [], last.schema, last.state)

    class SavesStateBeforeWrite(MutatedSource):
        pass

    class IgnoresCursor(MutatedSource):
        def read(self, stream: str, *, state: dict[str, Any] | None = None, offset: int = 0, limit: int = 1000) -> Iterator[RecordBatch]:
            return super().read(
                stream,
                state=None if self.phase == "incremental" else state,
                offset=offset,
                limit=limit,
            )

    class SwallowsRateLimit(MutatedSource):
        def read(self, stream: str, *, state: dict[str, Any] | None = None, offset: int = 0, limit: int = 1000) -> Iterator[RecordBatch]:
            if self.phase != "rate_limit":
                yield from super().read(stream, state=state, offset=offset, limit=limit)
                return
            try:
                yield from super().read(stream, state=state, offset=offset, limit=limit)
            except TransientExhausted:
                return

    class LeaksSecret(MutatedSource):
        def read(self, stream: str, *, state: dict[str, Any] | None = None, offset: int = 0, limit: int = 1000) -> Iterator[RecordBatch]:
            if self.phase == "auth":
                raise ConnectorAuthError(f"authentication rejected: {self.secret}")
            return super().read(stream, state=state, offset=offset, limit=limit)

    def broken_factory(source_type: type[MutatedSource]):
        def factory(
            base_url: str,
            phase: str,
            records: Any,
            sleep: Any = None,
        ) -> MutatedSource:
            connector = base.connector_factory(base_url, phase, records, sleep)
            return source_type(connector.config, phase, base.secret)

        return factory

    def save_state_before_write(
        source: DeclarativeSource,
        stream: str,
        write_fn: Any,
        load_state: Any,
        save_state: Any,
    ) -> int:
        state = load_state(stream)
        count = 0
        for batch in source.read(stream, state=state):
            save_state(stream, batch.state)
            write_fn(batch.records)
            count += len(batch.records)
        return count

    def all_incremental_rows(
        fixture: FixtureServer,
        phase: str,
        records: Any,
    ) -> None:
        if phase == "incremental":
            fixture.add_route(
                "/items",
                FixtureResponse(body={"data": [dict(record) for record in records]}),
            )
        else:
            base.fixture_routes(fixture, phase, records)

    def repeated_rate_limit(
        fixture: FixtureServer,
        phase: str,
        records: Any,
    ) -> None:
        if phase == "rate_limit":
            fixture.add_route(
                "/items",
                responses=[
                    FixtureResponse(
                        status=429,
                        body={"message": "rate limited"},
                        headers={"Retry-After": "2"},
                    )
                    for _ in range(4)
                ],
            )
        else:
            base.fixture_routes(fixture, phase, records)

    mutations: list[tuple[str, CertificationCase]] = [
        (
            "full_read",
            replace(base, connector_factory=broken_factory(DropsLastPage)),
        ),
        (
            "resume_after_failure",
            replace(
                base,
                connector_factory=broken_factory(SavesStateBeforeWrite),
                sync_runner=save_state_before_write,
            ),
        ),
        (
            "incremental_read",
            replace(
                base,
                connector_factory=broken_factory(IgnoresCursor),
                fixture_routes=all_incremental_rows,
            ),
        ),
        (
            "rate_limit",
            replace(
                base,
                connector_factory=broken_factory(SwallowsRateLimit),
                fixture_routes=repeated_rate_limit,
            ),
        ),
        (
            "bad_credentials",
            replace(base, connector_factory=broken_factory(LeaksSecret)),
        ),
    ]

    for expected_failure, case in mutations:
        report = certify(case)
        assert [step.name for step in report.failed_steps] == [expected_failure], report.to_dict()
