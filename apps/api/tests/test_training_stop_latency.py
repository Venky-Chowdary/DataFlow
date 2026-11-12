"""A training run stops within about a second of shutdown asking it to.

PR 410 review H1: the grounded-schema loop checked the stop event once per
schema, and one schema (the repo's 426-"column" RBAC route map, read from
``tests/fixtures`` as if it were an upload) ran ``compose_response`` for
minutes: the mapper scores every column pair, so the cost grows with the
square of the width. Shutdown returned on time, but the abandoned daemon
thread kept the CPU and the GIL for minutes afterwards.

Now the run checks the event before every question, schemas wider than
``MAX_GROUNDED_COLUMNS`` are skipped (and counted in the run's metrics), and
the feeder's default upload directories hold no test fixtures.
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path

import pytest

from src.ai.training.universal_data_feeder import UniversalSchema

STOP_BUDGET_SECONDS = 2.0


def _bare_agent(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    from src.ai.training.training_agent import DataTransferTrainingAgent

    return DataTransferTrainingAgent()


def _schema(name: str, width: int, source: str = "upload") -> UniversalSchema:
    columns = [f"customer_field_{i}" for i in range(width)]
    return UniversalSchema(
        name=name, source=source, columns=columns,
        samples={c: ["alpha", "beta"] for c in columns}, row_count=10,
    )


# --- H1: the real loop, a real wide schema ----------------------------------


def test_stop_requested_mid_grounded_loop_is_honoured_quickly(tmp_path, monkeypatch):
    """Real ``run_full_training`` + real data analyst on a 400-column upload.

    Shutdown arrives as the grounded loop starts its first analysis. The run
    must return ``stopped`` within ``STOP_BUDGET_SECONDS`` of that, not after
    the minutes one wide schema costs.
    """
    agent = _bare_agent(tmp_path, monkeypatch)
    schemas = [_schema("wide_route_map", 400)] + [_schema(f"orders_{i}", 12) for i in range(6)]
    monkeypatch.setattr(agent.feeder, "feed_all", lambda *a, **k: schemas)
    monkeypatch.setattr(agent.conversation_synth, "synthesize_full", lambda *a, **k: [])

    import src.ai.copilot.data_analyst as data_analyst

    analyst = data_analyst.CopilotDataAnalyst()
    real_analyze = analyst.analyze_schema
    # One-time mapper/model warm-up is not loop latency: pay it before the run.
    analyst.compose_response(real_analyze(_schema("warm_up", 4)), "Analyze warm up", "mapping_help")
    stop = threading.Event()
    stop_requested_at: list[float] = []

    def analyze_schema(schema):
        if not stop.is_set():  # shutdown arrives while the loop works
            stop_requested_at.append(time.monotonic())
            stop.set()
        return real_analyze(schema)

    analyst.analyze_schema = analyze_schema
    monkeypatch.setattr(data_analyst, "get_data_analyst", lambda: analyst)

    result: dict = {}

    def _run() -> None:
        result["run"] = agent.run_full_training(False, True, stop_event=stop)
        result["returned_at"] = time.monotonic()

    worker = threading.Thread(target=_run, name="h1-run", daemon=True)
    worker.start()
    assert stop.wait(60), "the grounded loop never started"
    worker.join(STOP_BUDGET_SECONDS)
    assert not worker.is_alive(), (
        f"run still going {STOP_BUDGET_SECONDS:.0f}s after the stop request: "
        "the grounded loop does not honour the stop event"
    )
    latency = result["returned_at"] - stop_requested_at[0]
    print(f"stop honoured after {latency:.2f}s")
    assert result["run"].status == "stopped"
    assert latency < STOP_BUDGET_SECONDS


class _CountingAnalyst:
    """Stand-in analyst; ``stop_on_compose`` sets the stop event on that call."""

    def __init__(self, stop: threading.Event, stop_on_compose: int | None = None) -> None:
        self.stop = stop
        self.stop_on_compose = stop_on_compose
        self.analysed: list[str] = []
        self.composed = 0

    def analyze_schema(self, schema):
        self.analysed.append(schema.name)
        return schema

    def compose_response(self, insight, question, mode):
        self.composed += 1
        if self.composed == self.stop_on_compose:
            self.stop.set()
        return "ok"


def _run_with_analyst(tmp_path, monkeypatch, schemas, analyst, stop):
    agent = _bare_agent(tmp_path, monkeypatch)
    monkeypatch.setattr(agent.feeder, "feed_all", lambda *a, **k: schemas)
    monkeypatch.setattr(agent.conversation_synth, "synthesize_full", lambda *a, **k: [])
    monkeypatch.setattr(agent, "_ingest_training_documents_batched", lambda docs, **k: len(docs))
    monkeypatch.setattr("src.services.catalog_service.catalog_training_docs", lambda: [])

    import src.ai.copilot.data_analyst as data_analyst

    monkeypatch.setattr(data_analyst, "get_data_analyst", lambda: analyst)
    return agent.run_full_training(False, True, stop_event=stop)


@pytest.mark.parametrize(
    ("stop_on_compose", "analysed", "composed"),
    [
        (1, ["s0"], 1),  # the check before each question
        (3, ["s0"], 3),  # the check before each schema
    ],
)
def test_grounded_loop_checks_before_every_question_and_schema(
    tmp_path, monkeypatch, stop_on_compose, analysed, composed,
):
    stop = threading.Event()
    analyst = _CountingAnalyst(stop, stop_on_compose)
    schemas = [_schema(f"s{i}", 3, source="industry") for i in range(4)]

    run = _run_with_analyst(tmp_path, monkeypatch, schemas, analyst, stop)

    assert run.status == "stopped"
    assert (analyst.analysed, analyst.composed) == (analysed, composed)


def test_schemas_wider_than_the_cap_are_skipped_and_reported(tmp_path, monkeypatch, caplog):
    import json
    import logging

    from src.ai.training.training_agent import MAX_GROUNDED_COLUMNS

    assert MAX_GROUNDED_COLUMNS == 64
    stop = threading.Event()
    analyst = _CountingAnalyst(stop)
    schemas = [
        _schema("acme_payroll_export", MAX_GROUNDED_COLUMNS + 1),
        _schema("at_the_cap", MAX_GROUNDED_COLUMNS),
        _schema("narrow", 3, source="industry"),
        _schema("route_map", 426),
    ]

    with caplog.at_level(logging.INFO):
        run = _run_with_analyst(tmp_path, monkeypatch, schemas, analyst, stop)

    assert run.status == "completed", run.errors
    assert analyst.analysed == ["at_the_cap", "narrow"]
    assert run.metrics["grounded_schemas"] == 2
    assert run.metrics["grounded_schemas_skipped_too_wide"] == 2
    assert run.metrics["data_grounded_examples"] == 6
    # Review L-T2: upload stems come from every workspace's uploads. The run
    # metrics (returned by /copilot/train to any workspace admin) carry the
    # count only; the names go to the server log.
    metrics_text = json.dumps(run.metrics, default=str)
    assert "acme_payroll_export" not in metrics_text and "route_map" not in metrics_text
    assert "grounded_schemas_skipped_names" not in run.metrics
    logged = " ".join(r.getMessage() for r in caplog.records)
    assert "acme_payroll_export" in logged and "route_map" in logged, logged


# --- the feeder never reads the repo's test fixtures ------------------------


def test_default_upload_dirs_hold_no_test_or_fixture_directory():
    from src.ai.training.universal_data_feeder import UniversalDataFeeder, default_upload_dirs

    api_root = Path(__file__).resolve().parents[1]
    # The feeder reads the deployment's upload directory (PR 405:
    # platform_config.upload_dir() plus DATAFLOW_PILOT_DATASET_DIRS);
    # default_upload_dirs() is the fixture-free list for an explicit api root.
    dirs = UniversalDataFeeder().upload_dirs
    for raw in [*dirs, *default_upload_dirs(api_root)]:
        parts = {p.lower() for p in Path(raw).parts}
        assert not parts & {"tests", "test", "fixtures"}, raw
    # apps/api/uploads is platform_config.upload_dir() when no volume or
    # DATAFLOW_UPLOAD_DIR is set: the local upload directory is still read.
    assert str(api_root / "uploads") in dirs


def test_feeder_reads_uploads_but_not_the_fixtures_beside_them(tmp_path):
    from src.ai.training.universal_data_feeder import UniversalDataFeeder, default_upload_dirs

    (tmp_path / "uploads").mkdir()
    (tmp_path / "uploads" / "orders.csv").write_text("id,amount\n1,2.50\n", encoding="utf-8")
    fixtures = tmp_path / "tests" / "fixtures"
    fixtures.mkdir(parents=True)
    (fixtures / "rbac_route_permissions.json").write_text(
        '{"GET /api/v1/jobs": "job.read", "POST /api/v1/jobs": "job.write"}', encoding="utf-8",
    )

    names = {s.name for s in UniversalDataFeeder(default_upload_dirs(tmp_path)).scan_uploads()}

    assert names == {"orders"}


# --- every stop point is exercised (review mutants M6, M7, M8, M15) --------


def _stub_phases(agent, monkeypatch, calls: list[str], *, after_ingest=None):
    def feed_all(*a, **k):
        calls.append("feed")
        return []

    def ingest(docs, **k):
        calls.append("ingest")
        if after_ingest is not None:
            after_ingest()
        return len(docs)

    def save_artifacts(*a, **k):
        calls.append("save")
        return {}

    monkeypatch.setattr(agent.feeder, "feed_all", feed_all)
    monkeypatch.setattr(agent.conversation_synth, "synthesize_full", lambda *a, **k: [])
    monkeypatch.setattr(agent.data_synth, "synthesize_full_dataset", lambda *a, **k: {})
    monkeypatch.setattr(agent, "_ingest_training_documents_batched", ingest)
    monkeypatch.setattr(agent, "_save_artifacts", save_artifacts)
    monkeypatch.setattr("src.services.catalog_service.catalog_training_docs", lambda: [])


def test_stop_before_the_run_starts_reads_no_data(tmp_path, monkeypatch):
    """Phase 1 check (M15): a stop already requested never feeds a schema."""
    agent = _bare_agent(tmp_path, monkeypatch)
    calls: list[str] = []
    _stub_phases(agent, monkeypatch, calls)
    stop = threading.Event()
    stop.set()

    run = agent.run_full_training(False, True, stop_event=stop)

    assert run.status == "stopped"
    assert calls == []


def test_stop_during_ingest_writes_no_artifacts(tmp_path, monkeypatch):
    """Phase 5 check (M6): a stop that lands during the last ingest batch."""
    agent = _bare_agent(tmp_path, monkeypatch)
    calls: list[str] = []
    stop = threading.Event()
    _stub_phases(agent, monkeypatch, calls, after_ingest=stop.set)

    run = agent.run_full_training(False, True, stop_event=stop)

    assert run.status == "stopped"
    assert calls[-1] == "ingest" and "save" not in calls, calls
    assert sorted(os.listdir(agent.OUTPUT_DIR)) == []


def test_ingest_stops_between_batches(tmp_path, monkeypatch):
    """Ingest batch check (M7): no batch is sent after the stop request."""
    from src.ai.training import training_agent
    from src.ai.training.training_runtime import TrainingStopped

    agent = _bare_agent(tmp_path, monkeypatch)
    stop = threading.Event()
    batches: list[int] = []

    class _Store:
        def add_documents(self, texts, metadatas, ids):
            batches.append(len(texts))
            stop.set()

    monkeypatch.setattr("src.ai.rag.vector_store.get_vector_store", lambda: _Store())
    docs = [{"id": f"d{i}", "text": "t", "metadata": {}} for i in range(training_agent.BATCH_SIZE * 3)]

    with pytest.raises(TrainingStopped):
        agent._ingest_training_documents_batched(docs, stop_event=stop)

    assert batches == [training_agent.BATCH_SIZE]


def test_conversation_synthesis_stops_between_steps_and_schemas(monkeypatch):
    """Conversation synthesis checks (M8)."""
    from src.ai.training.conversation_synthesis import ConversationSynthesizer
    from src.ai.training.training_runtime import TrainingStopped

    synth = ConversationSynthesizer()
    stop = threading.Event()
    steps: list[str] = []
    for step in (
        "synthesize_base_templates", "synthesize_from_industry_schemas",
        "synthesize_from_semantic_patterns", "synthesize_agent_actions", "synthesize_from_catalog",
    ):
        monkeypatch.setattr(synth, step, lambda *a, _s=step, **k: steps.append(_s) or [])

    def from_schema(*a, **k):
        steps.append("schema")
        stop.set()
        return []

    monkeypatch.setattr(synth, "synthesize_from_schema", from_schema)
    schemas = [{"name": f"s{i}", "columns": ["a"]} for i in range(3)]

    with pytest.raises(TrainingStopped):
        synth.synthesize_full(schemas, stop_event=stop)
    assert steps[-2:] == ["synthesize_from_catalog", "schema"], steps

    stop.clear()
    steps.clear()
    monkeypatch.setattr(synth, "synthesize_base_templates", lambda *a, **k: stop.set() or [])
    with pytest.raises(TrainingStopped):
        synth.synthesize_full(schemas, stop_event=stop)
    assert steps == [], steps


def test_data_synthesis_stops_between_datasets(monkeypatch):
    """Semantic data synthesis checks (M8)."""
    from src.ai.training.data_synthesis import DataTransferDataSynthesizer
    from src.ai.training.training_runtime import TrainingStopped

    synth = DataTransferDataSynthesizer()
    stop = threading.Event()
    built: list[str] = []

    def build(name):
        def _build(*a, **k):
            built.append(name)
            stop.set()
            return object()
        return _build

    for name in (
        "synthesize_column_classification", "synthesize_column_mapping",
        "synthesize_synonym_recognition", "synthesize_pii_detection",
    ):
        monkeypatch.setattr(synth, name, build(name))

    with pytest.raises(TrainingStopped):
        synth.synthesize_full_dataset(stop_event=stop)

    assert built == ["synthesize_column_classification"]
