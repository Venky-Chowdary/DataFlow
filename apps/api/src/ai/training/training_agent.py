"""
DataTransfer.space — Training Agent

Separate agent dedicated to training copilot models from universal data.
Feeds schemas, synthesizes conversations, updates RAG knowledge base.
"""

from __future__ import annotations
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

from .universal_data_feeder import UniversalDataFeeder
from .conversation_synthesis import ConversationSynthesizer
from .data_synthesis import DataTransferDataSynthesizer
from .fine_tuning import DataTransferFineTuningPipeline
from ..knowledge.copilot_knowledge import get_copilot_documents


CHECKPOINT_FILE = "training_checkpoint.json"
BATCH_SIZE = 200
MAX_GROUNDED_SCHEMAS = 40


@dataclass
class TrainingRun:
    id: str
    status: str = "pending"
    started_at: str = ""
    completed_at: str = ""
    metrics: dict = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


class DataTransferTrainingAgent:
    """
    Dedicated training agent — separate from the customer-facing Copilot.
    Ingests universal data (620+ connectors, industry templates, uploads),
    synthesizes conversations, updates RAG, exports OpenAI/Anthropic datasets.
    """

    OUTPUT_DIR = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "data", "training", "copilot"
    )

    def __init__(self):
        self.feeder = UniversalDataFeeder()
        self.conversation_synth = ConversationSynthesizer()
        self.data_synth = DataTransferDataSynthesizer()
        self.fine_tune = DataTransferFineTuningPipeline()
        self._runs: list[TrainingRun] = []
        self._last_run: TrainingRun | None = None
        os.makedirs(self.OUTPUT_DIR, exist_ok=True)

    def should_skip_training(self, min_docs: int = 400, max_age_hours: int = 6) -> bool:
        """Skip retrain if recently completed and vector store is populated."""
        checkpoint = self._load_checkpoint()
        if not checkpoint.get("completed_at"):
            return False
        try:
            completed = datetime.fromisoformat(checkpoint["completed_at"].replace("Z", "+00:00"))
            age = datetime.now(timezone.utc) - completed
            if age > timedelta(hours=max_age_hours):
                return False
        except Exception:
            return False
        try:
            from ..rag.vector_store import get_vector_store
            if get_vector_store().document_count >= min_docs:
                return True
        except Exception:
            pass
        return False

    def run_full_training(self, include_embedding_tune: bool = False, force: bool = False) -> TrainingRun:
        """Execute complete training pipeline from universal data."""
        import uuid

        if not force and self.should_skip_training():
            skip = TrainingRun(
                id="skip",
                status="skipped",
                started_at=datetime.now(timezone.utc).isoformat(),
                completed_at=datetime.now(timezone.utc).isoformat(),
                metrics={"reason": "recent_training", "checkpoint": self._load_checkpoint()},
            )
            self._last_run = skip
            return skip

        run = TrainingRun(
            id=str(uuid.uuid4())[:8],
            status="running",
            started_at=datetime.now(timezone.utc).isoformat(),
        )
        self._runs.append(run)
        self._last_run = run

        try:
            # Phase 1: Collect universal data (620+ connector profiles + industry + uploads)
            schemas = self.feeder.feed_all()
            schema_dicts = self.feeder.to_training_dicts(schemas)
            run.metrics["universal_schemas"] = len(schemas)
            run.metrics["upload_schemas"] = sum(1 for s in schemas if s.source == "upload")
            run.metrics["catalog_schemas"] = sum(1 for s in schemas if s.source == "catalog")
            run.metrics["industry_schemas"] = sum(1 for s in schemas if s.source == "industry")

            try:
                from .universal_source_registry import get_universal_schema_count
                run.metrics["registry"] = get_universal_schema_count()
            except Exception:
                pass

            # Phase 2: Synthesize conversation training (includes all 620 connector Q&A)
            conversations = self.conversation_synth.synthesize_full(schema_dicts)
            run.metrics["conversation_examples"] = len(conversations)
            run.metrics["agent_action_examples"] = sum(
                1 for c in conversations if c.context.get("tool")
            )

            # Phase 2b: Data-grounded NL from real analysis (uploads + industry, capped)
            from ..copilot.data_analyst import get_data_analyst
            analyst = get_data_analyst()
            grounded_schemas = [
                s for s in schemas
                if s.source in ("upload", "industry")
            ][:MAX_GROUNDED_SCHEMAS]
            if len(grounded_schemas) < MAX_GROUNDED_SCHEMAS:
                grounded_schemas.extend(
                    [s for s in schemas if s.source == "catalog"][: MAX_GROUNDED_SCHEMAS - len(grounded_schemas)]
                )

            data_grounded = []
            for schema in grounded_schemas:
                insight = analyst.analyze_schema(schema)
                for q in [
                    f"Analyze the {schema.name} data",
                    f"What PII is in {schema.name}?",
                    f"Show columns in {schema.name}",
                ]:
                    data_grounded.append({
                        "id": f"data_{schema.name}_{len(data_grounded)}",
                        "text": f"User: {q}\nAssistant: {analyst.compose_response(insight, q, 'mapping_help')}",
                        "metadata": {"type": "copilot_training", "intent": "data_analysis", "dataset": schema.name},
                    })

            conv_docs = self.conversation_synth.to_vector_documents(conversations)
            conv_docs.extend(data_grounded)
            run.metrics["data_grounded_examples"] = len(data_grounded)

            # Phase 2c: Full 620+ connector catalog → RAG
            from ...services.catalog_service import catalog_training_docs
            catalog_docs = catalog_training_docs()
            run.metrics["catalog_docs_total"] = len(catalog_docs)

            # Phase 3: Semantic training data
            semantic_datasets = self.data_synth.synthesize_full_dataset()
            semantic_count = sum(len(d.examples) for d in semantic_datasets.values())
            run.metrics["semantic_examples"] = semantic_count

            # Phase 4: Batch ingest into RAG vector store
            copilot_docs = get_copilot_documents()
            all_docs = copilot_docs + catalog_docs + conv_docs
            ingested = self._ingest_training_documents_batched(all_docs)
            run.metrics["rag_documents_ingested"] = ingested
            run.metrics["catalog_docs_ingested"] = len(catalog_docs)

            # Phase 5: Save artifacts + LLM fine-tune exports
            saved_paths = self._save_artifacts(conversations, conv_docs)
            ft_paths = self.fine_tune.export_llm_finetune_datasets(conversations)
            saved_paths.update(ft_paths)
            run.metrics["artifact_paths"] = saved_paths
            run.metrics["openai_finetune_examples"] = ft_paths.get("openai_count", 0)

            # Phase 6: Optional embedding fine-tune
            if include_embedding_tune:
                job = self.fine_tune.create_job("copilot_embeddings", "embedding_pairs")
                tune_result = self.fine_tune.run_embedding_fine_tune(job, epochs=2)
                run.metrics["embedding_tune"] = tune_result

            # Phase 7: Evaluate copilot readiness
            eval_result = self._evaluate_copilot_readiness(conversations)
            run.metrics["copilot_evaluation"] = eval_result

            run.status = "completed"
            run.completed_at = datetime.now(timezone.utc).isoformat()
            self._save_checkpoint(run)

        except Exception as e:
            run.status = "failed"
            run.errors.append(str(e))
            run.completed_at = datetime.now(timezone.utc).isoformat()

        return run

    def _ingest_training_documents_batched(self, docs: list[dict]) -> int:
        """Ingest documents in batches to avoid memory spikes."""
        from ..rag.vector_store import get_vector_store

        store = get_vector_store()
        total = 0
        for i in range(0, len(docs), BATCH_SIZE):
            batch = docs[i : i + BATCH_SIZE]
            texts = [d["text"] for d in batch]
            metadatas = [d["metadata"] for d in batch]
            ids = [d["id"] for d in batch]
            store.add_documents(texts, metadatas, ids)
            total += len(batch)
        return total

    def _ingest_training_documents(self, conv_docs: list[dict]) -> int:
        return self._ingest_training_documents_batched(conv_docs)

    def _checkpoint_path(self) -> str:
        return os.path.join(self.OUTPUT_DIR, CHECKPOINT_FILE)

    def _load_checkpoint(self) -> dict:
        path = self._checkpoint_path()
        if not os.path.isfile(path):
            return {}
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

    def _save_checkpoint(self, run: TrainingRun) -> None:
        payload = {
            "run_id": run.id,
            "completed_at": run.completed_at,
            "metrics": run.metrics,
        }
        with open(self._checkpoint_path(), "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

    def _save_artifacts(
        self,
        conversations: list,
        conv_docs: list[dict],
    ) -> dict:
        """Persist training data to disk."""
        paths = {}

        conv_path = os.path.join(self.OUTPUT_DIR, "conversations.jsonl")
        with open(conv_path, "w", encoding="utf-8") as f:
            for ex in conversations:
                f.write(json.dumps({
                    "user": ex.user_message,
                    "assistant": ex.assistant_message,
                    "intent": ex.intent,
                    "context": ex.context,
                }) + "\n")
        paths["conversations"] = conv_path

        docs_path = os.path.join(self.OUTPUT_DIR, "vector_documents.jsonl")
        with open(docs_path, "w", encoding="utf-8") as f:
            for doc in conv_docs:
                f.write(json.dumps(doc) + "\n")
        paths["vector_documents"] = docs_path

        feeder_status = self.feeder.get_status()
        status_path = os.path.join(self.OUTPUT_DIR, "feeder_status.json")
        with open(status_path, "w", encoding="utf-8") as f:
            json.dump(feeder_status, f, indent=2)
        paths["feeder_status"] = status_path

        return paths

    def _evaluate_copilot_readiness(self, conversations: list) -> dict:
        """Quick evaluation of training coverage."""
        intents = {}
        for ex in conversations:
            intents[ex.intent] = intents.get(ex.intent, 0) + 1

        from ..knowledge.copilot_knowledge import INTENT_PATTERNS
        covered_intents = set(intents.keys())
        expected_intents = set(INTENT_PATTERNS.keys())
        coverage = len(covered_intents & expected_intents) / len(expected_intents) if expected_intents else 0

        return {
            "total_examples": len(conversations),
            "intent_coverage": round(coverage, 2),
            "intents": intents,
            "ready": coverage >= 0.7 and len(conversations) >= 100,
        }

    def get_status(self) -> dict:
        last = self._last_run
        return {
            "agent": "DataTransferTrainingAgent",
            "checkpoint": self._load_checkpoint(),
            "last_run": {
                "id": last.id,
                "status": last.status,
                "metrics": last.metrics,
                "errors": last.errors,
            } if last else None,
            "feeder": self.feeder.get_status(),
            "output_dir": self.OUTPUT_DIR,
            "total_runs": len(self._runs),
        }

    def ingest_from_transfer(
        self,
        filename: str,
        columns: list[str],
        row_count: int,
        samples: dict[str, list[str]],
    ) -> dict:
        """Learn from a completed transfer — add to RAG + conversation training."""
        from ..copilot.data_analyst import get_data_analyst
        from ..training.conversation_synthesis import ConversationSynthesizer

        synth = ConversationSynthesizer()
        examples = synth.synthesize_from_schema(
            schema_name=filename,
            columns=columns,
            sample_values=samples,
        )
        analyst = get_data_analyst()
        from ..training.universal_data_feeder import UniversalSchema
        insight = analyst.analyze_schema(UniversalSchema(
            name=filename, source="transfer", columns=columns,
            samples=samples, row_count=row_count,
        ))
        docs = synth.to_vector_documents(examples)
        for q in [f"Analyze {filename}", f"What columns are in {filename}?"]:
            docs.append({
                "id": f"transfer_{filename}_{len(docs)}",
                "text": f"User: {q}\nAssistant: {analyst.compose_response(insight, q, 'mapping_help')}",
                "metadata": {"type": "copilot_training", "intent": "data_analysis", "dataset": filename, "source": "transfer"},
            })
        ingested = self._ingest_training_documents(docs)
        return {"ingested": ingested, "examples": len(examples)}


_agent: DataTransferTrainingAgent | None = None


def get_training_agent() -> DataTransferTrainingAgent:
    global _agent
    if _agent is None:
        _agent = DataTransferTrainingAgent()
    return _agent
