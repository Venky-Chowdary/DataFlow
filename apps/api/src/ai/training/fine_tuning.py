"""
DataTransfer.space — Fine-Tuning Pipeline

Prepare datasets and manage fine-tuning workflows.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime

from .data_synthesis import DataTransferDataSynthesizer


@dataclass
class FineTuningJob:
    """A fine-tuning job configuration."""
    id: str
    name: str
    dataset_name: str
    model_base: str = "all-MiniLM-L6-v2"
    status: str = "pending"
    created_at: str = ""
    metrics: dict = field(default_factory=dict)


class DataTransferFineTuningPipeline:
    """Manage fine-tuning dataset preparation and job tracking."""

    DEFAULT_OUTPUT_DIR = os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "data", "training"
    )

    def __init__(self, output_dir: str | None = None):
        self.output_dir = output_dir or self.DEFAULT_OUTPUT_DIR
        self.synthesizer = DataTransferDataSynthesizer()
        self.jobs: list[FineTuningJob] = []
        os.makedirs(self.output_dir, exist_ok=True)

    def prepare_datasets(self) -> dict:
        """Generate and save all training datasets."""
        datasets = self.synthesizer.synthesize_full_dataset()
        saved = {}

        for name, dataset in datasets.items():
            path = os.path.join(self.output_dir, f"{name}.jsonl")
            dataset.save(path)
            saved[name] = {
                "path": path,
                "count": len(dataset.examples),
                "metadata": dataset.metadata,
            }

        # Save combined dataset for embedding fine-tuning
        combined_path = os.path.join(self.output_dir, "combined.jsonl")
        with open(combined_path, "w") as f:
            for dataset in datasets.values():
                f.write(dataset.to_jsonl() + "\n")
        saved["combined"] = {"path": combined_path, "count": sum(len(d.examples) for d in datasets.values())}

        return saved

    def prepare_embedding_pairs(self) -> str:
        """Generate sentence pairs for embedding model fine-tuning."""
        from ..knowledge.synonyms import SYNONYM_DICTIONARY

        pairs = []
        for canonical, synonyms in SYNONYM_DICTIONARY.items():
            for syn in synonyms:
                pairs.append({"anchor": canonical, "positive": syn, "score": 1.0})
                # Hard negatives: random non-synonym
                import random
                other_keys = [k for k in SYNONYM_DICTIONARY if k != canonical]
                if other_keys:
                    neg_key = random.choice(other_keys)
                    neg_syn = random.choice(SYNONYM_DICTIONARY[neg_key])
                    pairs.append({"anchor": syn, "positive": neg_syn, "score": 0.0})

        path = os.path.join(self.output_dir, "embedding_pairs.jsonl")
        with open(path, "w") as f:
            for pair in pairs:
                f.write(json.dumps(pair) + "\n")

        return path

    def create_job(self, name: str, dataset_name: str, model_base: str = "all-MiniLM-L6-v2") -> FineTuningJob:
        """Create a fine-tuning job."""
        import uuid
        job = FineTuningJob(
            id=str(uuid.uuid4()),
            name=name,
            dataset_name=dataset_name,
            model_base=model_base,
            created_at=datetime.utcnow().isoformat(),
        )
        self.jobs.append(job)
        return job

    def run_embedding_fine_tune(self, job: FineTuningJob, epochs: int = 3) -> dict:
        """
        Fine-tune embedding model on synonym pairs.
        Uses sentence-transformers if available.
        """
        job.status = "running"
        pairs_path = self.prepare_embedding_pairs()

        try:
            from sentence_transformers import InputExample, SentenceTransformer, losses
            from torch.utils.data import DataLoader

            model = SentenceTransformer(job.model_base)
            examples = []

            with open(pairs_path) as f:
                for line in f:
                    pair = json.loads(line)
                    if pair["score"] > 0.5:
                        examples.append(InputExample(
                            texts=[pair["anchor"], pair["positive"]],
                            label=1.0,
                        ))

            if not examples:
                job.status = "failed"
                return {"error": "No training examples found"}

            loader = DataLoader(examples, shuffle=True, batch_size=16)
            loss = losses.CosineSimilarityLoss(model)
            model.fit(
                train_objectives=[(loader, loss)],
                epochs=epochs,
                warmup_steps=100,
                show_progress_bar=False,
            )

            output_path = os.path.join(self.output_dir, f"model_{job.id}")
            model.save(output_path)

            job.status = "completed"
            job.metrics = {
                "epochs": epochs,
                "examples": len(examples),
                "output_path": output_path,
            }
            return job.metrics

        except ImportError:
            job.status = "skipped"
            job.metrics = {"reason": "sentence-transformers not installed"}
            return job.metrics
        except Exception as e:
            job.status = "failed"
            job.metrics = {"error": str(e)}
            return job.metrics

    def get_status(self) -> dict:
        return {
            "output_dir": self.output_dir,
            "jobs": [
                {"id": j.id, "name": j.name, "status": j.status, "metrics": j.metrics}
                for j in self.jobs
            ],
        }

    def export_llm_finetune_datasets(self, conversations: list) -> dict:
        """
        Export OpenAI-compatible fine-tuning JSONL from conversation examples.
        Anthropic uses similar message formats for future custom model training.
        """
        from ..knowledge.copilot_knowledge import DATA_PILOT_PERSONA

        openai_path = os.path.join(self.output_dir, "openai_finetune.jsonl")
        anthropic_path = os.path.join(self.output_dir, "anthropic_finetune.jsonl")
        count = 0

        with open(openai_path, "w", encoding="utf-8") as fo, open(anthropic_path, "w", encoding="utf-8") as fa:
            for ex in conversations:
                if not ex.user_message or not ex.assistant_message:
                    continue
                openai_row = {
                    "messages": [
                        {"role": "system", "content": DATA_PILOT_PERSONA},
                        {"role": "user", "content": ex.user_message},
                        {"role": "assistant", "content": ex.assistant_message},
                    ]
                }
                fo.write(json.dumps(openai_row) + "\n")
                anthropic_row = {
                    "system": DATA_PILOT_PERSONA,
                    "messages": [
                        {"role": "user", "content": ex.user_message},
                        {"role": "assistant", "content": ex.assistant_message},
                    ],
                    "metadata": {"intent": ex.intent, **ex.context},
                }
                fa.write(json.dumps(anthropic_row) + "\n")
                count += 1

        return {
            "openai_finetune": openai_path,
            "anthropic_finetune": anthropic_path,
            "openai_count": count,
        }
