"""Save and load the first-party weights as a single ``npz``.

The checkpoint is the contract between ``train_first_party_pilot.py`` and
the serve path. It is not a HuggingFace model card and it is not a
foundation-model dump: hashed embeddings, two projection matrices, a
sentence pointer, a prefix classifier, and the gold questions the
rewriter may snap to.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .dual_encoder import DIM, DualEncoder
from .pointer_gen import PointerGenerator
from .tokens import HASH_SIZE

VERSION = 1
ARTIFACT_NAME = "pilot_fp_v1.npz"


def default_artifact_path() -> Path:
    return Path(__file__).resolve().parent / "artifacts" / ARTIFACT_NAME


@dataclass
class FirstPartyCheckpoint:
    """In-memory bundle the serve engine loads once per process."""

    encoder: DualEncoder
    generator: PointerGenerator
    gold_questions: tuple[str, ...]
    gold_vectors: np.ndarray
    version: int = VERSION

    def save(self, path: Path | None = None) -> Path:
        dest = path or default_artifact_path()
        dest.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            dest,
            version=np.asarray([self.version], dtype=np.int32),
            dim=np.asarray([self.encoder.dim], dtype=np.int32),
            hash_size=np.asarray([self.encoder.hash_size], dtype=np.int32),
            embeddings=self.encoder.embeddings.astype(np.float32),
            query_proj=self.encoder.query_proj.astype(np.float32),
            gold_proj=self.encoder.gold_proj.astype(np.float32),
            select=self.generator.select.astype(np.float32),
            prefix=self.generator.prefix.astype(np.float32),
            prefix_bias=self.generator.prefix_bias.astype(np.float32),
            gold_questions=np.asarray(self.gold_questions, dtype=object),
            gold_vectors=self.gold_vectors.astype(np.float32),
        )
        return dest


def load_checkpoint(path: Path | None = None) -> FirstPartyCheckpoint:
    src = path or default_artifact_path()
    data = np.load(src, allow_pickle=True)
    version = int(data["version"][0])
    if version != VERSION:
        raise ValueError(f"unsupported first-party checkpoint version {version}")
    dim = int(data["dim"][0])
    hash_size = int(data["hash_size"][0])
    encoder = DualEncoder(
        embeddings=data["embeddings"].astype(np.float64),
        query_proj=data["query_proj"].astype(np.float64),
        gold_proj=data["gold_proj"].astype(np.float64),
        dim=dim,
        hash_size=hash_size,
    )
    generator = PointerGenerator(
        select=data["select"].astype(np.float64),
        prefix=data["prefix"].astype(np.float64),
        prefix_bias=data["prefix_bias"].astype(np.float64),
        dim=dim,
    )
    golds = tuple(str(q) for q in data["gold_questions"].tolist())
    vectors = data["gold_vectors"].astype(np.float64)
    return FirstPartyCheckpoint(
        encoder=encoder,
        generator=generator,
        gold_questions=golds,
        gold_vectors=vectors,
        version=version,
    )


def train_checkpoint(
    *,
    seed: int = 7,
    encoder_epochs: int = 8,
    generator_epochs: int = 4,
) -> FirstPartyCheckpoint:
    """Fit both heads on the product corpus and freeze gold embeddings."""
    from .dataset import align_pairs, copy_examples, gold_questions

    encoder = DualEncoder.init(dim=DIM, hash_size=HASH_SIZE, seed=seed)
    pairs = [(p.query, p.gold) for p in align_pairs()]
    encoder.train_infonce(pairs, epochs=encoder_epochs, seed=seed)

    generator = PointerGenerator.init(dim=DIM, seed=seed)
    copies = [(e.question, e.evidence, e.answer) for e in copy_examples()]
    generator.train(encoder, copies, epochs=generator_epochs)

    golds = gold_questions()
    vectors = encoder.encode_golds(list(golds)) if golds else np.zeros((0, DIM))
    return FirstPartyCheckpoint(
        encoder=encoder,
        generator=generator,
        gold_questions=golds,
        gold_vectors=vectors,
    )
