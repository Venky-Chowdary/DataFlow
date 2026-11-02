"""Save and load the first-party attention+copy seq2seq as ``pilot_lm_v1.npz``."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .seq2seq_lm import HID, AttentionCopyDecoder
from .transformer_lm import Vocab

VERSION = 3
ARTIFACT_NAME = "pilot_lm_v1.npz"


def default_lm_path() -> Path:
    return Path(__file__).resolve().parent / "artifacts" / ARTIFACT_NAME


@dataclass
class FirstPartyLm:
    vocab: Vocab
    decoder: AttentionCopyDecoder
    version: int = VERSION

    def save(self, path: Path | None = None) -> Path:
        dest = path or default_lm_path()
        dest.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            dest,
            version=np.asarray([self.version], dtype=np.int32),
            hid=np.asarray([self.decoder.hid], dtype=np.int32),
            tokens=np.asarray(self.vocab.id_to_token, dtype=object),
            tok_emb=self.decoder.tok_emb.astype(np.float32),
            wz=self.decoder.wz.astype(np.float32),
            uz=self.decoder.uz.astype(np.float32),
            wr=self.decoder.wr.astype(np.float32),
            ur=self.decoder.ur.astype(np.float32),
            wh=self.decoder.wh.astype(np.float32),
            uh=self.decoder.uh.astype(np.float32),
            bz=self.decoder.bz.astype(np.float32),
            br=self.decoder.br.astype(np.float32),
            bh=self.decoder.bh.astype(np.float32),
            wa=self.decoder.wa.astype(np.float32),
            wp=self.decoder.wp.astype(np.float32),
            bp=self.decoder.bp.astype(np.float32),
        )
        return dest

    def generate_grounded(self, question: str, context: str) -> str:
        return (self.decoder.generate(self.vocab, question, context) or "").strip()


def load_lm(path: Path | None = None) -> FirstPartyLm:
    src = path or default_lm_path()
    data = np.load(src, allow_pickle=True)
    version = int(data["version"][0])
    if version != VERSION:
        raise ValueError(f"unsupported first-party LM version {version}")
    tokens = [str(t) for t in data["tokens"].tolist()]
    vocab = Vocab(token_to_id={t: i for i, t in enumerate(tokens)}, id_to_token=tokens)
    decoder = AttentionCopyDecoder(
        tok_emb=data["tok_emb"].astype(np.float64),
        wz=data["wz"].astype(np.float64),
        uz=data["uz"].astype(np.float64),
        wr=data["wr"].astype(np.float64),
        ur=data["ur"].astype(np.float64),
        wh=data["wh"].astype(np.float64),
        uh=data["uh"].astype(np.float64),
        bz=data["bz"].astype(np.float64),
        br=data["br"].astype(np.float64),
        bh=data["bh"].astype(np.float64),
        wa=data["wa"].astype(np.float64),
        wp=data["wp"].astype(np.float64),
        bp=np.asarray(data["bp"], dtype=np.float64).reshape(-1),
        hid=int(data["hid"][0]),
    )
    return FirstPartyLm(vocab=vocab, decoder=decoder, version=version)


def train_lm(
    *,
    seed: int = 7,
    epochs: int = 80,
    lr: float = 0.032,
) -> FirstPartyLm:
    """Overfit in-domain dialogue (repeated) plus a slice of product copy."""
    from .lm_dataset import _dialogue_examples, _product_examples

    dialogue = _dialogue_examples()
    product = _product_examples(limit=24)
    rows = dialogue + product
    texts = [r.question + " " + r.context + " " + r.answer for r in rows]
    vocab = Vocab.build(texts)
    decoder = AttentionCopyDecoder.init(vocab.size, hid=HID, seed=seed)
    dates = [row for row in dialogue if "TODAY_UTC" in row.context]
    creates = [row for row in dialogue if "CREATE_CONNECTION" in row.context]
    pipes = [row for row in dialogue if "PIPELINES:" in row.context]
    routes = [row for row in dialogue if row.question.startswith("plan")]
    greets = [
        row
        for row in dialogue
        if row not in dates
        and row not in creates
        and row not in pipes
        and row not in routes
    ]
    rng = np.random.default_rng(seed)

    def _take(pool: list, n: int) -> list:
        if not pool:
            return []
        pick = rng.integers(0, len(pool), size=n)
        return [pool[int(i)] for i in pick]

    for _epoch in range(epochs):
        batch = (
            _take(dates, 16)
            + _take(pipes, 16)
            + _take(creates, 16)
            + _take(greets, 8)
            + _take(routes, 4)
            + _take(product, 6)
        )
        rng.shuffle(batch)
        for row in batch:
            decoder.teacher_force(vocab, row.question, row.context, row.answer, lr=lr)
    return FirstPartyLm(vocab=vocab, decoder=decoder)
