"""Save and load the first-party GRU seq2seq as ``pilot_lm_v1.npz``."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .seq2seq_lm import HID, GruDecoder
from .transformer_lm import Vocab

VERSION = 2
ARTIFACT_NAME = "pilot_lm_v1.npz"


def default_lm_path() -> Path:
    return Path(__file__).resolve().parent / "artifacts" / ARTIFACT_NAME


@dataclass
class FirstPartyLm:
    vocab: Vocab
    decoder: GruDecoder
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
            wr=self.decoder.wr.astype(np.float32),
            wh=self.decoder.wh.astype(np.float32),
            bz=self.decoder.bz.astype(np.float32),
            br=self.decoder.br.astype(np.float32),
            bh=self.decoder.bh.astype(np.float32),
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
    decoder = GruDecoder(
        tok_emb=data["tok_emb"].astype(np.float64),
        wz=data["wz"].astype(np.float64),
        wr=data["wr"].astype(np.float64),
        wh=data["wh"].astype(np.float64),
        bz=data["bz"].astype(np.float64),
        br=data["br"].astype(np.float64),
        bh=data["bh"].astype(np.float64),
        hid=int(data["hid"][0]),
    )
    return FirstPartyLm(vocab=vocab, decoder=decoder, version=version)


def train_lm(
    *,
    seed: int = 7,
    epochs: int = 18,
    lr: float = 0.045,
) -> FirstPartyLm:
    """Overfit the in-domain dialogue + a slice of product copy. Measured, not claimed."""
    from .lm_dataset import _dialogue_examples, _product_examples

    dialogue = _dialogue_examples()
    product = _product_examples(limit=40)
    rows = dialogue + product
    texts = [r.question + " " + r.context + " " + r.answer for r in rows]
    vocab = Vocab.build(texts)
    decoder = GruDecoder.init(vocab.size, hid=HID, seed=seed)
    train_rows = dialogue * 4 + product
    rng = np.random.default_rng(seed)
    for _epoch in range(epochs):
        order = np.arange(len(train_rows))
        rng.shuffle(order)
        for idx in order:
            row = train_rows[int(idx)]
            decoder.teacher_force(vocab, row.question, row.context, row.answer, lr=lr)
    return FirstPartyLm(vocab=vocab, decoder=decoder)
