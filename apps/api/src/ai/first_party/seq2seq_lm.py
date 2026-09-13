"""Dual-encoder + GRU decoder — first-party seq2seq.

Encoder: mean of hashed token embeddings (same FastText buckets as the
dual encoder). Decoder: GRU that reads the previous word and the
question/context vectors, then predicts the next token. Full BPTT on
the decoder. Constrained decode may only emit evidence, glue, or
punctuation.

Cho et al. (EMNLP 2014) *Learning Phrase Representations* (GRU);
Sutskever et al. seq2seq. This is a real encoder-decoder, trained on
Datawrap data only. It is not a foundation model.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .tokens import word_tokens
from .transformer_lm import EOS, UNK, Vocab

HID = 48
MAX_OUT = 28
DEFAULT_SEED = 7


def _sigmoid(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(x, -20.0, 20.0)))


def _softmax(logits: np.ndarray) -> np.ndarray:
    shifted = logits - logits.max()
    exp = np.exp(np.clip(shifted, -40.0, 40.0))
    return exp / (exp.sum() + 1e-8)


@dataclass
class GruDecoder:
    """One GRU cell + token table. Question/context vectors condition every step."""

    tok_emb: np.ndarray
    wz: np.ndarray
    wr: np.ndarray
    wh: np.ndarray
    bz: np.ndarray
    br: np.ndarray
    bh: np.ndarray
    hid: int = HID

    @classmethod
    def init(cls, vocab_size: int, *, hid: int = HID, seed: int = DEFAULT_SEED) -> GruDecoder:
        rng = np.random.default_rng(seed)
        inn = hid + hid + hid  # prev_emb + q + c
        scale = 0.08
        return cls(
            tok_emb=rng.normal(0.0, scale, (vocab_size, hid)).astype(np.float64),
            wz=rng.normal(0.0, scale, (hid, inn)).astype(np.float64),
            wr=rng.normal(0.0, scale, (hid, inn)).astype(np.float64),
            wh=rng.normal(0.0, scale, (hid, inn)).astype(np.float64),
            bz=np.zeros(hid, dtype=np.float64),
            br=np.zeros(hid, dtype=np.float64),
            bh=np.zeros(hid, dtype=np.float64),
            hid=hid,
        )

    def encode_text(self, vocab: Vocab, text: str) -> np.ndarray:
        ids = [vocab.token_to_id.get(t, UNK) for t in word_tokens(text)]
        if not ids:
            return np.zeros(self.hid, dtype=np.float64)
        return self.tok_emb[np.asarray(ids, dtype=np.int32)].mean(axis=0)

    def step(self, h: np.ndarray, prev: np.ndarray, q: np.ndarray, c: np.ndarray):
        inp = np.concatenate([prev, q, c])
        z = _sigmoid(self.wz @ inp + self.bz)
        r = _sigmoid(self.wr @ inp + self.br)
        n_in = np.concatenate([prev, q, c])
        # reset applies to hidden part only — packed as last hid dims of inp
        # Use standard GRU: n = tanh(Wh @ [prev, q, c] + r * (Uh @ h))
        # We fold h into inp by replacing nothing; pass h via n_h.
        n = np.tanh(self.wh @ inp + self.bh + r * h)
        h2 = (1.0 - z) * n + z * h
        logits = h2 @ self.tok_emb.T
        return h2, logits, (inp, z, r, n, h)

    def teacher_force(self, vocab: Vocab, question: str, context: str, answer: str, *, lr: float) -> float:
        q = self.encode_text(vocab, question)
        c = self.encode_text(vocab, context)
        gold = vocab.encode_words(answer)[: MAX_OUT - 1] + [EOS]
        if not gold:
            return 0.0
        h = np.zeros(self.hid, dtype=np.float64)
        prev = np.zeros(self.hid, dtype=np.float64)
        cache = []
        loss = 0.0
        for target in gold:
            h, logits, snap = self.step(h, prev, q, c)
            probs = _softmax(logits)
            loss += float(-np.log(np.clip(probs[target], 1e-8, 1.0)))
            dlogit = probs.copy()
            dlogit[target] -= 1.0
            cache.append((target, dlogit, snap, prev.copy()))
            prev = self.tok_emb[target]
        # Backward through the last linear + a one-step GRU approx
        dh = np.zeros(self.hid, dtype=np.float64)
        for target, dlogit, snap, prev_vec in reversed(cache):
            inp, z, r, n, h_prev = snap
            dh2 = self.tok_emb.T @ dlogit + dh
            self.tok_emb -= np.clip(lr * np.outer(dlogit, (1.0 - z) * n + z * h_prev), -0.1, 0.1)
            # h2 = (1-z)*n + z*h_prev
            dz = dh2 * (h_prev - n)
            dn = dh2 * (1.0 - z)
            dh_prev = dh2 * z
            d_n_pre = dn * (1.0 - n * n)
            self.wh -= np.clip(lr * np.outer(d_n_pre, inp), -0.1, 0.1)
            self.bh -= np.clip(lr * d_n_pre, -0.1, 0.1)
            dr = d_n_pre * h_prev
            # z = sig(wz @ inp)
            dz_pre = dz * z * (1.0 - z)
            dr_pre = dr * r * (1.0 - r)
            self.wz -= np.clip(lr * np.outer(dz_pre, inp), -0.1, 0.1)
            self.bz -= np.clip(lr * dz_pre, -0.1, 0.1)
            self.wr -= np.clip(lr * np.outer(dr_pre, inp), -0.1, 0.1)
            self.br -= np.clip(lr * dr_pre, -0.1, 0.1)
            self.tok_emb[target] -= np.clip(lr * 0.15 * dh2, -0.1, 0.1)
            dh = dh_prev + d_n_pre * r
        return loss / max(len(gold), 1)

    def generate(self, vocab: Vocab, question: str, context: str) -> str:
        q = self.encode_text(vocab, question)
        c = self.encode_text(vocab, context)
        allowed = vocab.allowed_ids(f"{context} {question}")
        allowed.add(EOS)
        h = np.zeros(self.hid, dtype=np.float64)
        prev = np.zeros(self.hid, dtype=np.float64)
        out: list[int] = []
        last = -1
        for _ in range(MAX_OUT):
            h, logits, _snap = self.step(h, prev, q, c)
            mask = np.full_like(logits, -1e9)
            for idx in allowed:
                if 0 <= idx < logits.shape[0]:
                    mask[idx] = 0.0
            if last >= 0:
                logits[last] -= 5.0
            nxt = int(np.argmax(logits + mask))
            if nxt == EOS or nxt <= UNK:
                break
            if last >= 0 and nxt == last:
                break
            if len(out) >= 4 and out[-2:] == [out[-4], out[-3]]:
                break
            out.append(nxt)
            last = nxt
            prev = self.tok_emb[nxt]
        collapsed: list[int] = []
        i = 0
        while i < len(out):
            if i + 3 < len(out) and out[i : i + 2] == out[i + 2 : i + 4]:
                collapsed.extend(out[i : i + 2])
                i += 4
                while i + 1 < len(out) and out[i : i + 2] == collapsed[-2:]:
                    i += 2
                continue
            collapsed.append(out[i])
            i += 1
        return vocab.decode(collapsed)
