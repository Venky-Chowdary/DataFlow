"""Bahdanau/Luong attention + pointer-generator GRU.

Encoder: each source token (question + packed context) is an embedding.
Decoder: GRU attends over those states, then mixes

* generate from a closed dialogue vocab, and
* copy a source token (See, Liu & Manning, ACL 2017).

That is a real encoder-decoder. It is Datawrap's own weights. It is not
a foundation model and it is not ChatGPT. Constrained decode may only
emit dialogue glue, punctuation, or a token that appears in the pack.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .tokens import DIALOGUE_GLUE, word_tokens
from .transformer_lm import BOS, EOS, UNK, Vocab

HID = 48
MAX_SRC = 72
MAX_OUT = 32
DEFAULT_SEED = 7


def _sigmoid(x: np.ndarray | float) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-np.clip(np.asarray(x, dtype=np.float64), -20.0, 20.0)))


def _softmax(logits: np.ndarray) -> np.ndarray:
    shifted = logits - logits.max()
    exp = np.exp(np.clip(shifted, -40.0, 40.0))
    return exp / (exp.sum() + 1e-8)


def _source_ids(vocab: Vocab, question: str, context: str) -> list[int]:
    ids = [vocab.token_to_id.get(t, UNK) for t in word_tokens(f"{question} {context}")]
    ids = [i for i in ids if i != UNK][:MAX_SRC]
    return ids or [UNK]


def dialogue_allowed_ids(vocab: Vocab, question: str, context: str) -> set[int]:
    allowed = vocab.allowed_ids(f"{context} {question}")
    allowed.add(EOS)
    for word in DIALOGUE_GLUE:
        idx = vocab.token_to_id.get(word)
        if idx is not None:
            allowed.add(idx)
    return allowed


@dataclass
class AttentionCopyDecoder:
    """GRU decoder with Luong attention and a generate/copy mix."""

    tok_emb: np.ndarray
    wz: np.ndarray
    uz: np.ndarray
    wr: np.ndarray
    ur: np.ndarray
    wh: np.ndarray
    uh: np.ndarray
    bz: np.ndarray
    br: np.ndarray
    bh: np.ndarray
    wa: np.ndarray
    wp: np.ndarray
    bp: np.ndarray
    hid: int = HID

    @classmethod
    def init(
        cls, vocab_size: int, *, hid: int = HID, seed: int = DEFAULT_SEED
    ) -> AttentionCopyDecoder:
        rng = np.random.default_rng(seed)
        inn = hid * 2  # prev_emb + attention context
        scale = 0.08
        return cls(
            tok_emb=rng.normal(0.0, scale, (vocab_size, hid)).astype(np.float64),
            wz=rng.normal(0.0, scale, (hid, inn)).astype(np.float64),
            uz=rng.normal(0.0, scale, (hid, hid)).astype(np.float64),
            wr=rng.normal(0.0, scale, (hid, inn)).astype(np.float64),
            ur=rng.normal(0.0, scale, (hid, hid)).astype(np.float64),
            wh=rng.normal(0.0, scale, (hid, inn)).astype(np.float64),
            uh=rng.normal(0.0, scale, (hid, hid)).astype(np.float64),
            bz=np.zeros(hid, dtype=np.float64),
            br=np.zeros(hid, dtype=np.float64),
            bh=np.zeros(hid, dtype=np.float64),
            wa=rng.normal(0.0, scale, (hid, hid)).astype(np.float64),
            wp=rng.normal(0.0, 0.05, (hid * 3,)).astype(np.float64),
            # Prefer copy at init so slot values (dates, counts) win early.
            bp=np.asarray([-0.45], dtype=np.float64),
            hid=hid,
        )

    def _encode(self, vocab: Vocab, question: str, context: str) -> tuple[list[int], np.ndarray]:
        ids = _source_ids(vocab, question, context)
        return ids, self.tok_emb[np.asarray(ids, dtype=np.int32)]

    def _attend(self, h: np.ndarray, enc: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        proj = self.wa @ h
        scores = enc @ proj
        weights = _softmax(scores)
        ctx = weights @ enc
        return ctx, weights, proj

    def _gru(self, h: np.ndarray, prev: np.ndarray, ctx: np.ndarray):
        x = np.concatenate([prev, ctx])
        z = _sigmoid(self.wz @ x + self.uz @ h + self.bz)
        r = _sigmoid(self.wr @ x + self.ur @ h + self.br)
        n = np.tanh(self.wh @ x + r * (self.uh @ h) + self.bh)
        h2 = (1.0 - z) * n + z * h
        return h2, x, z, r, n

    def step(self, vocab: Vocab, h: np.ndarray, prev: np.ndarray, enc: np.ndarray, enc_ids: list[int]):
        ctx, attn, proj = self._attend(h, enc)
        h2, x, z, r, n = self._gru(h, prev, ctx)
        gen_logits = h2 @ self.tok_emb.T
        gen = _softmax(gen_logits)
        gate_in = np.concatenate([h2, ctx, prev])
        p_gen = float(_sigmoid(float(self.wp @ gate_in + self.bp[0])))
        copy = np.zeros(self.tok_emb.shape[0], dtype=np.float64)
        for i, tid in enumerate(enc_ids):
            copy[tid] += attn[i]
        mix = p_gen * gen + (1.0 - p_gen) * copy
        snap = {
            "enc": enc,
            "enc_ids": enc_ids,
            "h": h,
            "h2": h2,
            "x": x,
            "z": z,
            "r": r,
            "n": n,
            "ctx": ctx,
            "attn": attn,
            "proj": proj,
            "prev": prev,
            "p_gen": p_gen,
            "gen": gen,
            "gate_in": gate_in,
        }
        return h2, mix, snap

    def teacher_force(
        self, vocab: Vocab, question: str, context: str, answer: str, *, lr: float
    ) -> float:
        enc_ids, enc = self._encode(vocab, question, context)
        gold = vocab.encode_words(answer)[: MAX_OUT - 1] + [EOS]
        if not gold:
            return 0.0
        h = np.zeros(self.hid, dtype=np.float64)
        prev = self.tok_emb[BOS]
        cache: list[tuple] = []
        loss = 0.0
        for target in gold:
            h, mix, snap = self.step(vocab, h, prev, enc, enc_ids)
            loss += float(-np.log(np.clip(mix[target], 1e-8, 1.0)))
            dmix = mix.copy()
            dmix[target] -= 1.0
            cache.append((target, dmix, snap))
            prev = self.tok_emb[target]
        d_enc = np.zeros_like(enc)
        dh = np.zeros(self.hid, dtype=np.float64)
        for target, dmix, snap in reversed(cache):
            dh = self._backward_step(target, dmix, snap, d_enc, dh, lr)
        for i, tid in enumerate(enc_ids):
            self.tok_emb[tid] -= np.clip(lr * d_enc[i], -0.08, 0.08)
        return loss / max(len(gold), 1)

    def _backward_step(
        self,
        target: int,
        dmix: np.ndarray,
        snap: dict,
        d_enc: np.ndarray,
        dh_next: np.ndarray,
        lr: float,
    ) -> np.ndarray:
        hid = self.hid
        enc = snap["enc"]
        enc_ids = snap["enc_ids"]
        h = snap["h"]
        h2 = snap["h2"]
        x = snap["x"]
        z = snap["z"]
        r = snap["r"]
        n = snap["n"]
        ctx = snap["ctx"]
        attn = snap["attn"]
        proj = snap["proj"]
        prev = snap["prev"]
        p_gen = snap["p_gen"]
        gen = snap["gen"]
        gate_in = snap["gate_in"]

        d_gen = dmix * p_gen
        d_copy = dmix * (1.0 - p_gen)
        copy = np.zeros_like(gen)
        for i, tid in enumerate(enc_ids):
            copy[tid] += attn[i]
        d_pgen = float(dmix @ (gen - copy))

        d_logits = gen * (d_gen - float(np.dot(d_gen, gen)))
        self.tok_emb -= np.clip(lr * np.outer(d_logits, h2), -0.08, 0.08)
        dh2 = self.tok_emb.T @ d_logits + dh_next

        d_sig = d_pgen * p_gen * (1.0 - p_gen)
        self.wp -= np.clip(lr * d_sig * gate_in, -0.08, 0.08)
        self.bp -= np.clip(lr * np.asarray([d_sig]), -0.08, 0.08)
        dh2 = dh2 + d_sig * self.wp[:hid]
        d_ctx = d_sig * self.wp[hid : 2 * hid]
        d_prev = d_sig * self.wp[2 * hid :]

        d_attn = enc @ d_ctx
        for i, tid in enumerate(enc_ids):
            d_attn[i] += d_copy[tid]
        d_scores = attn * (d_attn - float(np.dot(d_attn, attn)))
        d_proj = enc.T @ d_scores
        d_enc += np.outer(attn, d_ctx) + np.outer(d_scores, proj)
        self.wa -= np.clip(lr * np.outer(d_proj, h), -0.08, 0.08)
        dh = self.wa.T @ d_proj

        # h2 = (1-z)*n + z*h
        dz = dh2 * (h - n)
        dn = dh2 * (1.0 - z)
        dh = dh + dh2 * z
        dn_pre = dn * (1.0 - n * n)
        uh_h = self.uh @ h
        self.wh -= np.clip(lr * np.outer(dn_pre, x), -0.08, 0.08)
        self.bh -= np.clip(lr * dn_pre, -0.08, 0.08)
        self.uh -= np.clip(lr * np.outer(dn_pre * r, h), -0.08, 0.08)
        dr = dn_pre * uh_h
        dh = dh + self.uh.T @ (dn_pre * r)

        dz_pre = dz * z * (1.0 - z)
        dr_pre = dr * r * (1.0 - r)
        self.wz -= np.clip(lr * np.outer(dz_pre, x), -0.08, 0.08)
        self.uz -= np.clip(lr * np.outer(dz_pre, h), -0.08, 0.08)
        self.bz -= np.clip(lr * dz_pre, -0.08, 0.08)
        self.wr -= np.clip(lr * np.outer(dr_pre, x), -0.08, 0.08)
        self.ur -= np.clip(lr * np.outer(dr_pre, h), -0.08, 0.08)
        self.br -= np.clip(lr * dr_pre, -0.08, 0.08)
        dh = dh + self.uz.T @ dz_pre + self.ur.T @ dr_pre

        dx = self.wz.T @ dz_pre + self.wr.T @ dr_pre + self.wh.T @ dn_pre
        d_prev = d_prev + dx[:hid]
        d_enc += np.outer(attn, dx[hid:])
        self.tok_emb[target] -= np.clip(lr * 0.2 * (dh2 + d_prev), -0.08, 0.08)
        return dh

    def generate(self, vocab: Vocab, question: str, context: str) -> str:
        enc_ids, enc = self._encode(vocab, question, context)
        allowed = dialogue_allowed_ids(vocab, question, context)
        h = np.zeros(self.hid, dtype=np.float64)
        prev = self.tok_emb[BOS]
        out: list[int] = []
        last = -1
        for _ in range(MAX_OUT):
            h, mix, _snap = self.step(vocab, h, prev, enc, enc_ids)
            for idx in range(mix.shape[0]):
                if idx not in allowed:
                    mix[idx] = 0.0
            if last >= 0:
                mix[last] *= 0.15
            if mix.sum() <= 1e-12:
                break
            mix = mix / mix.sum()
            nxt = int(np.argmax(mix))
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


# Older train/serve imports.
GruDecoder = AttentionCopyDecoder
