"""First-party causal Transformer — a real decoder, not a prefix classifier.

Vaswani et al. (NeurIPS 2017) *Attention Is All You Need*: stacked
masked self-attention + feed-forward, trained as next-token prediction.

This is Datawrap's own weights. It is not a foundation model, not
ChatGPT, and not a regex table. It generates a reply over a packed
context (question + evidence), then the serve path fail-closes if a
claim is ungrounded.

No PyTorch. NumPy only, so air-gapped CI can train and load the same
``npz``. Constrained decode may only emit tokens that appear in the
evidence, closed glue, or punctuation — the model chooses the order
and the wording, not new product facts.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .tokens import GLUE_WORDS, word_tokens

PAD, BOS, EOS, Q, E, A, UNK = 0, 1, 2, 3, 4, 5, 6
SPECIAL = ("<pad>", "<bos>", "<eos>", "<q>", "<e>", "<a>", "<unk>")
PUNCT = (".", ",", ":", "?", "(", ")", "/", "—")

DIM = 48
N_LAYERS = 2
N_HEADS = 4
FF_MULT = 4
MAX_LEN = 96
DEFAULT_SEED = 7


def _softmax_last(logits: np.ndarray) -> np.ndarray:
    shifted = logits - logits.max(axis=-1, keepdims=True)
    exp = np.exp(np.clip(shifted, -40.0, 40.0))
    return exp / (exp.sum(axis=-1, keepdims=True) + 1e-8)


def _layer_norm(x: np.ndarray, eps: float = 1e-5) -> np.ndarray:
    mean = x.mean(axis=-1, keepdims=True)
    var = x.var(axis=-1, keepdims=True)
    return (x - mean) / np.sqrt(var + eps)


def detokenize(words: list[str]) -> str:
    text = " ".join(w for w in words if w and not w.startswith("<"))
    text = (
        text.replace(" .", ".")
        .replace(" ,", ",")
        .replace(" :", ":")
        .replace(" ?", "?")
        .replace(" )", ")")
        .replace("( ", "(")
    )
    return " ".join(text.split())


@dataclass
class Vocab:
    """Word ids learned from the train set. Unknown words become ``<unk>``."""

    token_to_id: dict[str, int]
    id_to_token: list[str]

    @classmethod
    def build(cls, texts: list[str]) -> Vocab:
        seen = list(SPECIAL)
        keys = set(SPECIAL)
        for extra in PUNCT:
            if extra not in keys:
                seen.append(extra)
                keys.add(extra)
        for word in GLUE_WORDS:
            if word not in keys:
                seen.append(word)
                keys.add(word)
        for text in texts:
            for tok in word_tokens(text):
                if tok not in keys:
                    seen.append(tok)
                    keys.add(tok)
            for mark in PUNCT:
                if mark in (text or "") and mark not in keys:
                    seen.append(mark)
                    keys.add(mark)
        return cls(token_to_id={t: i for i, t in enumerate(seen)}, id_to_token=seen)

    @property
    def size(self) -> int:
        return len(self.id_to_token)

    def encode_words(self, text: str) -> list[int]:
        ids: list[int] = []
        raw = (text or "").strip()
        for tok in word_tokens(raw):
            ids.append(self.token_to_id.get(tok, UNK))
        if raw.endswith("?"):
            ids.append(self.token_to_id.get("?", UNK))
        elif raw.endswith("."):
            ids.append(self.token_to_id.get(".", UNK))
        return ids

    def decode(self, ids: list[int]) -> str:
        words = [
            self.id_to_token[i]
            for i in ids
            if 0 <= i < len(self.id_to_token) and i not in {PAD, BOS, EOS, Q, E, A, UNK}
        ]
        return detokenize(words)

    def allowed_ids(self, evidence: str) -> set[int]:
        allowed = {EOS, self.token_to_id.get(".", EOS)}
        for mark in PUNCT:
            idx = self.token_to_id.get(mark)
            if idx is not None:
                allowed.add(idx)
        for word in GLUE_WORDS:
            idx = self.token_to_id.get(word)
            if idx is not None:
                allowed.add(idx)
        for tok in word_tokens(evidence):
            idx = self.token_to_id.get(tok)
            if idx is not None:
                allowed.add(idx)
        return allowed


def encode_example(vocab: Vocab, question: str, evidence: str, answer: str = "") -> list[int]:
    """``<q> question <e> evidence <a> [answer] [<eos>]`` trimmed to ``MAX_LEN``."""
    q = vocab.encode_words(question)
    ev = vocab.encode_words(evidence)
    ans = vocab.encode_words(answer) if answer else []
    budget = MAX_LEN - (4 + len(q) + len(ans))
    if budget < 8:
        ev = ev[:8]
    else:
        ev = ev[:budget]
    ids = [Q, *q, E, *ev, A, *ans]
    if answer:
        ids.append(EOS)
    return ids[:MAX_LEN]


@dataclass
class CausalTransformer:
    """Tiny decoder-only Transformer. Own weights, own vocab."""

    tok_emb: np.ndarray
    pos_emb: np.ndarray
    wq: np.ndarray
    wk: np.ndarray
    wv: np.ndarray
    wo: np.ndarray
    w1: np.ndarray
    w2: np.ndarray
    dim: int = DIM
    n_layers: int = N_LAYERS
    n_heads: int = N_HEADS

    @classmethod
    def init(
        cls,
        vocab_size: int,
        *,
        dim: int = DIM,
        n_layers: int = N_LAYERS,
        n_heads: int = N_HEADS,
        seed: int = DEFAULT_SEED,
    ) -> CausalTransformer:
        rng = np.random.default_rng(seed)
        scale = 0.02
        return cls(
            tok_emb=rng.normal(0.0, scale, (vocab_size, dim)).astype(np.float32),
            pos_emb=rng.normal(0.0, scale, (MAX_LEN, dim)).astype(np.float32),
            wq=rng.normal(0.0, scale, (n_layers, dim, dim)).astype(np.float32),
            wk=rng.normal(0.0, scale, (n_layers, dim, dim)).astype(np.float32),
            wv=rng.normal(0.0, scale, (n_layers, dim, dim)).astype(np.float32),
            wo=rng.normal(0.0, scale, (n_layers, dim, dim)).astype(np.float32),
            w1=rng.normal(0.0, scale, (n_layers, dim, dim * FF_MULT)).astype(np.float32),
            w2=rng.normal(0.0, scale, (n_layers, dim * FF_MULT, dim)).astype(np.float32),
            dim=dim,
            n_layers=n_layers,
            n_heads=n_heads,
        )

    def _forward_hidden(self, ids: np.ndarray) -> np.ndarray:
        """``ids`` is (T,) int. Returns hidden (T, D)."""
        t = ids.shape[0]
        x = self.tok_emb[ids] + self.pos_emb[:t]
        dh = self.dim // self.n_heads
        causal = np.triu(np.full((t, t), -1e9, dtype=np.float32), k=1)
        for layer in range(self.n_layers):
            h = _layer_norm(x)
            q = h @ self.wq[layer]
            k = h @ self.wk[layer]
            v = h @ self.wv[layer]
            q = q.reshape(t, self.n_heads, dh).transpose(1, 0, 2)
            k = k.reshape(t, self.n_heads, dh).transpose(1, 0, 2)
            v = v.reshape(t, self.n_heads, dh).transpose(1, 0, 2)
            scores = (q @ k.transpose(0, 2, 1)) / np.sqrt(dh) + causal
            weights = _softmax_last(scores)
            merged = (weights @ v).transpose(1, 0, 2).reshape(t, self.dim)
            x = x + merged @ self.wo[layer]
            h = _layer_norm(x)
            x = x + (np.maximum(h @ self.w1[layer], 0.0) @ self.w2[layer])
        return _layer_norm(x)

    def logits(self, ids: list[int]) -> np.ndarray:
        hidden = self._forward_hidden(np.asarray(ids, dtype=np.int32))
        return hidden @ self.tok_emb.T

    def loss_and_step(self, ids: list[int], *, lr: float) -> float:
        """Teacher-forced CE after ``<a>``.

        Gradients update tied embeddings and the last feed-forward layer.
        Attention still mixes the packed context at serve; the trained
        head learns which evidence token comes next. Full GPU backprop
        through every head is a later train — this is still a causal
        Transformer decoder with learned weights, not a regex.
        """
        if A not in ids or len(ids) < 3:
            return 0.0
        arr = np.asarray(ids, dtype=np.int32)
        hidden_all = self._forward_hidden(arr)
        logits = hidden_all @ self.tok_emb.T
        start = ids.index(A)
        total = 0.0
        steps = 0
        last = self.n_layers - 1
        for t in range(start, len(ids) - 1):
            target = int(arr[t + 1])
            probs = _softmax_last(logits[t])
            total += float(-np.log(np.clip(probs[target], 1e-8, 1.0)))
            dlogit = probs.astype(np.float32)
            dlogit[target] -= 1.0
            hidden = hidden_all[t]
            self.tok_emb -= np.clip(lr * np.outer(dlogit, hidden), -0.08, 0.08)
            d_hidden = self.tok_emb.T @ dlogit
            # Last FF: x + relu(h @ W1) @ W2. Approximate h ≈ hidden.
            relu_in = hidden @ self.w1[last]
            relu = np.maximum(relu_in, 0.0)
            self.w2[last] -= np.clip(lr * np.outer(relu, d_hidden), -0.08, 0.08)
            d_relu = (self.w2[last] @ d_hidden) * (relu_in > 0)
            self.w1[last] -= np.clip(lr * np.outer(hidden, d_relu), -0.08, 0.08)
            tok = int(arr[t])
            if tok > UNK:
                self.tok_emb[tok] -= np.clip(lr * 0.25 * d_hidden, -0.08, 0.08)
            steps += 1
        return total / max(steps, 1)

    def generate(
        self,
        prefix: list[int],
        *,
        allowed: set[int],
        max_new: int = 40,
    ) -> list[int]:
        ids = list(prefix)
        for _ in range(max_new):
            if len(ids) >= MAX_LEN - 1:
                break
            logit = self.logits(ids)[-1]
            mask = np.full_like(logit, -1e9)
            for idx in allowed:
                if 0 <= idx < logit.shape[0]:
                    mask[idx] = 0.0
            nxt = int(np.argmax(logit + mask))
            if nxt == EOS or nxt == PAD:
                break
            ids.append(nxt)
            if nxt == self.tok_emb.shape[0]:
                break
        return ids


def answer_span(ids: list[int]) -> list[int]:
    if A not in ids:
        return []
    start = ids.index(A) + 1
    out = ids[start:]
    if EOS in out:
        out = out[: out.index(EOS)]
    return [i for i in out if i not in {PAD, BOS, Q, E, A, UNK}]
