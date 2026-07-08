import json
import pickle
from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import re

class MLBaselineModel:
    def __init__(self):
        self.vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4))
        self.target_embeddings = None
        self.targets = []

    def _normalize(self, name: str) -> str:
        s = name.strip().lower()
        s = re.sub(r"[^a-z0-9]+", "_", s)
        return re.sub(r"_+", "_", s).strip("_")

    def train(self, data_path: Path):
        targets_set = set()
        if data_path.exists():
            with data_path.open(encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    for m in record.get("output", {}).get("mappings", []):
                        tgt = m.get("target", "")
                        if tgt:
                            targets_set.add(self._normalize(tgt))
        
        # Add basic ones
        targets_set.update([
            "payment_amount", "payment_date", "customer_id", "account_number",
            "currency_code", "reference_number", "description", "status"
        ])
        
        self.targets = list(targets_set)
        if self.targets:
            self.target_embeddings = self.vectorizer.fit_transform(self.targets)

    def predict_target(self, source: str) -> tuple[str, float]:
        if not self.targets or self.target_embeddings is None:
            return "", 0.0
        
        src = self._normalize(source)
        src_emb = self.vectorizer.transform([src])
        sims = cosine_similarity(src_emb, self.target_embeddings)[0]
        best_idx = sims.argmax()
        return self.targets[best_idx], sims[best_idx]

def train_and_save():
    model = MLBaselineModel()
    data_path = Path(__file__).resolve().parents[2] / "src" / "ml" / "data" / "synthetic_v1.jsonl"
    model.train(data_path)
    
    out_path = Path(__file__).resolve().parents[1] / "models" / "baseline.pkl"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("wb") as f:
        pickle.dump(model, f)
    print(f"Model saved to {out_path}")

if __name__ == "__main__":
    train_and_save()
