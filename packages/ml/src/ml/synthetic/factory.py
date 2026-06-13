"""Synthetic schema pair generator for DataMap-LLM training."""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

AMOUNT_ALIASES = ["AMT", "amount", "Amount", "payment_amount", "Amt", "Fld_07", "value", "TXN_AMT"]
DATE_ALIASES = ["PAY_DT", "pay_date", "payment_date", "DtPmt", "value_date", "TXN_DATE", "ship_date"]
ACCOUNT_ALIASES = ["ACCT_NO", "account_number", "beneficiary_account", "AcctNum", "IBAN"]
LOGISTICS_ALIASES = {
    "customer_id": ["CUST_ID", "customerid", "client_no", "shipper_id"],
    "origin_city": ["ORIG_CITY", "origin", "from_city", "pickup_city"],
    "destination_city": ["DEST_CITY", "destination", "to_city", "delivery_city"],
    "shipment_weight_kg": ["WEIGHT", "weight_kg", "pkg_weight", "WT_KG"],
    "tracking_number": ["TRACK_NO", "tracking_id", "awb", "consignment_no"],
    "payment_amount": AMOUNT_ALIASES,
    "transaction_date": DATE_ALIASES,
}

DOMAIN_TEMPLATES = {
    "payments": {
        "targets": [
            ("payment_amount", "NUMBER"),
            ("payment_date", "DATE"),
            ("beneficiary_account", "VARCHAR"),
            ("currency_code", "VARCHAR"),
            ("transaction_id", "VARCHAR"),
        ],
        "source_pools": [AMOUNT_ALIASES, DATE_ALIASES, ACCOUNT_ALIASES],
    },
    "retail": {
        "targets": [
            ("order_total", "DECIMAL"),
            ("order_date", "TIMESTAMP"),
            ("customer_id", "VARCHAR"),
            ("sku", "VARCHAR"),
        ],
        "source_pools": [
            ["order_amt", "total", "OrderTotal", "amt_due"],
            ["order_dt", "created_at", "OrderDate"],
            ["cust_id", "CustomerID", "buyer_id"],
            ["product_sku", "SKU", "item_code"],
        ],
    },
    "logistics": {
        "targets": [
            ("customer_id", "VARCHAR"),
            ("origin_city", "VARCHAR"),
            ("destination_city", "VARCHAR"),
            ("shipment_weight_kg", "DECIMAL"),
            ("tracking_number", "VARCHAR"),
            ("payment_amount", "NUMBER"),
            ("transaction_date", "DATE"),
        ],
        "source_pools": [
            LOGISTICS_ALIASES["customer_id"],
            LOGISTICS_ALIASES["origin_city"],
            LOGISTICS_ALIASES["destination_city"],
            LOGISTICS_ALIASES["shipment_weight_kg"],
            LOGISTICS_ALIASES["tracking_number"],
            LOGISTICS_ALIASES["payment_amount"],
            LOGISTICS_ALIASES["transaction_date"],
        ],
    },
}


@dataclass
class TrainingExample:
    domain: str
    source_schema: list[dict]
    target_schema: list[dict]
    mappings: list[dict]
    format_hint: str = "csv"

    def to_jsonl_record(self) -> dict:
        return {
            "id": str(uuid.uuid4()),
            "domain": self.domain,
            "format": self.format_hint,
            "input": {
                "source_schema": self.source_schema,
                "target_schema": self.target_schema,
            },
            "output": {"mappings": self.mappings},
        }


@dataclass
class GeneratorStats:
    generated: int = 0
    by_domain: dict[str, int] = field(default_factory=dict)


class SyntheticSchemaFactory:
    """Generates schema-mapping training pairs across domains."""

    def __init__(self, seed: int = 42):
        random.seed(seed)

    def generate_example(self, domain: str = "payments") -> TrainingExample:
        template = DOMAIN_TEMPLATES[domain]
        source_schema = []
        mappings = []

        for i, (target_name, target_type) in enumerate(template["targets"]):
            pool = template["source_pools"][i] if i < len(template["source_pools"]) else [target_name]
            source_name = random.choice(pool)
            confidence = round(random.uniform(0.86, 0.99), 2)
            source_schema.append(
                {
                    "name": source_name,
                    "inferred_type": target_type,
                    "samples": self._samples_for(target_name),
                }
            )
            mappings.append(
                {
                    "source": source_name,
                    "target": target_name,
                    "confidence": confidence,
                    "reasoning": f"Semantic match: {source_name} represents {target_name}",
                }
            )

        target_schema = [{"name": t, "type": ty} for t, ty in template["targets"]]

        return TrainingExample(
            domain=domain,
            source_schema=source_schema,
            target_schema=target_schema,
            mappings=mappings,
            format_hint=random.choice(["csv", "fixed_width", "csa_positional", "excel"]),
        )

    def generate_batch(self, count: int, output_path: Path | None = None) -> GeneratorStats:
        stats = GeneratorStats()
        records: list[dict] = []
        domains = list(DOMAIN_TEMPLATES.keys())

        for _ in range(count):
            domain = random.choice(domains)
            ex = self.generate_example(domain)
            records.append(ex.to_jsonl_record())
            stats.generated += 1
            stats.by_domain[domain] = stats.by_domain.get(domain, 0) + 1

        if output_path:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with output_path.open("w", encoding="utf-8") as f:
                for r in records:
                    f.write(json.dumps(r) + "\n")

        return stats

    @staticmethod
    def _samples_for(target: str) -> list[str]:
        if "amount" in target or "total" in target:
            return ["150000", "230050", "9999", "1250.50"]
        if "date" in target:
            return ["20250115", "20250201", "20241231", "2025-06-12"]
        if "account" in target or "id" in target:
            return ["ACC123456", "GB82WEST1234", "TXN-9981", "CUST-4421"]
        if "city" in target:
            return ["Chicago", "Dallas", "Mumbai", "London"]
        if "weight" in target:
            return ["12.5", "450", "0.8", "1200"]
        if "tracking" in target:
            return ["TRK-8821", "AWB991023", "CONS-4412"]
        return ["sample_a", "sample_b"]


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Generate synthetic schema-mapping training data")
    parser.add_argument("--count", type=int, default=1000, help="Number of examples (default: 1000)")
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help="Output JSONL path (default: packages/ml/src/ml/data/synthetic_v1.jsonl)",
    )
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    factory = SyntheticSchemaFactory(seed=args.seed)
    out = Path(args.output) if args.output else Path(__file__).resolve().parents[1] / "data" / "synthetic_v1.jsonl"
    stats = factory.generate_batch(args.count, out)
    print(f"Generated {stats.generated} examples -> {out}")
    print(f"By domain: {stats.by_domain}")
    print("For large-scale training (e.g. 10M records), run: python factory.py --count 10000000 --output large.jsonl")


if __name__ == "__main__":
    main()
