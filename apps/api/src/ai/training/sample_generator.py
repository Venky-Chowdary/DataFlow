"""
DataTransfer.space — Sample Data Generator

Create diverse test datasets for logistics, finance, healthcare, retail, etc.
"""

from __future__ import annotations
import csv
import io
import json
import random
import uuid
from dataclasses import dataclass


@dataclass
class SampleDataset:
    """A generated sample dataset."""
    name: str
    industry: str
    format: str  # csv, json
    columns: list[str]
    rows: list[dict]
    row_count: int


class DataTransferSampleGenerator:
    """Generate realistic sample datasets for testing."""

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)

    def generate_logistics_csv(self, rows: int = 100) -> SampleDataset:
        """Generate logistics/shipping CSV data."""
        columns = [
            "shipment_id", "tracking_number", "origin_warehouse", "destination_city",
            "carrier", "ship_date", "weight_kg", "freight_cost", "status",
        ]
        carriers = ["FedEx", "UPS", "DHL", "USPS", "Amazon Logistics"]
        statuses = ["shipped", "in_transit", "delivered", "pending", "returned"]
        cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "London", "Tokyo"]

        data = []
        for i in range(rows):
            data.append({
                "shipment_id": f"SHP-{10000 + i}",
                "tracking_number": f"1Z{self.rng.randint(100000000, 999999999)}",
                "origin_warehouse": f"WH-{self.rng.randint(1, 20):03d}",
                "destination_city": self.rng.choice(cities),
                "carrier": self.rng.choice(carriers),
                "ship_date": f"2024-{self.rng.randint(1,12):02d}-{self.rng.randint(1,28):02d}",
                "weight_kg": round(self.rng.uniform(0.5, 50.0), 2),
                "freight_cost": round(self.rng.uniform(5.0, 200.0), 2),
                "status": self.rng.choice(statuses),
            })

        return SampleDataset("logistics_shipments", "logistics", "csv", columns, data, rows)

    def generate_finance_json(self, rows: int = 100) -> SampleDataset:
        """Generate financial transactions JSON data."""
        columns = [
            "transaction_id", "account_number", "amount", "currency",
            "transaction_date", "transaction_type", "description", "balance",
        ]
        types = ["debit", "credit", "transfer", "fee", "interest"]
        currencies = ["USD", "EUR", "GBP", "JPY"]

        data = []
        for i in range(rows):
            data.append({
                "transaction_id": f"TXN-{uuid.uuid4().hex[:8].upper()}",
                "account_number": f"****{self.rng.randint(1000, 9999)}",
                "amount": round(self.rng.uniform(-5000, 10000), 2),
                "currency": self.rng.choice(currencies),
                "transaction_date": f"2024-{self.rng.randint(1,12):02d}-{self.rng.randint(1,28):02d}T{self.rng.randint(0,23):02d}:{self.rng.randint(0,59):02d}:00Z",
                "transaction_type": self.rng.choice(types),
                "description": self.rng.choice(["Payment", "Deposit", "Withdrawal", "Transfer", "Fee"]),
                "balance": round(self.rng.uniform(0, 50000), 2),
            })

        return SampleDataset("finance_transactions", "finance", "json", columns, data, rows)

    def generate_healthcare_csv(self, rows: int = 50) -> SampleDataset:
        """Generate healthcare patient records (synthetic)."""
        columns = [
            "patient_id", "mrn", "first_name", "last_name", "date_of_birth",
            "diagnosis_code", "visit_date", "provider_npi", "insurance_id",
        ]
        first_names = ["John", "Jane", "Robert", "Maria", "David", "Sarah"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Davis"]
        diagnoses = ["J06.9", "I10", "E11.9", "M54.5", "F32.9", "J45.909"]

        data = []
        for i in range(rows):
            data.append({
                "patient_id": f"PAT-{1000 + i}",
                "mrn": f"MRN-{self.rng.randint(100000, 999999)}",
                "first_name": self.rng.choice(first_names),
                "last_name": self.rng.choice(last_names),
                "date_of_birth": f"{self.rng.randint(1940, 2005)}-{self.rng.randint(1,12):02d}-{self.rng.randint(1,28):02d}",
                "diagnosis_code": self.rng.choice(diagnoses),
                "visit_date": f"2024-{self.rng.randint(1,12):02d}-{self.rng.randint(1,28):02d}",
                "provider_npi": f"{self.rng.randint(1000000000, 9999999999)}",
                "insurance_id": f"INS-{self.rng.randint(10000, 99999)}",
            })

        return SampleDataset("healthcare_patients", "healthcare", "csv", columns, data, rows)

    def generate_retail_csv(self, rows: int = 100) -> SampleDataset:
        """Generate retail/e-commerce order data."""
        columns = [
            "order_id", "customer_id", "product_sku", "quantity",
            "unit_price", "total_amount", "order_date", "payment_status", "channel",
        ]
        statuses = ["paid", "pending", "refunded", "failed"]
        channels = ["web", "mobile", "marketplace", "in_store"]

        data = []
        for i in range(rows):
            qty = self.rng.randint(1, 10)
            price = round(self.rng.uniform(5.0, 500.0), 2)
            data.append({
                "order_id": f"ORD-{20000 + i}",
                "customer_id": f"CUST-{self.rng.randint(1000, 9999)}",
                "product_sku": f"SKU-{self.rng.randint(10000, 99999)}",
                "quantity": qty,
                "unit_price": price,
                "total_amount": round(qty * price, 2),
                "order_date": f"2024-{self.rng.randint(1,12):02d}-{self.rng.randint(1,28):02d}",
                "payment_status": self.rng.choice(statuses),
                "channel": self.rng.choice(channels),
            })

        return SampleDataset("retail_orders", "retail", "csv", columns, data, rows)

    def generate_all(self, rows: int = 50) -> dict[str, SampleDataset]:
        """Generate all industry sample datasets."""
        return {
            "logistics": self.generate_logistics_csv(rows),
            "finance": self.generate_finance_json(rows),
            "healthcare": self.generate_healthcare_csv(rows),
            "retail": self.generate_retail_csv(rows),
        }

    def to_csv_string(self, dataset: SampleDataset) -> str:
        """Convert dataset to CSV string."""
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=dataset.columns)
        writer.writeheader()
        writer.writerows(dataset.rows)
        return output.getvalue()

    def to_json_string(self, dataset: SampleDataset) -> str:
        """Convert dataset to JSON string."""
        return json.dumps(dataset.rows, indent=2)

    def to_schema_dict(self, dataset: SampleDataset) -> dict[str, list[str]]:
        """Convert to schema format for analysis (column → sample values)."""
        schema = {}
        for col in dataset.columns:
            schema[col] = [str(row[col]) for row in dataset.rows[:10]]
        return schema
