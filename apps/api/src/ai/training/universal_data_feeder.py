"""
DataTransfer.space — Universal Data Feeder

Ingest schemas from uploaded files, connectors, and industry templates for AI training.
"""

from __future__ import annotations
import os
from dataclasses import dataclass, field
from pathlib import Path

from ..knowledge.industry_schemas import INDUSTRY_SCHEMAS


@dataclass
class UniversalSchema:
    name: str
    source: str  # upload, industry, connector
    columns: list[str]
    samples: dict[str, list[str]] = field(default_factory=dict)
    row_count: int = 0
    industry: str | None = None
    file_type: str = ""


class UniversalDataFeeder:
    """Feed universal data schemas into the training pipeline."""

    def __init__(self, upload_dirs: list[str] | None = None):
        base = Path(__file__).resolve().parents[3]  # apps/api
        default_dirs = [
            str(base / "uploads"),
            str(base / "src" / "uploads"),
            str(base / "tests" / "fixtures"),
            str(base.parent / "api" / "uploads"),
        ]
        self.upload_dirs = upload_dirs or default_dirs
        self._parser = None

    @property
    def parser(self):
        if self._parser is None:
            from ...services.file_parser import FileParser
            self._parser = FileParser
        return self._parser

    def scan_uploads(self) -> list[UniversalSchema]:
        """Parse all files in upload directories."""
        schemas = []

        for upload_dir in self.upload_dirs:
            path = Path(upload_dir)
            if not path.exists():
                continue

            for file_path in path.iterdir():
                if not file_path.is_file():
                    continue
                if file_path.suffix.lower() not in (".csv", ".json", ".jsonl", ".tsv", ".ndjson"):
                    continue

                try:
                    content = file_path.read_bytes()
                    result = self.parser.parse(content, file_path.name)
                    if not result.success or not result.columns:
                        continue

                    samples: dict[str, list[str]] = {}
                    for col in result.columns:
                        vals = []
                        for row in result.data[:20]:
                            if isinstance(row, dict) and col in row and row[col] is not None:
                                vals.append(str(row[col]))
                        samples[col] = vals[:5]

                    schemas.append(UniversalSchema(
                        name=file_path.stem,
                        source="upload",
                        columns=result.columns,
                        samples=samples,
                        row_count=result.row_count,
                        file_type=result.file_type,
                    ))
                except Exception:
                    continue

        return schemas

    def load_industry_schemas(self) -> list[UniversalSchema]:
        """Convert industry templates to universal schemas."""
        schemas = []
        for key, schema in INDUSTRY_SCHEMAS.items():
            cols = list(schema["columns"].keys())
            samples = {}
            for col, info in schema["columns"].items():
                sem = info.get("semantic", col)
                samples[col] = [f"sample_{sem.lower().replace(' ', '_')}"]
            schemas.append(UniversalSchema(
                name=schema["name"],
                source="industry",
                columns=cols,
                samples=samples,
                row_count=0,
                industry=key,
            ))
        return schemas

    def feed_all(self) -> list[UniversalSchema]:
        """Collect schemas from all universal data sources."""
        seen = set()
        all_schemas = []

        try:
            from .universal_source_registry import load_connector_schemas, expand_schema_variants
            catalog_schemas = expand_schema_variants(load_connector_schemas(), max_variants=2)
        except Exception:
            catalog_schemas = []

        sources = self.scan_uploads() + self.load_industry_schemas() + catalog_schemas

        for schema in sources:
            key = (schema.name, tuple(sorted(schema.columns[:8])))
            if key not in seen:
                seen.add(key)
                all_schemas.append(schema)

        return all_schemas

    def to_training_dicts(self, schemas: list[UniversalSchema] | None = None) -> list[dict]:
        """Convert schemas to dicts for conversation synthesis."""
        schemas = schemas or self.feed_all()
        return [
            {
                "name": s.name,
                "columns": s.columns,
                "samples": s.samples,
                "industry": s.industry,
                "source": s.source,
                "row_count": s.row_count,
            }
            for s in schemas
        ]

    def get_status(self) -> dict:
        schemas = self.feed_all()
        uploads = [s for s in schemas if s.source == "upload"]
        industries = [s for s in schemas if s.source == "industry"]
        catalog = [s for s in schemas if s.source == "catalog"]
        registry_stats = {}
        try:
            from .universal_source_registry import get_universal_schema_count
            registry_stats = get_universal_schema_count()
        except Exception:
            pass
        return {
            "upload_dirs": self.upload_dirs,
            "total_schemas": len(schemas),
            "from_uploads": len(uploads),
            "from_industry": len(industries),
            "from_catalog": len(catalog),
            "total_columns": sum(len(s.columns) for s in schemas),
            "registry": registry_stats,
            "schemas": [
                {"name": s.name, "source": s.source, "columns": len(s.columns), "rows": s.row_count}
                for s in schemas[:20]
            ],
        }
