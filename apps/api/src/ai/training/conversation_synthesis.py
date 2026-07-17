"""
DataTransfer.space — Conversation Training Data Synthesis

Generate customer-facing Q&A pairs from universal data schemas.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from ..knowledge.copilot_knowledge import CONVERSATION_TEMPLATES
from ..knowledge.industry_schemas import INDUSTRY_SCHEMAS
from ..knowledge.semantic_patterns import SEMANTIC_PATTERNS


@dataclass
class ConversationExample:
    id: str
    user_message: str
    assistant_message: str
    intent: str
    context: dict = field(default_factory=dict)


class ConversationSynthesizer:
    """Generate copilot conversation training data from universal schemas."""

    def synthesize_from_industry_schemas(self) -> list[ConversationExample]:
        """Create Q&A from industry schema templates."""
        examples = []

        for industry_key, schema in INDUSTRY_SCHEMAS.items():
            name = schema["name"]
            cols = list(schema["columns"].keys())
            pii_cols = [
                c for c, info in schema["columns"].items()
                if info.get("pii") or info.get("semantic", "").lower().find("ssn") >= 0
            ]

            examples.append(ConversationExample(
                id=str(uuid.uuid4()),
                user_message=f"Move my {industry_key} data to MongoDB",
                assistant_message=(
                    f"I can help you transfer **{name}** data. Your schema typically includes "
                    f"columns like {', '.join(cols[:5])}{'...' if len(cols) > 5 else ''}.\n\n"
                    f"Go to **Transfer** → upload your {industry_key} file → AI will recognize "
                    f"these {len(cols)} column types automatically → select MongoDB destination → "
                    f"run preflight → execute."
                ),
                intent="transfer_help",
                context={"industry": industry_key, "columns": cols},
            ))

            if pii_cols:
                examples.append(ConversationExample(
                    id=str(uuid.uuid4()),
                    user_message=f"What PII is in {industry_key} data?",
                    assistant_message=(
                        f"**{name}** data typically contains PII in: {', '.join(pii_cols)}.\n\n"
                        f"Upload your file in Transfer Step 2 — AI Analysis will flag each column "
                        f"with compliance tags (GDPR, HIPAA, etc.). Review before transferring."
                    ),
                    intent="pii_compliance",
                    context={"industry": industry_key, "pii_columns": pii_cols},
                ))

            examples.append(ConversationExample(
                id=str(uuid.uuid4()),
                user_message=f"Map {industry_key} columns to a warehouse schema",
                assistant_message=(
                    f"For **{name}** data, AI mapping handles common abbreviations automatically. "
                    f"Key columns: {', '.join(cols[:6])}.\n\n"
                    f"In Transfer Step 2, review the AI-suggested mappings. Columns like "
                    f"`{cols[0]}` are detected as **{schema['columns'][cols[0]].get('semantic', 'text')}** "
                    f"with high confidence."
                ),
                intent="mapping_help",
                context={"industry": industry_key},
            ))

        return examples

    def synthesize_from_schema(
        self,
        schema_name: str,
        columns: list[str],
        sample_values: dict[str, list[str]] | None = None,
        industry: str | None = None,
    ) -> list[ConversationExample]:
        """Create Q&A from a real uploaded schema."""
        examples = []
        col_preview = ", ".join(columns[:8])
        industry_tag = f" ({industry})" if industry else ""

        examples.append(ConversationExample(
            id=str(uuid.uuid4()),
            user_message=f"What columns are in my {schema_name} file?",
            assistant_message=(
                f"Your **{schema_name}**{industry_tag} dataset has {len(columns)} columns: "
                f"{col_preview}{'...' if len(columns) > 8 else ''}.\n\n"
                f"AI Analysis in the Transfer wizard will classify each column's semantic type "
                f"and flag any PII."
            ),
            intent="mapping_help",
            context={"schema": schema_name, "columns": columns},
        ))

        examples.append(ConversationExample(
            id=str(uuid.uuid4()),
            user_message=f"Transfer {schema_name} to MongoDB",
            assistant_message=(
                f"To transfer **{schema_name}** ({len(columns)} columns) to MongoDB:\n\n"
                f"1. **Transfer** → your file is already uploaded\n"
                f"2. Review AI analysis for: {col_preview}\n"
                f"3. Select your MongoDB connector\n"
                f"4. Run preflight gates\n"
                f"5. Execute\n\n"
                f"Estimated columns to map: {len(columns)}."
            ),
            intent="transfer_help",
            context={"schema": schema_name},
        ))

        if sample_values:
            pii_hints = []
            for col, samples in sample_values.items():
                for sample in samples[:3]:
                    s = str(sample)
                    if "@" in s and "." in s:
                        pii_hints.append(col)
                        break
                    if any(c.isdigit() for c in s) and len(s.replace("-", "").replace(" ", "")) >= 9:
                        if "ssn" in col.lower() or "social" in col.lower():
                            pii_hints.append(col)

            if pii_hints:
                examples.append(ConversationExample(
                    id=str(uuid.uuid4()),
                    user_message=f"Is there PII in {schema_name}?",
                    assistant_message=(
                        f"Based on your **{schema_name}** samples, potential PII columns: "
                        f"{', '.join(set(pii_hints))}.\n\n"
                        f"Run full PII detection in Transfer Step 2 for GDPR/HIPAA compliance tags."
                    ),
                    intent="pii_compliance",
                    context={"schema": schema_name, "pii_candidates": list(set(pii_hints))},
                ))

        return examples

    def synthesize_from_semantic_patterns(self, count: int = 50) -> list[ConversationExample]:
        """Generate mapping Q&A from semantic patterns."""
        import random
        rng = random.Random(42)
        examples = []

        for _ in range(min(count, len(SEMANTIC_PATTERNS))):
            pattern = rng.choice(SEMANTIC_PATTERNS)
            if len(pattern.patterns) < 2:
                continue
            src = rng.choice(pattern.patterns)
            tgt = rng.choice(pattern.synonyms or pattern.patterns)

            examples.append(ConversationExample(
                id=str(uuid.uuid4()),
                user_message=f"Does {src} map to {tgt}?",
                assistant_message=(
                    f"Yes — **{src}** and **{tgt}** both map to semantic type "
                    f"**{pattern.name}** ({pattern.category.value}). "
                    f"Confidence: {pattern.base_confidence:.0%}. "
                    f"{'This is PII — handle with compliance care.' if pattern.is_pii else ''}"
                ).strip(),
                intent="mapping_help",
                context={"semantic_type": pattern.name, "source": src, "target": tgt},
            ))

        return examples

    def synthesize_base_templates(self) -> list[ConversationExample]:
        """Convert static templates to training examples."""
        return [
            ConversationExample(
                id=str(uuid.uuid4()),
                user_message=t["user"],
                assistant_message=t["assistant"],
                intent=t["intent"],
            )
            for t in CONVERSATION_TEMPLATES
        ]

    def synthesize_agent_actions(self, universal_schemas: list[dict] | None = None) -> list[ConversationExample]:
        """Train Data Pilot agent tool-use and in-app actions."""
        examples = [
            ConversationExample(
                id=str(uuid.uuid4()),
                user_message="What data do I have?",
                assistant_message=(
                    "Let me check your indexed datasets.\n\n"
                    "I found your uploads and sample fixtures — logistics, retail, payments, and HR. "
                    "Ask me to analyze any one in detail, or say \"compare logistics and retail\"."
                ),
                intent="mapping_help",
                context={"tool": "list_datasets"},
            ),
            ConversationExample(
                id=str(uuid.uuid4()),
                user_message="Show my transfer jobs",
                assistant_message=(
                    "Here are your recent transfer jobs. I can see completed uploads to MongoDB "
                    "and any failed runs with error details. Want me to open the Jobs page?"
                ),
                intent="transfer_help",
                context={"tool": "list_jobs", "action": "navigate:jobs"},
            ),
            ConversationExample(
                id=str(uuid.uuid4()),
                user_message="Take me to new transfer",
                assistant_message="Opening **New Transfer** — upload your file and I'll analyze columns, PII, and mappings before you execute.",
                intent="transfer_help",
                context={"tool": "navigate", "action": "navigate:transfer"},
            ),
            ConversationExample(
                id=str(uuid.uuid4()),
                user_message="Can I move Snowflake data to PostgreSQL?",
                assistant_message=(
                    "Yes — universal transfer supports **database → database** migrations including "
                    "Snowflake → PostgreSQL. Tables are auto-created with typed columns. "
                    "Go to New Transfer, pick your Snowflake connector as source and PostgreSQL as destination."
                ),
                intent="transfer_help",
                context={"tool": "get_transfer_capabilities"},
            ),
        ]

        for schema in (universal_schemas or [])[:6]:
            name = schema.get("name", "dataset")
            cols = schema.get("columns", [])
            label = name.replace("sample_", "").replace("_", " ")
            examples.append(ConversationExample(
                id=str(uuid.uuid4()),
                user_message=f"Tell me everything about {label}",
                assistant_message=(
                    f"I analyzed **{label}** — {len(cols)} columns"
                    + (f", {schema.get('row_count', 0):,} rows" if schema.get("row_count") else "")
                    + f". Columns include {', '.join(cols[:5])}{'...' if len(cols) > 5 else ''}. "
                    "Ask about PII, sample rows, or mapping to MongoDB/Snowflake."
                ),
                intent="mapping_help",
                context={"tool": "analyze_dataset", "dataset": name},
            ))

        return examples

    def synthesize_from_catalog(self) -> list[ConversationExample]:
        """Generate Q&A for every connector in the 620+ catalog."""
        from ...services.catalog_service import load_catalog

        examples = []
        for c in load_catalog().get("connectors", []):
            name = c["name"]
            cid = c["id"]
            status = c.get("status", "planned")
            category = c.get("category", "other")

            examples.append(ConversationExample(
                id=str(uuid.uuid4()),
                user_message=f"How do I set up {name} as a source?",
                assistant_message=(
                    f"To use **{name}** as a source:\n\n"
                    f"1. Go to **Connectors** → **+ New Source**\n"
                    f"2. Search for `{name}` in the 620+ catalog\n"
                    f"3. Configure credentials — category: **{category}**, status: **{status}**\n"
                    f"4. Data Pilot can then read from {name} and map columns automatically\n\n"
                    f"{c.get('description', '')}"
                ),
                intent="transfer_help",
                context={"tool": "search_connectors", "connector_id": cid, "role": "source"},
            ))

            if status in ("live", "beta"):
                examples.append(ConversationExample(
                    id=str(uuid.uuid4()),
                    user_message=f"Move data from {name} to Snowflake",
                    assistant_message=(
                        f"Universal transfer supports **{name} → Snowflake**"
                        + (" (live route)." if status == "live" else " (beta — may need connector setup).")
                        + f"\n\nOpen **New Transfer**, select your {name} connector as source, "
                        f"Snowflake as destination. AI will infer schema, detect PII, and auto-create tables."
                    ),
                    intent="transfer_help",
                    context={"tool": "get_transfer_capabilities", "connector_id": cid},
                ))

        return examples

    def synthesize_full(self, universal_schemas: list[dict] | None = None) -> list[ConversationExample]:
        """Generate complete conversation training set."""
        examples = []
        examples.extend(self.synthesize_base_templates())
        examples.extend(self.synthesize_from_industry_schemas())
        examples.extend(self.synthesize_from_semantic_patterns(50))
        examples.extend(self.synthesize_agent_actions(universal_schemas))
        examples.extend(self.synthesize_from_catalog())

        for schema in universal_schemas or []:
            examples.extend(self.synthesize_from_schema(
                schema_name=schema.get("name", "upload"),
                columns=schema.get("columns", []),
                sample_values=schema.get("samples"),
                industry=schema.get("industry"),
            ))

        return examples

    def to_vector_documents(self, examples: list[ConversationExample]) -> list[dict]:
        """Convert examples to RAG vector store documents."""
        docs = []
        for i, ex in enumerate(examples):
            docs.append({
                "id": f"conv_train_{ex.id[:8]}_{i}",
                "text": f"User: {ex.user_message}\nAssistant: {ex.assistant_message}",
                "metadata": {
                    "type": "copilot_training",
                    "intent": ex.intent,
                    "schema": ex.context.get("schema", ""),
                    "industry": ex.context.get("industry", ""),
                },
            })
        return docs
