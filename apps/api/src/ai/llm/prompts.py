"""
DataTransfer.space — LLM Prompt Templates

Structured prompts for schema analysis, mapping, PII detection, and transformations.
"""

CHAIN_OF_THOUGHT_TEMPLATE = """Think step by step to solve this data engineering task.

{task_description}

Follow these reasoning steps:
1. Identify all column names and their likely semantic meaning
2. Check for synonyms and abbreviations (e.g., AMT=amount, cust=customer)
3. Analyze sample data patterns if provided
4. Determine data types and PII classification
5. Suggest mappings or transformations with confidence scores
6. Provide final answer with reasoning

{context}

Query: {query}

Respond in JSON format with keys: reasoning (list of steps), answer (final result), confidence (0-1)."""

SCHEMA_ANALYSIS_PROMPT = """Analyze the following data column using chain-of-thought reasoning.

Column Name: {column_name}
Sample Values: {sample_values}
Retrieved Context:
{context}

Steps:
1. Tokenize and normalize the column name
2. Match against known semantic patterns and synonyms
3. Validate against sample data patterns
4. Classify PII status and compliance requirements
5. Recommend transformations

Respond with JSON: {{"semantic_type": "...", "category": "...", "is_pii": bool, "confidence": float, "reasoning": ["step1", ...], "transformations": [...]}}"""

COLUMN_MAPPING_PROMPT = """Map source columns to target columns using intelligent reasoning.

Source Columns: {source_columns}
Target Columns: {target_columns}
Source Samples: {source_samples}
Retrieved Context:
{context}

For each source column:
1. Resolve synonyms (AMT→amount, cust→customer, qty→quantity)
2. Match semantic types between source and target
3. Calculate confidence based on name similarity + semantic match + data validation
4. Suggest type conversions if needed

Respond with JSON: {{"mappings": [{{"source": "...", "target": "...", "confidence": float, "reason": "...", "transformation": "..."}}], "reasoning": ["step1", ...]}}"""

PII_DETECTION_PROMPT = """Detect PII in the following data columns.

Columns and Samples:
{columns}

For each column:
1. Check column name against PII patterns
2. Validate sample values against PII formats (SSN, email, phone, etc.)
3. Assign compliance frameworks (GDPR, HIPAA, PCI-DSS, etc.)
4. Recommend protection actions

Respond with JSON: {{"pii_columns": [{{"column": "...", "type": "...", "risk": "high|medium|low", "compliance": [...], "actions": [...]}}], "reasoning": ["step1", ...]}}"""

TRANSFORMATION_PROMPT = """Suggest data transformations for the following mapping.

Source Column: {source_column} (type: {source_type})
Target Column: {target_column} (type: {target_type})
Semantic Type: {semantic_type}
Sample Values: {sample_values}

Steps:
1. Check if direct type conversion is possible
2. Identify semantic transformations (mask, encrypt, format)
3. Recommend validation rules
4. Estimate data loss risk

Respond with JSON: {{"transformations": [...], "validation_rules": [...], "lossy": bool, "reasoning": ["step1", ...]}}"""

NATURAL_LANGUAGE_PROMPT = """You are the DataTransfer.space AI assistant. Answer this data engineering question.

Question: {query}

Relevant Knowledge:
{context}

Use chain-of-thought reasoning. Respond with a clear, actionable answer about the data schema, types, or mappings involved."""

COPILOT_CHAT_PROMPT = """{persona}

Current user intent: {intent}

Conversation history:
{history}

Trained knowledge (from universal data):
{context}

User message: {message}

Respond as the DataTransfer Copilot. Be friendly, concise, and actionable.
Use markdown for formatting. Give specific steps when helping with transfers or connectors.
Do not mention internal system details like RAG or vector stores."""

SCHEMA_ANALYSIS_PROMPT_SIMPLE = """Analyze this data query using the retrieved context.

Query: {query}
Context:
{context}

Provide a clear, actionable answer about the data schema, types, or mappings involved."""
