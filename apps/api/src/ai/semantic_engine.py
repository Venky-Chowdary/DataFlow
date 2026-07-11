"""
DataTransfer.space — AI Semantic Mapping Engine
Enterprise-grade intelligent data analysis and mapping

This is the core AI engine that differentiates us from competitors:
- Semantic understanding of column names and data
- PII detection with compliance mapping
- Intelligent schema mapping
- Data quality analysis
- Transformation recommendations
"""

import re
import math
from difflib import SequenceMatcher
from typing import Optional
from dataclasses import dataclass, field
from enum import Enum


class DataCategory(Enum):
    """High-level data categories"""
    IDENTIFIER = "identifier"
    PERSONAL = "personal"
    CONTACT = "contact"
    FINANCIAL = "financial"
    TEMPORAL = "temporal"
    GEOGRAPHIC = "geographic"
    NUMERIC = "numeric"
    TEXT = "text"
    BINARY = "binary"
    STATUS = "status"
    METADATA = "metadata"


class ComplianceFramework(Enum):
    """Regulatory compliance frameworks"""
    GDPR = "gdpr"
    CCPA = "ccpa"
    HIPAA = "hipaa"
    PCI_DSS = "pci_dss"
    SOX = "sox"
    GLBA = "glba"
    FERPA = "ferpa"
    TCPA = "tcpa"


@dataclass
class SemanticType:
    """Definition of a semantic data type"""
    name: str
    category: DataCategory
    patterns: list[str]
    regex_patterns: list[str] = field(default_factory=list)
    sample_patterns: list[str] = field(default_factory=list)
    synonyms: list[str] = field(default_factory=list)
    is_pii: bool = False
    compliance: list[ComplianceFramework] = field(default_factory=list)
    transformations: list[str] = field(default_factory=list)
    base_confidence: float = 0.85


# ═══════════════════════════════════════════════════════════════════════════════
# SEMANTIC KNOWLEDGE BASE
# Comprehensive patterns for intelligent data understanding
# ═══════════════════════════════════════════════════════════════════════════════

SEMANTIC_TYPES: list[SemanticType] = [
    # ─────────────────────────────────────────────────────────────────────────
    # IDENTIFIERS
    # ─────────────────────────────────────────────────────────────────────────
    SemanticType(
        name="Primary Key",
        category=DataCategory.IDENTIFIER,
        patterns=["id", "pk", "key", "uid", "uuid", "guid"],
        regex_patterns=[r"^id$", r"_id$", r"^pk_", r"_pk$"],
        synonyms=["identifier", "primary_key", "record_id"],
        base_confidence=0.95,
    ),
    SemanticType(
        name="Foreign Key",
        category=DataCategory.IDENTIFIER,
        patterns=["fk", "ref", "parent_id", "related"],
        regex_patterns=[r"_fk$", r"^fk_", r"_ref$"],
        synonyms=["reference", "foreign_key", "link"],
        base_confidence=0.90,
    ),
    SemanticType(
        name="Customer ID",
        category=DataCategory.IDENTIFIER,
        patterns=["customer_id", "cust_id", "client_id", "account_id", "user_id", "member_id"],
        synonyms=["customerid", "custid", "clientid", "userid", "memberid", "acct_id"],
        base_confidence=0.92,
    ),
    SemanticType(
        name="Order ID",
        category=DataCategory.IDENTIFIER,
        patterns=["order_id", "orderid", "order_no", "order_number", "purchase_id", "transaction_id"],
        synonyms=["ordernumber", "order_num", "txn_id", "trans_id"],
        base_confidence=0.92,
    ),
    SemanticType(
        name="Product ID",
        category=DataCategory.IDENTIFIER,
        patterns=["product_id", "prod_id", "item_id", "sku", "upc", "ean", "asin"],
        synonyms=["productid", "prodid", "itemid", "product_code", "item_code"],
        base_confidence=0.92,
    ),
    
    # ─────────────────────────────────────────────────────────────────────────
    # PERSONAL INFORMATION (PII)
    # ─────────────────────────────────────────────────────────────────────────
    SemanticType(
        name="Full Name",
        category=DataCategory.PERSONAL,
        patterns=["name", "full_name", "fullname", "customer_name", "person_name"],
        regex_patterns=[r"^name$", r"full.?name", r"person.?name"],
        sample_patterns=[r"^[A-Z][a-z]+\s[A-Z][a-z]+$"],
        synonyms=["complete_name", "display_name", "legal_name"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR, ComplianceFramework.CCPA],
        transformations=["mask", "pseudonymize", "encrypt"],
        base_confidence=0.90,
    ),
    SemanticType(
        name="First Name",
        category=DataCategory.PERSONAL,
        patterns=["first_name", "firstname", "fname", "given_name", "forename"],
        sample_patterns=[r"^[A-Z][a-z]{1,15}$"],
        synonyms=["givenname", "first"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR, ComplianceFramework.CCPA],
        transformations=["mask", "pseudonymize"],
        base_confidence=0.92,
    ),
    SemanticType(
        name="Last Name",
        category=DataCategory.PERSONAL,
        patterns=["last_name", "lastname", "lname", "surname", "family_name"],
        sample_patterns=[r"^[A-Z][a-z]{1,20}$"],
        synonyms=["familyname", "last"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR, ComplianceFramework.CCPA],
        transformations=["mask", "pseudonymize"],
        base_confidence=0.92,
    ),
    SemanticType(
        name="Date of Birth",
        category=DataCategory.PERSONAL,
        patterns=["dob", "date_of_birth", "birth_date", "birthdate", "birthday"],
        regex_patterns=[r"birth.*date", r"date.*birth", r"^dob$"],
        synonyms=["dateofbirth", "bday"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR, ComplianceFramework.CCPA, ComplianceFramework.HIPAA],
        transformations=["age_bucket", "year_only", "encrypt"],
        base_confidence=0.95,
    ),
    SemanticType(
        name="Gender",
        category=DataCategory.PERSONAL,
        patterns=["gender", "sex", "gender_code"],
        sample_patterns=[r"^(M|F|Male|Female|Other|Non-binary)$"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR],
        transformations=["generalize"],
        base_confidence=0.90,
    ),
    SemanticType(
        name="Age",
        category=DataCategory.PERSONAL,
        patterns=["age", "customer_age", "person_age"],
        sample_patterns=[r"^\d{1,3}$"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR, ComplianceFramework.CCPA],
        transformations=["age_bucket"],
        base_confidence=0.85,
    ),
    
    # ─────────────────────────────────────────────────────────────────────────
    # CONTACT INFORMATION (PII)
    # ─────────────────────────────────────────────────────────────────────────
    SemanticType(
        name="Email Address",
        category=DataCategory.CONTACT,
        patterns=["email", "email_address", "emailaddress", "e_mail", "mail"],
        regex_patterns=[r"e.?mail", r"email.*addr"],
        sample_patterns=[r"^[\w\.-]+@[\w\.-]+\.\w{2,}$"],
        synonyms=["email_id", "contact_email", "work_email", "personal_email"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR, ComplianceFramework.CCPA, ComplianceFramework.TCPA],
        transformations=["mask_email", "hash", "encrypt"],
        base_confidence=0.98,
    ),
    SemanticType(
        name="Phone Number",
        category=DataCategory.CONTACT,
        patterns=["phone", "telephone", "mobile", "cell", "phone_number", "contact_number"],
        regex_patterns=[r"phone", r"mobile", r"cell", r"tel\b"],
        sample_patterns=[
            r"^\+?1?\d{10,14}$",
            r"^\(\d{3}\)\s?\d{3}-\d{4}$",
            r"^\d{3}-\d{3}-\d{4}$",
        ],
        synonyms=["phoneno", "ph_no", "contact_phone", "work_phone", "home_phone"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR, ComplianceFramework.CCPA, ComplianceFramework.TCPA],
        transformations=["mask", "format_e164", "encrypt"],
        base_confidence=0.96,
    ),
    SemanticType(
        name="Social Security Number",
        category=DataCategory.PERSONAL,
        patterns=["ssn", "social_security", "social_security_number", "ss_number"],
        regex_patterns=[r"^ssn$", r"social.*security"],
        sample_patterns=[r"^\d{3}-\d{2}-\d{4}$", r"^\d{9}$"],
        synonyms=["ss_num", "socialsecurity"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR, ComplianceFramework.CCPA, ComplianceFramework.SOX, ComplianceFramework.GLBA],
        transformations=["mask", "encrypt", "tokenize"],
        base_confidence=0.99,
    ),
    SemanticType(
        name="National ID",
        category=DataCategory.PERSONAL,
        patterns=["national_id", "national_identifier", "nino", "passport", "passport_number", "drivers_license", "license_number"],
        synonyms=["govt_id", "government_id", "id_number"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR, ComplianceFramework.CCPA],
        transformations=["mask", "encrypt", "tokenize"],
        base_confidence=0.95,
    ),
    
    # ─────────────────────────────────────────────────────────────────────────
    # GEOGRAPHIC
    # ─────────────────────────────────────────────────────────────────────────
    SemanticType(
        name="Street Address",
        category=DataCategory.GEOGRAPHIC,
        patterns=["address", "street", "street_address", "address_line", "addr"],
        regex_patterns=[r"address.*line", r"street.*addr"],
        sample_patterns=[r"^\d+\s+\w+"],
        synonyms=["addr1", "addr2", "address1", "address2", "mailing_address", "shipping_address"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR, ComplianceFramework.CCPA],
        transformations=["mask", "generalize_to_zip"],
        base_confidence=0.88,
    ),
    SemanticType(
        name="City",
        category=DataCategory.GEOGRAPHIC,
        patterns=["city", "town", "municipality", "locality"],
        sample_patterns=[r"^[A-Z][a-z]+(?:\s[A-Z][a-z]+)*$"],
        synonyms=["city_name", "shipping_city", "billing_city"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR],
        base_confidence=0.85,
    ),
    SemanticType(
        name="State/Province",
        category=DataCategory.GEOGRAPHIC,
        patterns=["state", "province", "region", "state_code"],
        sample_patterns=[r"^[A-Z]{2}$"],
        synonyms=["state_name", "st", "prov"],
        base_confidence=0.85,
    ),
    SemanticType(
        name="Postal Code",
        category=DataCategory.GEOGRAPHIC,
        patterns=["zip", "zipcode", "zip_code", "postal_code", "postcode"],
        regex_patterns=[r"zip", r"postal.*code", r"post.*code"],
        sample_patterns=[r"^\d{5}(-\d{4})?$", r"^[A-Z]\d[A-Z]\s?\d[A-Z]\d$"],
        synonyms=["postalcode", "zip5", "zip9"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR],
        transformations=["truncate_to_3"],
        base_confidence=0.92,
    ),
    SemanticType(
        name="Country",
        category=DataCategory.GEOGRAPHIC,
        patterns=["country", "country_code", "nation"],
        sample_patterns=[r"^[A-Z]{2,3}$"],
        synonyms=["country_name", "ctry", "country_iso"],
        base_confidence=0.88,
    ),
    SemanticType(
        name="Latitude",
        category=DataCategory.GEOGRAPHIC,
        patterns=["lat", "latitude"],
        sample_patterns=[r"^-?\d{1,2}\.\d+$"],
        base_confidence=0.90,
    ),
    SemanticType(
        name="Longitude",
        category=DataCategory.GEOGRAPHIC,
        patterns=["lon", "lng", "longitude"],
        sample_patterns=[r"^-?\d{1,3}\.\d+$"],
        base_confidence=0.90,
    ),
    
    # ─────────────────────────────────────────────────────────────────────────
    # FINANCIAL
    # ─────────────────────────────────────────────────────────────────────────
    SemanticType(
        name="Credit Card Number",
        category=DataCategory.FINANCIAL,
        patterns=["credit_card", "card_number", "cc_number", "card_no", "pan"],
        regex_patterns=[r"credit.*card", r"card.*num"],
        sample_patterns=[r"^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$", r"^\d{16}$"],
        synonyms=["ccn", "cardnumber"],
        is_pii=True,
        compliance=[ComplianceFramework.PCI_DSS, ComplianceFramework.GDPR],
        transformations=["mask_pan", "tokenize", "encrypt"],
        base_confidence=0.99,
    ),
    SemanticType(
        name="CVV",
        category=DataCategory.FINANCIAL,
        patterns=["cvv", "cvc", "security_code", "card_security"],
        sample_patterns=[r"^\d{3,4}$"],
        is_pii=True,
        compliance=[ComplianceFramework.PCI_DSS],
        transformations=["remove", "never_store"],
        base_confidence=0.98,
    ),
    SemanticType(
        name="Bank Account Number",
        category=DataCategory.FINANCIAL,
        patterns=["account_number", "bank_account", "acct_no", "account_no"],
        regex_patterns=[r"account.*num", r"bank.*acct"],
        synonyms=["bankaccount", "acctnum"],
        is_pii=True,
        compliance=[ComplianceFramework.GLBA, ComplianceFramework.GDPR],
        transformations=["mask", "encrypt", "tokenize"],
        base_confidence=0.92,
    ),
    SemanticType(
        name="Routing Number",
        category=DataCategory.FINANCIAL,
        patterns=["routing_number", "routing_no", "aba_number", "sort_code"],
        sample_patterns=[r"^\d{9}$"],
        is_pii=True,
        compliance=[ComplianceFramework.GLBA],
        transformations=["mask"],
        base_confidence=0.90,
    ),
    SemanticType(
        name="IBAN",
        category=DataCategory.FINANCIAL,
        patterns=["iban", "international_bank_account"],
        sample_patterns=[r"^[A-Z]{2}\d{2}[A-Z0-9]{11,30}$"],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR, ComplianceFramework.GLBA],
        transformations=["mask", "encrypt"],
        base_confidence=0.95,
    ),
    SemanticType(
        name="Currency Amount",
        category=DataCategory.FINANCIAL,
        patterns=["amount", "price", "cost", "total", "subtotal", "revenue", "sales", "payment"],
        regex_patterns=[r"amount", r"price", r"cost", r"total", r"_amt$", r"_val$"],
        sample_patterns=[r"^\$?\d+\.?\d{0,2}$", r"^-?\d+\.?\d{0,2}$"],
        synonyms=["amt", "value", "sum", "charge", "fee", "balance"],
        transformations=["round", "convert_currency"],
        base_confidence=0.88,
    ),
    SemanticType(
        name="Currency Code",
        category=DataCategory.FINANCIAL,
        patterns=["currency", "currency_code", "ccy"],
        sample_patterns=[r"^[A-Z]{3}$"],
        synonyms=["curr_code", "money_type"],
        base_confidence=0.90,
    ),
    
    # ─────────────────────────────────────────────────────────────────────────
    # TEMPORAL
    # ─────────────────────────────────────────────────────────────────────────
    SemanticType(
        name="Date",
        category=DataCategory.TEMPORAL,
        patterns=["date", "dt"],
        regex_patterns=[r"_date$", r"_dt$", r"^date_"],
        sample_patterns=[
            r"^\d{4}-\d{2}-\d{2}$",
            r"^\d{2}/\d{2}/\d{4}$",
            r"^\d{2}-\d{2}-\d{4}$",
        ],
        synonyms=["effective_date", "as_of_date"],
        transformations=["standardize_iso8601"],
        base_confidence=0.88,
    ),
    SemanticType(
        name="Timestamp",
        category=DataCategory.TEMPORAL,
        patterns=["timestamp", "datetime", "created_at", "updated_at", "modified_at"],
        regex_patterns=[r"_at$", r"timestamp", r"datetime"],
        sample_patterns=[r"^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}"],
        synonyms=["ts", "time_stamp"],
        transformations=["standardize_iso8601", "convert_timezone"],
        base_confidence=0.92,
    ),
    SemanticType(
        name="Time",
        category=DataCategory.TEMPORAL,
        patterns=["time", "hour", "start_time", "end_time"],
        sample_patterns=[r"^\d{2}:\d{2}(:\d{2})?$"],
        base_confidence=0.85,
    ),
    SemanticType(
        name="Year",
        category=DataCategory.TEMPORAL,
        patterns=["year", "yr", "fiscal_year", "calendar_year"],
        sample_patterns=[r"^(19|20)\d{2}$"],
        base_confidence=0.88,
    ),
    SemanticType(
        name="Month",
        category=DataCategory.TEMPORAL,
        patterns=["month", "mo", "month_name", "month_num"],
        sample_patterns=[r"^(0?[1-9]|1[0-2])$", r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)"],
        base_confidence=0.85,
    ),
    
    # ─────────────────────────────────────────────────────────────────────────
    # HEALTH (HIPAA)
    # ─────────────────────────────────────────────────────────────────────────
    SemanticType(
        name="Medical Record Number",
        category=DataCategory.PERSONAL,
        patterns=["mrn", "medical_record", "patient_id", "health_record"],
        is_pii=True,
        compliance=[ComplianceFramework.HIPAA, ComplianceFramework.GDPR],
        transformations=["mask", "encrypt", "tokenize"],
        base_confidence=0.95,
    ),
    SemanticType(
        name="Health Insurance ID",
        category=DataCategory.PERSONAL,
        patterns=["insurance_id", "member_id", "policy_number", "subscriber_id", "hic_number"],
        is_pii=True,
        compliance=[ComplianceFramework.HIPAA],
        transformations=["mask", "encrypt"],
        base_confidence=0.92,
    ),
    SemanticType(
        name="Diagnosis Code",
        category=DataCategory.PERSONAL,
        patterns=["icd", "icd_code", "diagnosis", "diag_code", "icd10", "icd9"],
        sample_patterns=[r"^[A-Z]\d{2}\.?\d{0,2}$"],
        is_pii=True,
        compliance=[ComplianceFramework.HIPAA],
        transformations=["generalize"],
        base_confidence=0.90,
    ),
    
    # ─────────────────────────────────────────────────────────────────────────
    # STATUS & ENUMS
    # ─────────────────────────────────────────────────────────────────────────
    SemanticType(
        name="Status",
        category=DataCategory.STATUS,
        patterns=["status", "state", "condition", "stage"],
        sample_patterns=[r"^(active|inactive|pending|complete|approved|rejected)$"],
        synonyms=["sts", "current_status", "order_status"],
        base_confidence=0.85,
    ),
    SemanticType(
        name="Boolean Flag",
        category=DataCategory.BINARY,
        patterns=["is_", "has_", "can_", "flag", "enabled", "active", "deleted"],
        regex_patterns=[r"^is_", r"^has_", r"^can_", r"_flag$"],
        sample_patterns=[r"^(true|false|1|0|yes|no|y|n)$"],
        synonyms=["indicator", "bool"],
        base_confidence=0.90,
    ),
    SemanticType(
        name="Priority",
        category=DataCategory.STATUS,
        patterns=["priority", "urgency", "importance"],
        sample_patterns=[r"^(low|medium|high|critical|urgent)$", r"^[1-5]$"],
        base_confidence=0.88,
    ),
    
    # ─────────────────────────────────────────────────────────────────────────
    # NUMERIC
    # ─────────────────────────────────────────────────────────────────────────
    SemanticType(
        name="Quantity",
        category=DataCategory.NUMERIC,
        patterns=["quantity", "qty", "count", "num", "number_of"],
        regex_patterns=[r"quantity", r"_qty$", r"_count$", r"^num_"],
        sample_patterns=[r"^\d+$"],
        synonyms=["units", "items", "pieces"],
        transformations=["validate_positive"],
        base_confidence=0.85,
    ),
    SemanticType(
        name="Percentage",
        category=DataCategory.NUMERIC,
        patterns=["percent", "pct", "rate", "ratio", "percentage"],
        regex_patterns=[r"_pct$", r"_rate$", r"percent"],
        sample_patterns=[r"^\d{1,3}\.?\d{0,2}%?$"],
        synonyms=["proportion"],
        transformations=["standardize_decimal"],
        base_confidence=0.88,
    ),
    SemanticType(
        name="Score",
        category=DataCategory.NUMERIC,
        patterns=["score", "rating", "rank", "grade"],
        sample_patterns=[r"^\d{1,3}\.?\d{0,2}$"],
        base_confidence=0.85,
    ),
    
    # ─────────────────────────────────────────────────────────────────────────
    # TEXT
    # ─────────────────────────────────────────────────────────────────────────
    SemanticType(
        name="Description",
        category=DataCategory.TEXT,
        patterns=["description", "desc", "details", "notes", "comments", "remarks"],
        synonyms=["descr", "comment", "note", "memo"],
        base_confidence=0.85,
    ),
    SemanticType(
        name="URL",
        category=DataCategory.TEXT,
        patterns=["url", "link", "website", "web_address", "uri", "href"],
        sample_patterns=[r"^https?://"],
        synonyms=["web_url", "webpage"],
        base_confidence=0.92,
    ),
    SemanticType(
        name="IP Address",
        category=DataCategory.METADATA,
        patterns=["ip", "ip_address", "ipaddress", "client_ip", "source_ip"],
        sample_patterns=[
            r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$",
            r"^[0-9a-fA-F:]+$",
        ],
        is_pii=True,
        compliance=[ComplianceFramework.GDPR],
        transformations=["mask", "hash"],
        base_confidence=0.95,
    ),
]


# ═══════════════════════════════════════════════════════════════════════════════
# ANALYSIS RESULTS
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class ColumnAnalysis:
    """Complete analysis of a single column"""
    column_name: str
    inferred_type: str
    semantic_type: Optional[str] = None
    category: Optional[DataCategory] = None
    confidence: float = 0.0
    is_pii: bool = False
    compliance: list[ComplianceFramework] = field(default_factory=list)
    suggested_transformations: list[str] = field(default_factory=list)
    null_percentage: float = 0.0
    unique_percentage: float = 0.0
    sample_values: list[str] = field(default_factory=list)
    statistics: dict = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


@dataclass
class MappingSuggestion:
    """Suggested mapping between source and target columns"""
    source_column: str
    target_column: str
    confidence: float
    reason: str
    transformation_needed: bool = False
    suggested_transformation: Optional[str] = None


@dataclass
class SchemaAnalysis:
    """Complete schema analysis result"""
    columns: list[ColumnAnalysis]
    pii_columns: list[str]
    compliance_requirements: dict[ComplianceFramework, list[str]]
    quality_score: float
    recommendations: list[str]


# ═══════════════════════════════════════════════════════════════════════════════
# SEMANTIC ANALYZER ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class SemanticAnalyzer:
    """
    AI-powered semantic analysis engine.
    
    This is the core intelligence that differentiates DataTransfer.space.
    It uses multiple signals to understand data:
    
    1. Column name analysis (NLP-based)
    2. Sample data pattern matching
    3. Statistical analysis
    4. Cross-column relationship detection
    """
    
    def __init__(self):
        self.semantic_types = SEMANTIC_TYPES
        self._build_pattern_index()
    
    def _build_pattern_index(self):
        """Build optimized pattern lookup structures"""
        self.pattern_index: dict[str, list[SemanticType]] = {}
        for st in self.semantic_types:
            for pattern in st.patterns:
                key = pattern.lower()
                if key not in self.pattern_index:
                    self.pattern_index[key] = []
                self.pattern_index[key].append(st)
            for synonym in st.synonyms:
                key = synonym.lower()
                if key not in self.pattern_index:
                    self.pattern_index[key] = []
                self.pattern_index[key].append(st)
    
    def _normalize_column_name(self, name: str) -> str:
        """Normalize column name for matching"""
        name = name.lower().strip()
        name = re.sub(r'[^a-z0-9]', '_', name)
        name = re.sub(r'_+', '_', name)
        return name.strip('_')
    
    def _tokenize_column_name(self, name: str) -> list[str]:
        """Split column name into semantic tokens"""
        normalized = self._normalize_column_name(name)
        tokens = re.split(r'_', normalized)
        
        camel_case_splits = []
        for token in tokens:
            camel_case_splits.extend(
                re.findall(r'[a-z]+|[A-Z][a-z]*|\d+', token)
            )
        
        return [t.lower() for t in camel_case_splits if t]
    
    def _match_column_name(self, column_name: str) -> tuple[Optional[SemanticType], float]:
        """
        Match column name against semantic patterns.
        Returns the best matching type and confidence score.
        """
        normalized = self._normalize_column_name(column_name)
        tokens = self._tokenize_column_name(column_name)
        
        best_match: Optional[SemanticType] = None
        best_score = 0.0
        
        if normalized in self.pattern_index:
            matches = self.pattern_index[normalized]
            if matches:
                best_match = matches[0]
                best_score = matches[0].base_confidence * 1.1
        
        for st in self.semantic_types:
            for pattern in st.patterns:
                if pattern.lower() == normalized:
                    score = st.base_confidence * 1.0
                    if score > best_score:
                        best_match = st
                        best_score = score
                    break
                
                if pattern.lower() in normalized:
                    score = st.base_confidence * 0.85
                    if score > best_score:
                        best_match = st
                        best_score = score
                
                if pattern.lower() in tokens:
                    score = st.base_confidence * 0.75
                    if score > best_score:
                        best_match = st
                        best_score = score
            
            for regex in st.regex_patterns:
                if re.search(regex, normalized, re.IGNORECASE):
                    score = st.base_confidence * 0.9
                    if score > best_score:
                        best_match = st
                        best_score = score
        
        return best_match, min(best_score, 0.99)
    
    def _analyze_sample_data(self, values: list[str], semantic_type: Optional[SemanticType]) -> float:
        """
        Analyze sample data to validate/adjust semantic type confidence.
        Returns confidence adjustment factor.
        """
        if not values:
            return 0.8
        
        non_empty = [v for v in values if v and str(v).strip()]
        if not non_empty:
            return 0.5
        
        if semantic_type and semantic_type.sample_patterns:
            match_count = 0
            for value in non_empty[:100]:
                for pattern in semantic_type.sample_patterns:
                    if re.match(pattern, str(value).strip(), re.IGNORECASE):
                        match_count += 1
                        break
            
            match_rate = match_count / min(len(non_empty), 100)
            if match_rate > 0.8:
                return 1.15
            elif match_rate > 0.5:
                return 1.0
            elif match_rate < 0.2:
                return 0.6
        
        return 0.9
    
    def _infer_data_type(self, values: list[str]) -> str:
        """Infer the basic data type from sample values"""
        if not values:
            return "unknown"
        
        non_empty = [str(v).strip() for v in values if v and str(v).strip()]
        if not non_empty:
            return "null"
        
        int_count = 0
        float_count = 0
        bool_count = 0
        date_count = 0
        
        for val in non_empty[:100]:
            if re.match(r'^-?\d+$', val):
                int_count += 1
            elif re.match(r'^-?\d+\.?\d*$', val):
                float_count += 1
            elif val.lower() in ('true', 'false', '0', '1', 'yes', 'no', 'y', 'n'):
                bool_count += 1
            elif re.match(r'^\d{4}-\d{2}-\d{2}', val) or re.match(r'^\d{2}/\d{2}/\d{4}', val):
                date_count += 1
        
        sample_size = min(len(non_empty), 100)
        if int_count / sample_size > 0.8:
            return "integer"
        if (int_count + float_count) / sample_size > 0.8:
            return "decimal"
        if bool_count / sample_size > 0.8:
            return "boolean"
        if date_count / sample_size > 0.5:
            return "datetime"
        
        return "string"
    
    def _calculate_statistics(self, values: list[str]) -> dict:
        """Calculate statistical properties of the data"""
        if not values:
            return {}
        
        non_empty = [v for v in values if v and str(v).strip()]
        total = len(values)
        non_empty_count = len(non_empty)
        
        stats = {
            "total_count": total,
            "non_empty_count": non_empty_count,
            "null_count": total - non_empty_count,
            "null_percentage": ((total - non_empty_count) / total * 100) if total > 0 else 0,
            "unique_count": len(set(non_empty)),
            "unique_percentage": (len(set(non_empty)) / non_empty_count * 100) if non_empty_count > 0 else 0,
        }
        
        str_lengths = [len(str(v)) for v in non_empty]
        if str_lengths:
            stats["min_length"] = min(str_lengths)
            stats["max_length"] = max(str_lengths)
            stats["avg_length"] = sum(str_lengths) / len(str_lengths)
        
        return stats
    
    def analyze_column(self, column_name: str, sample_values: list[str] = None) -> ColumnAnalysis:
        """
        Perform complete analysis of a single column.
        
        This is the main entry point for column-level analysis.
        It combines name analysis, sample data analysis, and statistics.
        """
        if sample_values is None:
            sample_values = []
        
        semantic_type, name_confidence = self._match_column_name(column_name)
        
        data_confidence = self._analyze_sample_data(sample_values, semantic_type)
        final_confidence = name_confidence * data_confidence
        
        stats = self._calculate_statistics(sample_values)
        inferred_type = self._infer_data_type(sample_values)
        
        warnings = []
        if stats.get("null_percentage", 0) > 50:
            warnings.append(f"High null rate: {stats['null_percentage']:.1f}%")
        if stats.get("unique_percentage", 0) < 1 and inferred_type not in ("boolean",):
            warnings.append("Very low cardinality - consider as enum/category")
        
        return ColumnAnalysis(
            column_name=column_name,
            inferred_type=inferred_type,
            semantic_type=semantic_type.name if semantic_type else None,
            category=semantic_type.category if semantic_type else None,
            confidence=round(final_confidence, 3),
            is_pii=semantic_type.is_pii if semantic_type else False,
            compliance=[c for c in (semantic_type.compliance if semantic_type else [])],
            suggested_transformations=semantic_type.transformations if semantic_type else [],
            null_percentage=stats.get("null_percentage", 0),
            unique_percentage=stats.get("unique_percentage", 0),
            sample_values=sample_values[:5],
            statistics=stats,
            warnings=warnings,
        )
    
    def analyze_schema(self, columns: dict[str, list[str]]) -> SchemaAnalysis:
        """
        Analyze a complete schema (multiple columns).
        
        Args:
            columns: Dict mapping column names to sample values
            
        Returns:
            SchemaAnalysis with complete analysis of all columns
        """
        analyses = []
        pii_columns = []
        compliance_map: dict[ComplianceFramework, list[str]] = {}
        
        for col_name, values in columns.items():
            analysis = self.analyze_column(col_name, values)
            analyses.append(analysis)
            
            if analysis.is_pii:
                pii_columns.append(col_name)
                for compliance in analysis.compliance:
                    if compliance not in compliance_map:
                        compliance_map[compliance] = []
                    compliance_map[compliance].append(col_name)
        
        avg_confidence = sum(a.confidence for a in analyses) / len(analyses) if analyses else 0
        avg_null_rate = sum(a.null_percentage for a in analyses) / len(analyses) if analyses else 0
        quality_score = (avg_confidence * 0.6 + (100 - avg_null_rate) / 100 * 0.4) * 100
        
        recommendations = []
        if pii_columns:
            recommendations.append(f"PII detected in {len(pii_columns)} columns - consider encryption or masking")
        if avg_null_rate > 20:
            recommendations.append(f"Average null rate is {avg_null_rate:.1f}% - consider data quality checks")
        low_conf = [a.column_name for a in analyses if a.confidence < 0.7]
        if low_conf:
            recommendations.append(f"Low confidence mapping for: {', '.join(low_conf[:3])} - manual review recommended")
        
        return SchemaAnalysis(
            columns=analyses,
            pii_columns=pii_columns,
            compliance_requirements=compliance_map,
            quality_score=round(quality_score, 1),
            recommendations=recommendations,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SMART MAPPER
# ═══════════════════════════════════════════════════════════════════════════════

class SmartMapper:
    """
    Intelligent schema mapping between source and destination.
    
    Uses multiple strategies:
    1. Exact name match
    2. Normalized name match
    3. Semantic type match
    4. Token overlap scoring
    5. Synonym matching
    """
    
    def __init__(self, analyzer: SemanticAnalyzer):
        self.analyzer = analyzer
        self.synonym_groups = self._build_synonym_groups()
    
    def _build_synonym_groups(self) -> list[set[str]]:
        """Build groups of synonymous terms"""
        groups = []
        for st in SEMANTIC_TYPES:
            group = set()
            group.add(st.name.lower())
            group.update(p.lower() for p in st.patterns)
            group.update(s.lower() for s in st.synonyms)
            groups.append(group)
        return groups
    
    def _normalize(self, name: str) -> str:
        """Normalize column name"""
        name = name.lower()
        name = re.sub(r'[^a-z0-9]', '', name)
        return name
    
    def _get_tokens(self, name: str) -> set[str]:
        """Get semantic tokens from column name"""
        normalized = re.sub(r'[^a-z0-9]', '_', name.lower())
        tokens = set(re.split(r'_+', normalized))
        tokens.discard('')
        return tokens
    
    def _token_similarity(self, name1: str, name2: str) -> float:
        """Calculate token-based similarity score"""
        tokens1 = self._get_tokens(name1)
        tokens2 = self._get_tokens(name2)
        
        if not tokens1 or not tokens2:
            return 0.0
        
        intersection = tokens1 & tokens2
        union = tokens1 | tokens2
        
        return len(intersection) / len(union)
    
    def _synonym_match(self, name1: str, name2: str) -> bool:
        """Check if names are synonyms"""
        norm1 = self._normalize(name1)
        norm2 = self._normalize(name2)
        
        for group in self.synonym_groups:
            if norm1 in group and norm2 in group:
                return True
        return False

    def _char_similarity(self, name1: str, name2: str) -> float:
        """Character-level similarity on normalized names.

        Catches typos and abbreviations that token/synonym matching miss,
        e.g. ``custmer_id`` vs ``customer_id`` or ``phonenum`` vs ``phone_number``.
        """
        norm1 = self._normalize(name1)
        norm2 = self._normalize(name2)
        if not norm1 or not norm2:
            return 0.0
        return SequenceMatcher(None, norm1, norm2).ratio()

    def _score_pair(
        self,
        src_col: str,
        tgt_col: str,
        src_analysis: "ColumnAnalysis",
        tgt_analysis: "ColumnAnalysis",
    ) -> tuple[float, str]:
        """Score a single source→target pair. Higher is better."""
        if self._normalize(src_col) == self._normalize(tgt_col):
            return 0.98, "Exact match"
        if src_col.lower() == tgt_col.lower():
            return 0.95, "Case-insensitive match"
        if self._synonym_match(src_col, tgt_col):
            return 0.88, "Synonym match"
        if src_analysis.semantic_type and src_analysis.semantic_type == tgt_analysis.semantic_type:
            return 0.85, f"Same semantic type: {src_analysis.semantic_type}"

        token_sim = self._token_similarity(src_col, tgt_col)
        char_sim = self._char_similarity(src_col, tgt_col)

        token_score = 0.6 + token_sim * 0.3 if token_sim > 0.3 else 0.0
        # Character similarity rescues abbreviations/typos with no shared tokens.
        char_score = 0.55 + (char_sim - 0.7) * 1.0 if char_sim >= 0.7 else 0.0

        if token_score >= char_score and token_score > 0.0:
            return token_score, f"Token similarity: {token_sim:.0%}"
        if char_score > 0.0:
            return round(char_score, 3), f"Name similarity: {char_sim:.0%}"
        return 0.0, "No suitable match found"

    def map_columns(
        self,
        source_columns: list[str],
        target_columns: list[str],
        source_samples: dict[str, list[str]] = None
    ) -> list[MappingSuggestion]:
        """
        Generate intelligent column mappings.

        Uses order-independent global assignment: every source→target pair is
        scored, then the highest-confidence pairs are assigned first so a weaker
        earlier column can never claim a target that is a stronger match for a
        later column (the classic greedy pitfall). Each target is used at most
        once. Character-level similarity rescues typos and abbreviations that
        exact/synonym/token matching miss.

        Args:
            source_columns: List of source column names
            target_columns: List of target column names
            source_samples: Optional sample data for source columns

        Returns:
            List of mapping suggestions with confidence scores
        """
        if source_samples is None:
            source_samples = {}

        source_analyses = {
            col: self.analyzer.analyze_column(col, source_samples.get(col, []))
            for col in source_columns
        }
        target_analyses = {
            col: self.analyzer.analyze_column(col, [])
            for col in target_columns
        }

        # Score every candidate pair once.
        candidates: list[tuple[float, str, str]] = []
        for src_col in source_columns:
            for tgt_col in target_columns:
                score, _reason = self._score_pair(
                    src_col, tgt_col, source_analyses[src_col], target_analyses[tgt_col]
                )
                if score > 0.5:
                    candidates.append((score, src_col, tgt_col))

        # Assign globally: strongest pairs first, one target per source and
        # one source per target. Deterministic tie-break keeps results stable.
        candidates.sort(key=lambda c: (-c[0], c[1], c[2]))
        assigned: dict[str, tuple[str, float]] = {}
        used_targets: set[str] = set()
        for score, src_col, tgt_col in candidates:
            if src_col in assigned or tgt_col in used_targets:
                continue
            assigned[src_col] = (tgt_col, score)
            used_targets.add(tgt_col)

        mappings = []
        for src_col in source_columns:
            src_analysis = source_analyses[src_col]
            match = assigned.get(src_col)
            if match:
                tgt_col, score = match
                _score, reason = self._score_pair(
                    src_col, tgt_col, src_analysis, target_analyses[tgt_col]
                )
                needs_transform = False
                transform = None
                if src_analysis.inferred_type != target_analyses[tgt_col].inferred_type:
                    needs_transform = True
                    transform = (
                        f"Convert {src_analysis.inferred_type} to "
                        f"{target_analyses[tgt_col].inferred_type}"
                    )
                mappings.append(MappingSuggestion(
                    source_column=src_col,
                    target_column=tgt_col,
                    confidence=round(score, 3),
                    reason=reason,
                    transformation_needed=needs_transform,
                    suggested_transformation=transform,
                ))
            else:
                mappings.append(MappingSuggestion(
                    source_column=src_col,
                    target_column="<unmapped>",
                    confidence=0.0,
                    reason="No suitable match found",
                ))

        return sorted(mappings, key=lambda m: m.confidence, reverse=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ═══════════════════════════════════════════════════════════════════════════════

_analyzer = SemanticAnalyzer()
_mapper = SmartMapper(_analyzer)


def analyze_column(name: str, samples: list[str] = None) -> ColumnAnalysis:
    """Analyze a single column"""
    return _analyzer.analyze_column(name, samples or [])


def analyze_schema(columns: dict[str, list[str]]) -> SchemaAnalysis:
    """Analyze a complete schema"""
    return _analyzer.analyze_schema(columns)


def generate_mappings(
    source_columns: list[str],
    target_columns: list[str],
    source_samples: dict[str, list[str]] = None
) -> list[MappingSuggestion]:
    """Generate intelligent column mappings"""
    return _mapper.map_columns(source_columns, target_columns, source_samples)


def detect_pii(columns: dict[str, list[str]]) -> dict[str, list[ComplianceFramework]]:
    """Detect PII columns and their compliance requirements"""
    analysis = analyze_schema(columns)
    return {
        col: analysis.compliance_requirements.get(framework, [])
        for framework in ComplianceFramework
        for col in analysis.pii_columns
    }


# ═══════════════════════════════════════════════════════════════════════════════
# EXAMPLE USAGE
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("DataTransfer.space — AI Semantic Engine Demo")
    print("=" * 60)
    
    sample_schema = {
        "cust_id": ["C001", "C002", "C003"],
        "customer_name": ["John Smith", "Jane Doe", "Bob Wilson"],
        "email_address": ["john@email.com", "jane@email.com", "bob@email.com"],
        "phone_number": ["+1-555-123-4567", "555-987-6543", "5551234567"],
        "ssn": ["123-45-6789", "987-65-4321", "456-78-9012"],
        "dob": ["1985-03-15", "1990-07-22", "1978-11-08"],
        "order_amt": ["150.00", "89.99", "234.50"],
        "created_at": ["2024-01-15T10:30:00Z", "2024-01-16T14:22:00Z", "2024-01-17T09:15:00Z"],
    }
    
    print("\n📊 Schema Analysis Results:")
    print("-" * 40)
    
    analysis = analyze_schema(sample_schema)
    
    for col in analysis.columns:
        pii_badge = "🔴 PII" if col.is_pii else "✅ OK"
        print(f"\n  {col.column_name}")
        print(f"    Type: {col.semantic_type or col.inferred_type}")
        print(f"    Confidence: {col.confidence:.1%}")
        print(f"    Status: {pii_badge}")
        if col.compliance:
            print(f"    Compliance: {', '.join(c.value.upper() for c in col.compliance)}")
    
    print(f"\n📈 Quality Score: {analysis.quality_score:.1f}%")
    print(f"🔐 PII Columns: {', '.join(analysis.pii_columns)}")
    
    if analysis.recommendations:
        print("\n💡 Recommendations:")
        for rec in analysis.recommendations:
            print(f"    • {rec}")
    
    print("\n" + "=" * 60)
    print("🔄 Smart Mapping Demo")
    print("-" * 40)
    
    source_cols = ["customer_id", "cust_name", "email", "mobile_phone", "amt"]
    target_cols = ["id", "full_name", "email_address", "phone_number", "amount"]
    
    mappings = generate_mappings(source_cols, target_cols)
    
    for m in mappings:
        status = "✅" if m.confidence > 0.7 else "⚠️" if m.confidence > 0.5 else "❌"
        print(f"  {status} {m.source_column} → {m.target_column}")
        print(f"      Confidence: {m.confidence:.1%} | Reason: {m.reason}")
        if m.transformation_needed:
            print(f"      Transform: {m.suggested_transformation}")
    
    print("\n" + "=" * 60)
