"""
DataTransfer.space — Expanded Semantic Patterns (200+)

Comprehensive semantic type definitions for universal data understanding.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class PatternCategory(Enum):
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
    HEALTHCARE = "healthcare"
    LOGISTICS = "logistics"
    RETAIL = "retail"
    HR = "hr"
    MANUFACTURING = "manufacturing"
    EDUCATION = "education"
    TELECOM = "telecom"
    INSURANCE = "insurance"
    REAL_ESTATE = "real_estate"


@dataclass
class SemanticPattern:
    """A semantic data pattern definition."""
    name: str
    category: PatternCategory
    patterns: list[str]
    regex_patterns: list[str] = field(default_factory=list)
    sample_patterns: list[str] = field(default_factory=list)
    synonyms: list[str] = field(default_factory=list)
    is_pii: bool = False
    compliance: list[str] = field(default_factory=list)
    transformations: list[str] = field(default_factory=list)
    base_confidence: float = 0.85
    data_type: str = "string"


def _build_patterns() -> list[SemanticPattern]:
    """Build the complete pattern catalog."""
    patterns: list[SemanticPattern] = []

    def add(name, cat, pats, **kwargs):
        patterns.append(SemanticPattern(name=name, category=cat, patterns=pats, **kwargs))

    # ── IDENTIFIERS (25) ──
    add("Primary Key", PatternCategory.IDENTIFIER, ["id", "pk", "key", "uid", "uuid", "guid"],
        regex_patterns=[r"^id$", r"_id$", r"^pk_"], synonyms=["identifier", "record_id"], base_confidence=0.95)
    add("Foreign Key", PatternCategory.IDENTIFIER, ["fk", "ref", "parent_id", "related"],
        regex_patterns=[r"_fk$", r"^fk_"], synonyms=["reference", "foreign_key"], base_confidence=0.90)
    add("Customer ID", PatternCategory.IDENTIFIER, ["customer_id", "cust_id", "client_id", "account_id", "user_id", "member_id"],
        synonyms=["customerid", "custid", "clientid", "userid"], base_confidence=0.92)
    add("Order ID", PatternCategory.IDENTIFIER, ["order_id", "order_no", "order_number", "purchase_id", "transaction_id"],
        synonyms=["orderid", "txn_id", "trans_id"], base_confidence=0.92)
    add("Product ID", PatternCategory.IDENTIFIER, ["product_id", "prod_id", "item_id", "sku", "upc", "ean", "asin"],
        synonyms=["productid", "prodid", "itemid"], base_confidence=0.92)
    add("Employee ID", PatternCategory.IDENTIFIER, ["employee_id", "emp_id", "staff_id", "worker_id", "badge_id"],
        synonyms=["empid", "personnel_id"], base_confidence=0.90)
    add("Vendor ID", PatternCategory.IDENTIFIER, ["vendor_id", "supplier_id", "seller_id", "merchant_id"],
        synonyms=["vendorid", "supplierid"], base_confidence=0.90)
    add("Shipment ID", PatternCategory.LOGISTICS, ["shipment_id", "ship_id", "tracking_id", "tracking_number", "waybill"],
        synonyms=["tracking_no", "consignment_id", "bol_number"], base_confidence=0.91)
    add("Invoice ID", PatternCategory.FINANCIAL, ["invoice_id", "invoice_no", "invoice_number", "bill_id"],
        synonyms=["inv_id", "inv_no"], base_confidence=0.91)
    add("Contract ID", PatternCategory.IDENTIFIER, ["contract_id", "contract_no", "agreement_id"],
        synonyms=["contract_number"], base_confidence=0.88)
    add("Session ID", PatternCategory.METADATA, ["session_id", "sess_id", "visit_id", "tracking_session"],
        synonyms=["sessionid"], base_confidence=0.88)
    add("Batch ID", PatternCategory.MANUFACTURING, ["batch_id", "batch_number", "lot_id", "lot_number"],
        synonyms=["batch_no", "lot_no"], base_confidence=0.90)
    add("Serial Number", PatternCategory.MANUFACTURING, ["serial_number", "serial_no", "serial", "sn"],
        synonyms=["equipment_serial"], base_confidence=0.90)
    add("Work Order", PatternCategory.MANUFACTURING, ["work_order", "wo", "wo_number", "production_order"],
        synonyms=["job_number"], base_confidence=0.89)
    add("Policy ID", PatternCategory.INSURANCE, ["policy_id", "policy_number", "policy_no", "coverage_id"],
        synonyms=["insurance_id"], base_confidence=0.91)
    add("Claim ID", PatternCategory.INSURANCE, ["claim_id", "claim_no", "claim_number"],
        synonyms=["claims_id"], base_confidence=0.90)
    add("Student ID", PatternCategory.EDUCATION, ["student_id", "pupil_id", "enrollment_id", "student_number"],
        synonyms=["studentid"], base_confidence=0.91)
    add("Course ID", PatternCategory.EDUCATION, ["course_id", "course_code", "class_id", "section_id", "crn"],
        synonyms=["courseid"], base_confidence=0.90)
    add("Property ID", PatternCategory.REAL_ESTATE, ["property_id", "parcel_id", "parcel_number", "apn", "listing_id"],
        synonyms=["mls_id"], base_confidence=0.90)
    add("Subscriber ID", PatternCategory.TELECOM, ["subscriber_id", "sub_id", "msisdn", "imsi"],
        synonyms=["subscriberid"], base_confidence=0.90)
    add("Device ID", PatternCategory.TELECOM, ["device_id", "imei", "device_imei", "hardware_id"],
        synonyms=["deviceid"], base_confidence=0.89)
    add("Ticket ID", PatternCategory.IDENTIFIER, ["ticket_id", "case_id", "issue_id", "support_id", "incident_id"],
        synonyms=["ticket_no", "case_number"], base_confidence=0.89)
    add("Project ID", PatternCategory.IDENTIFIER, ["project_id", "proj_id", "initiative_id"],
        synonyms=["project_code"], base_confidence=0.88)
    add("Campaign ID", PatternCategory.RETAIL, ["campaign_id", "marketing_id", "promo_id"],
        synonyms=["campaign_code"], base_confidence=0.87)
    add("Warehouse ID", PatternCategory.LOGISTICS, ["warehouse_id", "wh_id", "depot_id", "dc_id"],
        synonyms=["wh", "distribution_center"], base_confidence=0.89)

    # ── PERSONAL / PII (30) ──
    add("Full Name", PatternCategory.PERSONAL, ["name", "full_name", "fullname", "customer_name", "person_name"],
        regex_patterns=[r"^name$", r"full.?name"], sample_patterns=[r"^[A-Z][a-z]+\s[A-Z][a-z]+$"],
        synonyms=["complete_name", "display_name", "legal_name"], is_pii=True, compliance=["gdpr", "ccpa"], base_confidence=0.90)
    add("First Name", PatternCategory.PERSONAL, ["first_name", "firstname", "fname", "given_name", "forename"],
        sample_patterns=[r"^[A-Z][a-z]{1,15}$"], synonyms=["givenname", "first"],
        is_pii=True, compliance=["gdpr", "ccpa"], transformations=["mask", "pseudonymize"], base_confidence=0.92)
    add("Last Name", PatternCategory.PERSONAL, ["last_name", "lastname", "lname", "surname", "family_name"],
        sample_patterns=[r"^[A-Z][a-z]{1,20}$"], synonyms=["familyname", "last"],
        is_pii=True, compliance=["gdpr", "ccpa"], transformations=["mask", "pseudonymize"], base_confidence=0.92)
    add("Middle Name", PatternCategory.PERSONAL, ["middle_name", "middlename", "mname", "middle_initial"],
        synonyms=["mi"], is_pii=True, compliance=["gdpr"], base_confidence=0.88)
    add("Date of Birth", PatternCategory.PERSONAL, ["dob", "date_of_birth", "birth_date", "birthdate", "birthday"],
        regex_patterns=[r"birth.*date", r"^dob$"], synonyms=["dateofbirth", "bday"],
        is_pii=True, compliance=["gdpr", "ccpa", "hipaa"], transformations=["age_bucket", "year_only"], base_confidence=0.95)
    add("Gender", PatternCategory.PERSONAL, ["gender", "sex", "gender_code"],
        sample_patterns=[r"^(M|F|Male|Female|Other)$"], is_pii=True, compliance=["gdpr"], base_confidence=0.90)
    add("Age", PatternCategory.PERSONAL, ["age", "customer_age", "person_age"],
        sample_patterns=[r"^\d{1,3}$"], is_pii=True, compliance=["gdpr", "ccpa"], transformations=["age_bucket"], base_confidence=0.85)
    add("Social Security Number", PatternCategory.PERSONAL, ["ssn", "social_security", "social_security_number"],
        regex_patterns=[r"^ssn$"], sample_patterns=[r"^\d{3}-\d{2}-\d{4}$", r"^\d{9}$"],
        synonyms=["ss_num", "socialsecurity"], is_pii=True, compliance=["gdpr", "ccpa", "sox", "glba"],
        transformations=["mask", "encrypt", "tokenize"], base_confidence=0.99)
    add("National ID", PatternCategory.PERSONAL, ["national_id", "passport", "passport_number", "drivers_license"],
        synonyms=["govt_id", "government_id", "id_number", "tax_id", "tin"],
        is_pii=True, compliance=["gdpr", "ccpa"], transformations=["mask", "encrypt"], base_confidence=0.95)
    add("Medical Record Number", PatternCategory.HEALTHCARE, ["mrn", "medical_record", "patient_id", "health_record"],
        is_pii=True, compliance=["hipaa", "gdpr"], transformations=["mask", "encrypt", "tokenize"], base_confidence=0.95)
    add("Health Insurance ID", PatternCategory.HEALTHCARE, ["insurance_id", "policy_number", "subscriber_id", "hic_number"],
        is_pii=True, compliance=["hipaa"], transformations=["mask", "encrypt"], base_confidence=0.92)
    add("Diagnosis Code", PatternCategory.HEALTHCARE, ["icd", "icd_code", "diagnosis", "diag_code", "icd10"],
        sample_patterns=[r"^[A-Z]\d{2}\.?\d{0,2}$"], is_pii=True, compliance=["hipaa"], transformations=["generalize"], base_confidence=0.90)
    add("Procedure Code", PatternCategory.HEALTHCARE, ["cpt", "cpt_code", "procedure", "proc_code"],
        synonyms=["treatment_code"], base_confidence=0.88)
    add("Medication", PatternCategory.HEALTHCARE, ["medication", "drug", "drug_name", "prescription", "rx", "ndc"],
        synonyms=["med", "ndc_code"], base_confidence=0.87)
    add("Provider NPI", PatternCategory.HEALTHCARE, ["npi", "npi_number", "physician", "doctor", "prescriber"],
        synonyms=["dr", "attending_physician"], base_confidence=0.89)
    add("Ethnicity", PatternCategory.PERSONAL, ["ethnicity", "race", "ethnic_group"],
        is_pii=True, compliance=["gdpr"], transformations=["generalize"], base_confidence=0.85)
    add("Marital Status", PatternCategory.PERSONAL, ["marital_status", "marriage_status"],
        synonyms=["marital"], is_pii=True, compliance=["gdpr"], base_confidence=0.85)
    add("Occupation", PatternCategory.PERSONAL, ["occupation", "job", "profession", "employment"],
        synonyms=["job_title", "career"], is_pii=True, compliance=["gdpr"], base_confidence=0.83)
    add("Income", PatternCategory.PERSONAL, ["income", "annual_income", "household_income", "salary_range"],
        is_pii=True, compliance=["gdpr", "ccpa"], transformations=["bucket", "generalize"], base_confidence=0.85)
    add("Biometric", PatternCategory.PERSONAL, ["fingerprint", "retina", "facial_recognition", "biometric"],
        is_pii=True, compliance=["gdpr"], transformations=["never_store", "hash"], base_confidence=0.95)
    add("Photo", PatternCategory.PERSONAL, ["photo", "picture", "image", "avatar", "profile_photo"],
        is_pii=True, compliance=["gdpr"], transformations=["blur", "remove"], base_confidence=0.88)
    add("Signature", PatternCategory.PERSONAL, ["signature", "digital_signature", "esign"],
        is_pii=True, compliance=["gdpr"], base_confidence=0.90)
    add("Mother Maiden Name", PatternCategory.PERSONAL, ["mother_maiden_name", "mothers_maiden", "maiden_name"],
        is_pii=True, compliance=["gdpr", "glba"], transformations=["mask"], base_confidence=0.92)
    add("Veteran Status", PatternCategory.PERSONAL, ["veteran", "veteran_status", "military_status"],
        is_pii=True, compliance=["gdpr"], base_confidence=0.85)
    add("Disability Status", PatternCategory.PERSONAL, ["disability", "disability_status", "handicap"],
        is_pii=True, compliance=["gdpr", "hipaa"], transformations=["generalize"], base_confidence=0.85)
    add("Religion", PatternCategory.PERSONAL, ["religion", "religious_affiliation", "faith"],
        is_pii=True, compliance=["gdpr"], transformations=["generalize"], base_confidence=0.85)
    add("Citizenship", PatternCategory.PERSONAL, ["citizenship", "nationality", "citizen_status"],
        is_pii=True, compliance=["gdpr"], base_confidence=0.85)
    add("Emergency Contact", PatternCategory.PERSONAL, ["emergency_contact", "emergency_name", "emergency_phone"],
        is_pii=True, compliance=["gdpr", "hipaa"], base_confidence=0.88)
    add("Allergies", PatternCategory.HEALTHCARE, ["allergy", "allergies", "allergen", "adverse_reaction"],
        is_pii=True, compliance=["hipaa"], base_confidence=0.87)
    add("Blood Type", PatternCategory.HEALTHCARE, ["blood_type", "blood_group"],
        sample_patterns=[r"^(A|B|AB|O)[+-]?$"], is_pii=True, compliance=["hipaa"], base_confidence=0.90)

    # ── CONTACT (15) ──
    add("Email Address", PatternCategory.CONTACT, ["email", "email_address", "emailaddress", "e_mail", "mail"],
        regex_patterns=[r"e.?mail"], sample_patterns=[r"^[\w\.-]+@[\w\.-]+\.\w{2,}$"],
        synonyms=["email_id", "contact_email", "work_email"], is_pii=True, compliance=["gdpr", "ccpa", "tcpa"],
        transformations=["mask_email", "hash"], base_confidence=0.98)
    add("Phone Number", PatternCategory.CONTACT, ["phone", "telephone", "mobile", "cell", "phone_number"],
        regex_patterns=[r"phone", r"mobile", r"cell"], sample_patterns=[r"^\+?1?\d{10,14}$", r"^\d{3}-\d{3}-\d{4}$"],
        synonyms=["phoneno", "ph_no", "contact_phone", "mobile_phone"], is_pii=True, compliance=["gdpr", "ccpa", "tcpa"],
        transformations=["mask", "format_e164"], base_confidence=0.96)
    add("Fax Number", PatternCategory.CONTACT, ["fax", "fax_number", "facsimile"],
        synonyms=["fax_no"], is_pii=True, compliance=["gdpr"], base_confidence=0.88)
    add("Street Address", PatternCategory.GEOGRAPHIC, ["address", "street", "street_address", "address_line", "addr"],
        regex_patterns=[r"address.*line"], sample_patterns=[r"^\d+\s+\w+"],
        synonyms=["addr1", "addr2", "mailing_address", "shipping_address"], is_pii=True, compliance=["gdpr", "ccpa"],
        transformations=["mask", "generalize_to_zip"], base_confidence=0.88)
    add("City", PatternCategory.GEOGRAPHIC, ["city", "town", "municipality", "locality"],
        synonyms=["city_name", "shipping_city"], is_pii=True, compliance=["gdpr"], base_confidence=0.85)
    add("State/Province", PatternCategory.GEOGRAPHIC, ["state", "province", "region", "state_code"],
        sample_patterns=[r"^[A-Z]{2}$"], synonyms=["state_name", "st", "prov"], base_confidence=0.85)
    add("Postal Code", PatternCategory.GEOGRAPHIC, ["zip", "zipcode", "zip_code", "postal_code", "postcode"],
        regex_patterns=[r"zip", r"postal.*code"], sample_patterns=[r"^\d{5}(-\d{4})?$"],
        synonyms=["postalcode", "zip5"], is_pii=True, compliance=["gdpr"], transformations=["truncate_to_3"], base_confidence=0.92)
    add("Country", PatternCategory.GEOGRAPHIC, ["country", "country_code", "nation"],
        sample_patterns=[r"^[A-Z]{2,3}$"], synonyms=["country_name", "ctry"], base_confidence=0.88)
    add("Latitude", PatternCategory.GEOGRAPHIC, ["lat", "latitude"],
        sample_patterns=[r"^-?\d{1,2}\.\d+$"], base_confidence=0.90)
    add("Longitude", PatternCategory.GEOGRAPHIC, ["lon", "lng", "longitude"],
        sample_patterns=[r"^-?\d{1,3}\.\d+$"], base_confidence=0.90)
    add("Geohash", PatternCategory.GEOGRAPHIC, ["geohash", "geo_hash"],
        base_confidence=0.85)
    add("Timezone", PatternCategory.GEOGRAPHIC, ["timezone", "tz", "time_zone", "utc_offset"],
        synonyms=["tz_name"], base_confidence=0.87)
    add("County", PatternCategory.GEOGRAPHIC, ["county", "parish", "borough"],
        base_confidence=0.83)
    add("Continent", PatternCategory.GEOGRAPHIC, ["continent", "continent_code"],
        base_confidence=0.80)
    add("Coordinates", PatternCategory.GEOGRAPHIC, ["coordinates", "geo_point", "location_coords"],
        base_confidence=0.85)

    # ── FINANCIAL (25) ──
    add("Credit Card Number", PatternCategory.FINANCIAL, ["credit_card", "card_number", "cc_number", "card_no", "pan"],
        regex_patterns=[r"credit.*card"], sample_patterns=[r"^\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}$"],
        synonyms=["ccn", "cardnumber"], is_pii=True, compliance=["pci_dss", "gdpr"],
        transformations=["mask_pan", "tokenize"], base_confidence=0.99)
    add("CVV", PatternCategory.FINANCIAL, ["cvv", "cvc", "security_code", "card_security"],
        sample_patterns=[r"^\d{3,4}$"], is_pii=True, compliance=["pci_dss"], transformations=["remove"], base_confidence=0.98)
    add("Bank Account Number", PatternCategory.FINANCIAL, ["account_number", "bank_account", "acct_no"],
        regex_patterns=[r"account.*num"], synonyms=["bankaccount", "acctnum"],
        is_pii=True, compliance=["glba", "gdpr"], transformations=["mask", "encrypt"], base_confidence=0.92)
    add("Routing Number", PatternCategory.FINANCIAL, ["routing_number", "routing_no", "aba_number", "sort_code"],
        sample_patterns=[r"^\d{9}$"], is_pii=True, compliance=["glba"], transformations=["mask"], base_confidence=0.90)
    add("IBAN", PatternCategory.FINANCIAL, ["iban", "international_bank_account"],
        sample_patterns=[r"^[A-Z]{2}\d{2}[A-Z0-9]{11,30}$"], is_pii=True, compliance=["gdpr", "glba"],
        transformations=["mask", "encrypt"], base_confidence=0.95)
    add("SWIFT/BIC", PatternCategory.FINANCIAL, ["swift", "bic", "swift_code", "bic_code"],
        sample_patterns=[r"^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$"], base_confidence=0.92)
    add("Currency Amount", PatternCategory.FINANCIAL, ["amount", "price", "cost", "total", "subtotal", "revenue", "payment"],
        regex_patterns=[r"amount", r"price", r"_amt$", r"_val$"], sample_patterns=[r"^\$?\d+\.?\d{0,2}$"],
        synonyms=["amt", "value", "sum", "charge", "fee", "balance"], transformations=["round", "convert_currency"],
        base_confidence=0.88, data_type="decimal")
    add("Currency Code", PatternCategory.FINANCIAL, ["currency", "currency_code", "ccy"],
        sample_patterns=[r"^[A-Z]{3}$"], synonyms=["curr_code"], base_confidence=0.90)
    add("Tax Amount", PatternCategory.FINANCIAL, ["tax_amount", "tax_amt", "tax", "vat", "gst", "sales_tax"],
        synonyms=["tax_total", "tax_value"], base_confidence=0.87, data_type="decimal")
    add("Discount", PatternCategory.FINANCIAL, ["discount", "disc", "discount_amount", "rebate", "coupon_value"],
        synonyms=["disc_amt", "promo_amount"], base_confidence=0.86, data_type="decimal")
    add("Interest Rate", PatternCategory.FINANCIAL, ["interest_rate", "int_rate", "apr", "annual_rate"],
        synonyms=["rate", "interest_pct"], base_confidence=0.88, data_type="decimal")
    add("Credit Score", PatternCategory.FINANCIAL, ["credit_score", "fico", "fico_score", "credit_rating"],
        synonyms=["risk_score"], base_confidence=0.90, data_type="integer")
    add("Premium Amount", PatternCategory.INSURANCE, ["premium", "premium_amount", "premium_amt"],
        synonyms=["insurance_premium"], base_confidence=0.88, data_type="decimal")
    add("Deductible", PatternCategory.INSURANCE, ["deductible", "deductible_amount"],
        base_confidence=0.87, data_type="decimal")
    add("Claim Amount", PatternCategory.INSURANCE, ["claim_amount", "claim_amt", "payout_amount"],
        base_confidence=0.88, data_type="decimal")
    add("Net Amount", PatternCategory.FINANCIAL, ["net_amount", "net_amt", "net_total", "net_value"],
        synonyms=["net"], base_confidence=0.87, data_type="decimal")
    add("Gross Amount", PatternCategory.FINANCIAL, ["gross_amount", "gross_amt", "gross_total", "gross_value"],
        synonyms=["gross"], base_confidence=0.87, data_type="decimal")
    add("Unit Price", PatternCategory.FINANCIAL, ["unit_price", "item_price", "list_price", "sale_price"],
        synonyms=["price_per_unit"], base_confidence=0.88, data_type="decimal")
    add("Commission", PatternCategory.FINANCIAL, ["commission", "commission_amount", "comm_amt"],
        base_confidence=0.86, data_type="decimal")
    add("Fee", PatternCategory.FINANCIAL, ["fee", "service_fee", "processing_fee", "transaction_fee"],
        synonyms=["fee_amount"], base_confidence=0.85, data_type="decimal")
    add("Budget", PatternCategory.FINANCIAL, ["budget", "budget_amount", "allocated_amount"],
        base_confidence=0.84, data_type="decimal")
    add("Cost Center", PatternCategory.FINANCIAL, ["cost_center", "cost_centre", "cc_code"],
        base_confidence=0.85)
    add("Exchange Rate", PatternCategory.FINANCIAL, ["exchange_rate", "fx_rate", "conversion_rate"],
        base_confidence=0.88, data_type="decimal")
    add("Profit Margin", PatternCategory.FINANCIAL, ["profit_margin", "margin", "margin_pct"],
        base_confidence=0.86, data_type="decimal")
    add("ROI", PatternCategory.FINANCIAL, ["roi", "return_on_investment", "return_pct"],
        base_confidence=0.85, data_type="decimal")

    # ── TEMPORAL (15) ──
    add("Date", PatternCategory.TEMPORAL, ["date", "dt"],
        regex_patterns=[r"_date$", r"_dt$"], sample_patterns=[r"^\d{4}-\d{2}-\d{2}$", r"^\d{2}/\d{2}/\d{4}$"],
        synonyms=["effective_date", "as_of_date"], transformations=["standardize_iso8601"], base_confidence=0.88, data_type="date")
    add("Timestamp", PatternCategory.TEMPORAL, ["timestamp", "datetime", "created_at", "updated_at", "modified_at"],
        regex_patterns=[r"_at$"], sample_patterns=[r"^\d{4}-\d{2}-\d{2}[T\s]\d{2}:\d{2}:\d{2}"],
        synonyms=["ts", "time_stamp"], transformations=["standardize_iso8601"], base_confidence=0.92, data_type="datetime")
    add("Time", PatternCategory.TEMPORAL, ["time", "hour", "start_time", "end_time"],
        sample_patterns=[r"^\d{2}:\d{2}(:\d{2})?$"], base_confidence=0.85)
    add("Year", PatternCategory.TEMPORAL, ["year", "yr", "fiscal_year", "calendar_year"],
        sample_patterns=[r"^(19|20)\d{2}$"], base_confidence=0.88, data_type="integer")
    add("Month", PatternCategory.TEMPORAL, ["month", "mo", "month_name", "month_num"],
        sample_patterns=[r"^(0?[1-9]|1[0-2])$"], base_confidence=0.85)
    add("Quarter", PatternCategory.TEMPORAL, ["quarter", "qtr", "fiscal_quarter"],
        sample_patterns=[r"^Q[1-4]$"], base_confidence=0.85)
    add("Week", PatternCategory.TEMPORAL, ["week", "week_number", "week_of_year"],
        base_confidence=0.83)
    add("Day of Week", PatternCategory.TEMPORAL, ["day_of_week", "dow", "weekday"],
        base_confidence=0.82)
    add("Duration", PatternCategory.TEMPORAL, ["duration", "elapsed", "elapsed_time", "processing_time"],
        synonyms=["lead_time", "cycle_time"], base_confidence=0.85)
    add("Expiry Date", PatternCategory.TEMPORAL, ["expiry_date", "expiration_date", "exp_date", "valid_until"],
        synonyms=["expires_at"], base_confidence=0.90, data_type="date")
    add("Hire Date", PatternCategory.HR, ["hire_date", "start_date", "employment_date", "join_date"],
        synonyms=["onboarding_date"], base_confidence=0.90, data_type="date")
    add("Termination Date", PatternCategory.HR, ["termination_date", "end_date", "exit_date", "last_day"],
        synonyms=["separation_date"], base_confidence=0.90, data_type="date")
    add("Due Date", PatternCategory.TEMPORAL, ["due_date", "payment_due", "deadline"],
        base_confidence=0.88, data_type="date")
    add("Ship Date", PatternCategory.LOGISTICS, ["ship_date", "shipping_date", "dispatch_date"],
        base_confidence=0.90, data_type="datetime")
    add("Delivery Date", PatternCategory.LOGISTICS, ["delivery_date", "delivered_at", "arrival_date"],
        base_confidence=0.90, data_type="datetime")

    # ── NUMERIC (15) ──
    add("Quantity", PatternCategory.NUMERIC, ["quantity", "qty", "count", "num", "number_of"],
        regex_patterns=[r"quantity", r"_qty$", r"_count$"], sample_patterns=[r"^\d+$"],
        synonyms=["units", "items", "pieces"], transformations=["validate_positive"], base_confidence=0.85, data_type="integer")
    add("Percentage", PatternCategory.NUMERIC, ["percent", "pct", "rate", "ratio", "percentage"],
        regex_patterns=[r"_pct$", r"percent"], sample_patterns=[r"^\d{1,3}\.?\d{0,2}%?$"],
        synonyms=["proportion"], base_confidence=0.88, data_type="decimal")
    add("Score", PatternCategory.NUMERIC, ["score", "rating", "rank", "grade"],
        sample_patterns=[r"^\d{1,3}\.?\d{0,2}$"], base_confidence=0.85, data_type="decimal")
    add("Weight", PatternCategory.LOGISTICS, ["weight", "wt", "gross_weight", "net_weight", "shipping_weight"],
        synonyms=["package_weight", "mass"], base_confidence=0.87, data_type="decimal")
    add("Volume", PatternCategory.LOGISTICS, ["volume", "cubic_feet", "cbm", "cubic_meters"],
        base_confidence=0.85, data_type="decimal")
    add("Distance", PatternCategory.LOGISTICS, ["distance", "miles", "kilometers", "km", "mi"],
        base_confidence=0.85, data_type="decimal")
    add("Inventory Level", PatternCategory.RETAIL, ["inventory", "stock", "stock_level", "on_hand", "available_qty"],
        synonyms=["reserved_qty", "backorder_qty"], base_confidence=0.86, data_type="integer")
    add("Temperature", PatternCategory.MANUFACTURING, ["temperature", "temp", "celsius", "fahrenheit"],
        base_confidence=0.87, data_type="decimal")
    add("Pressure", PatternCategory.MANUFACTURING, ["pressure", "psi", "bar", "pascal"],
        base_confidence=0.85, data_type="decimal")
    add("Speed", PatternCategory.LOGISTICS, ["speed", "velocity", "mph", "kph"],
        base_confidence=0.84, data_type="decimal")
    add("Frequency", PatternCategory.NUMERIC, ["frequency", "freq", "occurrence_count"],
        base_confidence=0.83, data_type="integer")
    add("Sequence Number", PatternCategory.NUMERIC, ["sequence", "seq_no", "seq_num", "line_number"],
        synonyms=["line_no", "row_num"], base_confidence=0.85, data_type="integer")
    add("Version Number", PatternCategory.METADATA, ["version", "ver", "revision", "rev", "build"],
        synonyms=["release"], base_confidence=0.85)
    add("Port Number", PatternCategory.METADATA, ["port", "port_number", "port_no"],
        sample_patterns=[r"^\d{1,5}$"], base_confidence=0.88, data_type="integer")
    add("Bandwidth", PatternCategory.TELECOM, ["bandwidth", "data_rate", "throughput"],
        base_confidence=0.84, data_type="decimal")

    # ── STATUS & FLAGS (12) ──
    add("Status", PatternCategory.STATUS, ["status", "state", "condition", "stage"],
        sample_patterns=[r"^(active|inactive|pending|complete)$"], synonyms=["sts", "current_status", "order_status"],
        base_confidence=0.85)
    add("Boolean Flag", PatternCategory.BINARY, ["is_", "has_", "can_", "flag", "enabled", "active", "deleted"],
        regex_patterns=[r"^is_", r"^has_", r"_flag$"], sample_patterns=[r"^(true|false|1|0|yes|no)$"],
        synonyms=["indicator", "bool"], base_confidence=0.90, data_type="boolean")
    add("Priority", PatternCategory.STATUS, ["priority", "urgency", "importance", "severity"],
        sample_patterns=[r"^(low|medium|high|critical)$"], base_confidence=0.88)
    add("Approval Status", PatternCategory.STATUS, ["approval_status", "approved", "approval_state"],
        base_confidence=0.86)
    add("Payment Status", PatternCategory.STATUS, ["payment_status", "pay_status", "paid", "unpaid"],
        base_confidence=0.87)
    add("Shipping Status", PatternCategory.LOGISTICS, ["shipping_status", "ship_status", "delivery_status"],
        base_confidence=0.87)
    add("Employment Status", PatternCategory.HR, ["employment_status", "emp_status", "active_employee"],
        base_confidence=0.86)
    add("Enrollment Status", PatternCategory.EDUCATION, ["enrollment_status", "enroll_status", "registered"],
        base_confidence=0.85)
    add("Claim Status", PatternCategory.INSURANCE, ["claim_status", "claims_status"],
        base_confidence=0.86)
    add("Subscription Status", PatternCategory.TELECOM, ["subscription_status", "sub_status", "plan_status"],
        base_confidence=0.86)
    add("Listing Status", PatternCategory.REAL_ESTATE, ["listing_status", "property_status"],
        base_confidence=0.85)
    add("Quality Status", PatternCategory.MANUFACTURING, ["quality_status", "qc_status", "pass_fail"],
        base_confidence=0.86)

    # ── TEXT & METADATA (20) ──
    add("Description", PatternCategory.TEXT, ["description", "desc", "details", "notes", "comments", "remarks"],
        synonyms=["descr", "comment", "note", "memo", "summary"], base_confidence=0.85)
    add("URL", PatternCategory.TEXT, ["url", "link", "website", "web_address", "uri", "href"],
        sample_patterns=[r"^https?://"], synonyms=["web_url"], base_confidence=0.92)
    add("IP Address", PatternCategory.METADATA, ["ip", "ip_address", "ipaddress", "client_ip", "source_ip"],
        sample_patterns=[r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"], is_pii=True, compliance=["gdpr"],
        transformations=["mask", "hash"], base_confidence=0.95)
    add("User Agent", PatternCategory.METADATA, ["user_agent", "browser", "client_info"],
        base_confidence=0.85)
    add("Hash", PatternCategory.METADATA, ["hash", "checksum", "md5", "sha256", "sha1"],
        base_confidence=0.88)
    add("Token", PatternCategory.METADATA, ["token", "access_token", "api_key", "auth_token"],
        is_pii=True, compliance=["pci_dss"], transformations=["mask", "never_log"], base_confidence=0.90)
    add("Language", PatternCategory.METADATA, ["language", "lang", "locale", "language_code"],
        sample_patterns=[r"^[a-z]{2}(-[A-Z]{2})?$"], base_confidence=0.87)
    add("Encoding", PatternCategory.METADATA, ["encoding", "charset", "character_set"],
        base_confidence=0.83)
    add("MIME Type", PatternCategory.METADATA, ["mime_type", "content_type", "media_type"],
        base_confidence=0.85)
    add("File Name", PatternCategory.METADATA, ["filename", "file_name", "document_name"],
        base_confidence=0.85)
    add("File Size", PatternCategory.METADATA, ["file_size", "size_bytes", "content_length"],
        base_confidence=0.86, data_type="integer")
    add("Content", PatternCategory.TEXT, ["content", "body", "text", "message", "html"],
        base_confidence=0.80)
    add("Title", PatternCategory.TEXT, ["title", "heading", "subject", "headline"],
        base_confidence=0.83)
    add("Tag", PatternCategory.METADATA, ["tag", "tags", "label", "keyword", "keywords"],
        base_confidence=0.82)
    add("Category", PatternCategory.METADATA, ["category", "cat", "type", "classification", "segment"],
        synonyms=["group", "class", "kind"], base_confidence=0.84)
    add("Brand", PatternCategory.RETAIL, ["brand", "brand_name", "manufacturer", "mfr", "make"],
        base_confidence=0.86)
    add("Color", PatternCategory.RETAIL, ["color", "colour", "color_code"],
        base_confidence=0.83)
    add("Size", PatternCategory.RETAIL, ["size", "size_code", "dimension"],
        base_confidence=0.82)
    add("Barcode", PatternCategory.RETAIL, ["barcode", "bar_code", "gtin"],
        base_confidence=0.90)
    add("QR Code", PatternCategory.RETAIL, ["qr_code", "qrcode", "qr"],
        base_confidence=0.88)

    # ── LOGISTICS (10) ──
    add("Carrier", PatternCategory.LOGISTICS, ["carrier", "shipper", "shipping_company", "courier"],
        synonyms=["logistics_provider", "freight_carrier"], base_confidence=0.87)
    add("Tracking Number", PatternCategory.LOGISTICS, ["tracking_number", "tracking_no", "track_num"],
        base_confidence=0.91)
    add("Freight Cost", PatternCategory.LOGISTICS, ["freight_cost", "shipping_cost", "freight_amt"],
        synonyms=["shipping_fee"], base_confidence=0.87, data_type="decimal")
    add("Origin", PatternCategory.LOGISTICS, ["origin", "origin_city", "ship_from", "source_location"],
        base_confidence=0.85)
    add("Destination", PatternCategory.LOGISTICS, ["destination", "dest_city", "ship_to", "target_location"],
        base_confidence=0.85)
    add("Container ID", PatternCategory.LOGISTICS, ["container_id", "container_number", "container_no"],
        base_confidence=0.89)
    add("Pallet ID", PatternCategory.LOGISTICS, ["pallet_id", "pallet_number", "pallet_no"],
        base_confidence=0.87)
    add("Dock Door", PatternCategory.LOGISTICS, ["dock_door", "dock", "bay_number"],
        base_confidence=0.84)
    add("Customs Code", PatternCategory.LOGISTICS, ["hs_code", "tariff_code", "customs_code", "harmonized_code"],
        base_confidence=0.88)
    add("Incoterms", PatternCategory.LOGISTICS, ["incoterms", "incoterm", "shipping_terms"],
        base_confidence=0.85)

    # ── HR (8) ──
    add("Department", PatternCategory.HR, ["department", "dept", "division", "business_unit", "cost_center"],
        synonyms=["bu", "org_unit"], base_confidence=0.86)
    add("Job Title", PatternCategory.HR, ["job_title", "title", "position", "role", "designation"],
        synonyms=["job_role"], base_confidence=0.87)
    add("Salary", PatternCategory.HR, ["salary", "compensation", "pay", "wage", "base_salary"],
        synonyms=["annual_salary", "hourly_rate"], is_pii=True, compliance=["gdpr"], base_confidence=0.88, data_type="decimal")
    add("Manager ID", PatternCategory.HR, ["manager_id", "supervisor_id", "reports_to"],
        base_confidence=0.87)
    add("Benefits", PatternCategory.HR, ["benefits", "benefit_plan", "health_plan"],
        base_confidence=0.84)
    add("PTO Balance", PatternCategory.HR, ["pto", "pto_balance", "vacation_days", "sick_days"],
        base_confidence=0.85, data_type="decimal")
    add("Performance Rating", PatternCategory.HR, ["performance_rating", "perf_rating", "review_score"],
        base_confidence=0.86, data_type="decimal")
    add("Employee Type", PatternCategory.HR, ["employee_type", "emp_type", "employment_type"],
        synonyms=["full_time", "part_time", "contractor"], base_confidence=0.85)

    # ── REAL ESTATE (5) ──
    add("Square Feet", PatternCategory.REAL_ESTATE, ["square_feet", "sqft", "sq_ft", "living_area"],
        synonyms=["lot_size"], base_confidence=0.88, data_type="integer")
    add("Bedrooms", PatternCategory.REAL_ESTATE, ["bedrooms", "beds", "bedroom_count"],
        synonyms=["num_bedrooms"], base_confidence=0.88, data_type="integer")
    add("Bathrooms", PatternCategory.REAL_ESTATE, ["bathrooms", "baths", "bathroom_count"],
        synonyms=["num_bathrooms"], base_confidence=0.88, data_type="decimal")
    add("List Price", PatternCategory.REAL_ESTATE, ["list_price", "asking_price", "sale_price"],
        base_confidence=0.88, data_type="decimal")
    add("Property Type", PatternCategory.REAL_ESTATE, ["property_type", "home_type", "building_type"],
        base_confidence=0.85)

    # ── ADDITIONAL UNIVERSAL TYPES (25+) ──
    add("UUID", PatternCategory.IDENTIFIER, ["uuid", "guid", "unique_id", "universal_id"],
        sample_patterns=[r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"], base_confidence=0.93)
    add("MAC Address", PatternCategory.METADATA, ["mac_address", "mac", "mac_addr", "hardware_address"],
        sample_patterns=[r"^([0-9A-Fa-f]{2}:){5}[0-9A-Fa-f]{2}$"], base_confidence=0.92)
    add("Hostname", PatternCategory.METADATA, ["hostname", "host_name", "server_name", "machine_name"],
        base_confidence=0.87)
    add("Port", PatternCategory.METADATA, ["port", "port_number", "tcp_port", "udp_port"],
        base_confidence=0.86, data_type="integer")
    add("API Key", PatternCategory.METADATA, ["api_key", "api_secret", "access_key", "secret_key"],
        is_pii=True, compliance=["pci_dss"], transformations=["mask", "never_log"], base_confidence=0.91)
    add("OAuth Token", PatternCategory.METADATA, ["oauth_token", "refresh_token", "bearer_token"],
        is_pii=True, transformations=["mask", "never_store"], base_confidence=0.90)
    add("JSON Payload", PatternCategory.TEXT, ["json", "json_data", "payload", "raw_json", "json_body"],
        base_confidence=0.85, data_type="json")
    add("XML Data", PatternCategory.TEXT, ["xml", "xml_data", "xml_body", "soap_body"],
        base_confidence=0.84)
    add("Binary Data", PatternCategory.BINARY, ["binary", "blob", "bytea", "raw_bytes", "binary_data"],
        base_confidence=0.83, data_type="binary")
    add("Base64", PatternCategory.BINARY, ["base64", "b64", "encoded_data"],
        base_confidence=0.85)
    add("Checksum", PatternCategory.METADATA, ["checksum", "hash_value", "digest", "fingerprint"],
        base_confidence=0.88)
    add("Locale", PatternCategory.METADATA, ["locale", "locale_code", "i18n", "language_locale"],
        base_confidence=0.86)
    add("Referrer", PatternCategory.METADATA, ["referrer", "referer", "referral_url", "source_url"],
        base_confidence=0.84)
    add("Campaign Source", PatternCategory.METADATA, ["utm_source", "utm_medium", "utm_campaign", "utm_term"],
        base_confidence=0.85)
    add("Event Type", PatternCategory.METADATA, ["event_type", "event_name", "action", "event_category"],
        base_confidence=0.86)
    add("Error Code", PatternCategory.STATUS, ["error_code", "err_code", "exception_code", "fault_code"],
        base_confidence=0.87)
    add("HTTP Status", PatternCategory.STATUS, ["http_status", "status_code", "response_code"],
        sample_patterns=[r"^[1-5]\d{2}$"], base_confidence=0.90, data_type="integer")
    add("Latency", PatternCategory.NUMERIC, ["latency", "response_time", "rt", "round_trip"],
        base_confidence=0.85, data_type="decimal")
    add("Throughput", PatternCategory.NUMERIC, ["throughput", "tps", "qps", "requests_per_second"],
        base_confidence=0.84, data_type="decimal")
    add("Memory Usage", PatternCategory.NUMERIC, ["memory", "mem_usage", "ram", "heap_size"],
        base_confidence=0.83, data_type="decimal")
    add("CPU Usage", PatternCategory.NUMERIC, ["cpu", "cpu_usage", "cpu_percent", "processor_load"],
        base_confidence=0.83, data_type="decimal")
    add("Disk Usage", PatternCategory.NUMERIC, ["disk", "disk_usage", "storage_used", "disk_space"],
        base_confidence=0.83, data_type="decimal")
    add("Record Count", PatternCategory.NUMERIC, ["record_count", "row_count", "total_records", "num_records"],
        base_confidence=0.86, data_type="integer")
    add("Page Number", PatternCategory.NUMERIC, ["page", "page_number", "page_num", "page_no"],
        base_confidence=0.84, data_type="integer")
    add("Offset", PatternCategory.NUMERIC, ["offset", "skip", "start_index", "from_index"],
        base_confidence=0.83, data_type="integer")
    add("Limit", PatternCategory.NUMERIC, ["limit", "max_results", "page_size", "batch_size"],
        base_confidence=0.83, data_type="integer")
    add("Sort Order", PatternCategory.METADATA, ["sort_order", "order_by", "sort_direction", "sort_key"],
        base_confidence=0.82)
    add("Namespace", PatternCategory.METADATA, ["namespace", "ns", "schema_name", "db_schema"],
        base_confidence=0.84)
    add("Table Name", PatternCategory.METADATA, ["table_name", "table", "collection_name", "entity_name"],
        base_confidence=0.85)
    add("Column Name", PatternCategory.METADATA, ["column_name", "field_name", "attribute_name", "property_name"],
        base_confidence=0.84)

    return patterns


SEMANTIC_PATTERNS: list[SemanticPattern] = _build_patterns()


def get_all_patterns() -> list[SemanticPattern]:
    return SEMANTIC_PATTERNS


def get_pattern_by_name(name: str) -> SemanticPattern | None:
    for p in SEMANTIC_PATTERNS:
        if p.name.lower() == name.lower():
            return p
    return None


def get_pattern_count() -> int:
    return len(SEMANTIC_PATTERNS)
