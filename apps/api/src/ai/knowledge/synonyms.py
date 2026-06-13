"""
DataTransfer.space — Synonym Dictionary

Comprehensive synonym mappings for column name normalization.
Handles abbreviations like AMT=amount, cust=customer, qty=quantity.
"""

from __future__ import annotations

# Canonical form -> list of synonyms/abbreviations
SYNONYM_DICTIONARY: dict[str, list[str]] = {
    # Identifiers
    "id": ["id", "pk", "key", "uid", "uuid", "guid", "identifier", "record_id", "row_id", "seq", "sequence"],
    "customer_id": ["customer_id", "cust_id", "custid", "client_id", "clientid", "account_id", "acct_id", "user_id", "userid", "member_id", "memberid", "buyer_id", "subscriber_id"],
    "order_id": ["order_id", "orderid", "order_no", "order_number", "ordernumber", "order_num", "purchase_id", "transaction_id", "txn_id", "trans_id", "invoice_id", "receipt_id"],
    "product_id": ["product_id", "productid", "prod_id", "prodid", "item_id", "itemid", "sku", "upc", "ean", "asin", "part_number", "part_no", "material_id"],
    "employee_id": ["employee_id", "emp_id", "empid", "staff_id", "worker_id", "personnel_id", "badge_id", "badge_number"],
    "vendor_id": ["vendor_id", "vendorid", "supplier_id", "supplierid", "seller_id", "merchant_id"],
    "shipment_id": ["shipment_id", "ship_id", "tracking_id", "tracking_number", "tracking_no", "consignment_id", "waybill", "bol_number"],
    "patient_id": ["patient_id", "patientid", "mrn", "medical_record_number", "health_record_id", "chart_id"],
    "policy_id": ["policy_id", "policy_number", "policy_no", "insurance_id", "coverage_id"],
    "account_number": ["account_number", "account_no", "acct_no", "acctnum", "account_num", "bank_account", "bank_account_number"],

    # Personal
    "name": ["name", "full_name", "fullname", "complete_name", "display_name", "legal_name", "person_name"],
    "first_name": ["first_name", "firstname", "fname", "given_name", "givenname", "forename", "first"],
    "last_name": ["last_name", "lastname", "lname", "surname", "family_name", "familyname", "last"],
    "middle_name": ["middle_name", "middlename", "mname", "middle_initial", "mi"],
    "date_of_birth": ["date_of_birth", "dob", "birth_date", "birthdate", "birthday", "dateofbirth", "bday", "birth_dt"],
    "gender": ["gender", "sex", "gender_code", "gender_identity"],
    "age": ["age", "customer_age", "person_age", "years_old"],
    "ssn": ["ssn", "social_security", "social_security_number", "ss_number", "ss_num", "socialsecurity", "national_insurance"],
    "national_id": ["national_id", "national_identifier", "nino", "passport", "passport_number", "drivers_license", "license_number", "govt_id", "government_id", "id_number", "tax_id", "tin", "ein"],

    # Contact
    "email": ["email", "email_address", "emailaddress", "e_mail", "mail", "email_id", "contact_email", "work_email", "personal_email", "email_addr"],
    "phone": ["phone", "telephone", "mobile", "cell", "phone_number", "contact_number", "phoneno", "ph_no", "contact_phone", "work_phone", "home_phone", "mobile_phone", "cell_phone", "tel", "fax"],
    "address": ["address", "street", "street_address", "address_line", "addr", "addr1", "addr2", "address1", "address2", "mailing_address", "shipping_address", "billing_address", "street_addr"],
    "city": ["city", "town", "municipality", "locality", "city_name", "shipping_city", "billing_city"],
    "state": ["state", "province", "region", "state_code", "state_name", "st", "prov", "state_province"],
    "postal_code": ["postal_code", "zip", "zipcode", "zip_code", "postcode", "postalcode", "zip5", "zip9", "post_code"],
    "country": ["country", "country_code", "nation", "country_name", "ctry", "country_iso", "iso_country"],

    # Financial
    "amount": ["amount", "amt", "value", "sum", "total", "subtotal", "price", "cost", "charge", "fee", "balance", "payment", "revenue", "sales", "net_amount", "gross_amount", "unit_price", "list_price", "sale_price", "order_amt", "invoice_amt", "payment_amt", "trans_amt", "txn_amt"],
    "currency": ["currency", "currency_code", "ccy", "curr_code", "money_type", "currency_type"],
    "credit_card": ["credit_card", "card_number", "cc_number", "card_no", "pan", "ccn", "cardnumber", "debit_card", "payment_card"],
    "cvv": ["cvv", "cvc", "security_code", "card_security", "card_cvv"],
    "routing_number": ["routing_number", "routing_no", "aba_number", "sort_code", "bsb", "swift", "bic", "iban"],
    "tax_amount": ["tax_amount", "tax_amt", "tax", "vat", "gst", "sales_tax", "tax_total", "tax_value"],
    "discount": ["discount", "disc", "discount_amount", "disc_amt", "rebate", "coupon_value", "promo_amount"],
    "interest_rate": ["interest_rate", "int_rate", "rate", "apr", "interest_pct", "annual_rate"],
    "credit_score": ["credit_score", "fico", "fico_score", "credit_rating", "risk_score"],

    # Temporal
    "date": ["date", "dt", "effective_date", "as_of_date", "report_date", "business_date", "trans_date", "transaction_date"],
    "timestamp": ["timestamp", "datetime", "created_at", "updated_at", "modified_at", "ts", "time_stamp", "inserted_at", "deleted_at", "last_modified", "record_date"],
    "time": ["time", "hour", "start_time", "end_time", "open_time", "close_time"],
    "year": ["year", "yr", "fiscal_year", "calendar_year", "reporting_year"],
    "month": ["month", "mo", "month_name", "month_num", "reporting_month"],
    "quarter": ["quarter", "qtr", "fiscal_quarter", "reporting_quarter"],
    "duration": ["duration", "elapsed", "elapsed_time", "processing_time", "lead_time", "cycle_time"],

    # Geographic / Logistics
    "latitude": ["latitude", "lat", "geo_lat", "y_coord"],
    "longitude": ["longitude", "lon", "lng", "geo_lon", "geo_lng", "x_coord"],
    "warehouse": ["warehouse", "wh", "warehouse_id", "wh_id", "depot", "distribution_center", "dc", "fulfillment_center", "fc"],
    "location": ["location", "loc", "location_id", "loc_id", "site", "site_id", "facility", "facility_id", "store", "store_id", "branch", "branch_id"],
    "region": ["region", "territory", "zone", "area", "district", "market", "geo_region"],
    "carrier": ["carrier", "shipper", "shipping_company", "courier", "logistics_provider", "freight_carrier"],
    "weight": ["weight", "wt", "gross_weight", "net_weight", "shipping_weight", "package_weight", "mass"],
    "dimensions": ["dimensions", "dim", "length", "width", "height", "size", "volume", "cubic_feet", "cbm"],

    # Numeric / Quantities
    "quantity": ["quantity", "qty", "count", "num", "number_of", "units", "items", "pieces", "unit_count", "item_count", "order_qty", "ship_qty", "stock_qty", "inventory_qty"],
    "percentage": ["percentage", "percent", "pct", "rate", "ratio", "proportion", "pct_complete", "completion_rate"],
    "score": ["score", "rating", "rank", "grade", "points", "stars", "review_score", "nps"],
    "inventory": ["inventory", "stock", "stock_level", "on_hand", "available_qty", "reserved_qty", "backorder_qty"],

    # Status / Flags
    "status": ["status", "state", "condition", "stage", "sts", "current_status", "order_status", "ship_status", "payment_status", "record_status"],
    "flag": ["flag", "is_active", "is_deleted", "enabled", "active", "deleted", "indicator", "bool", "boolean", "is_enabled", "is_valid"],
    "priority": ["priority", "urgency", "importance", "severity", "criticality"],
    "category": ["category", "cat", "type", "classification", "segment", "group", "class", "kind"],

    # Text / Metadata
    "description": ["description", "desc", "details", "notes", "comments", "remarks", "descr", "comment", "note", "memo", "summary", "narrative"],
    "url": ["url", "link", "website", "web_address", "uri", "href", "web_url", "webpage", "homepage"],
    "ip_address": ["ip_address", "ip", "ipaddress", "client_ip", "source_ip", "remote_ip", "host_ip"],
    "user_agent": ["user_agent", "browser", "client_info", "device_info"],
    "version": ["version", "ver", "revision", "rev", "build", "release"],

    # Healthcare
    "diagnosis": ["diagnosis", "diag", "diag_code", "icd", "icd_code", "icd10", "icd9", "condition", "disease_code"],
    "procedure": ["procedure", "proc", "proc_code", "cpt", "cpt_code", "treatment", "surgery_code"],
    "medication": ["medication", "med", "drug", "drug_name", "prescription", "rx", "ndc", "ndc_code"],
    "provider": ["provider", "physician", "doctor", "dr", "npi", "npi_number", "prescriber", "attending_physician"],
    "insurance": ["insurance", "payer", "insurance_id", "member_id", "policy_number", "subscriber_id", "hic_number", "plan_id"],
    "allergy": ["allergy", "allergies", "allergen", "adverse_reaction"],
    "lab_result": ["lab_result", "lab_value", "test_result", "result_value", "loinc", "loinc_code", "observation"],

    # Retail / E-commerce
    "brand": ["brand", "brand_name", "manufacturer", "mfr", "vendor_name", "make"],
    "category_retail": ["product_category", "dept", "department", "merchandise_category", "product_type", "item_category"],
    "promotion": ["promotion", "promo", "promo_code", "coupon", "coupon_code", "discount_code", "campaign"],
    "channel": ["channel", "sales_channel", "order_channel", "source_channel", "marketplace"],

    # HR / Employee
    "department": ["department", "dept", "division", "business_unit", "bu", "cost_center", "org_unit"],
    "job_title": ["job_title", "title", "position", "role", "designation", "job_role"],
    "salary": ["salary", "compensation", "pay", "wage", "base_salary", "annual_salary", "hourly_rate", "pay_rate"],
    "hire_date": ["hire_date", "start_date", "employment_date", "join_date", "onboarding_date"],
    "termination_date": ["termination_date", "end_date", "exit_date", "separation_date", "last_day"],

    # Manufacturing
    "batch": ["batch", "batch_id", "batch_number", "lot", "lot_id", "lot_number", "production_batch"],
    "serial_number": ["serial_number", "serial_no", "serial", "sn", "equipment_serial"],
    "work_order": ["work_order", "wo", "wo_number", "production_order", "job_number"],
    "bom": ["bom", "bill_of_materials", "material_list", "component_list"],

    # Real Estate
    "property_id": ["property_id", "property", "parcel_id", "parcel_number", "apn", "listing_id", "mls_id"],
    "square_feet": ["square_feet", "sqft", "sq_ft", "area_sqft", "living_area", "lot_size"],
    "bedrooms": ["bedrooms", "beds", "bedroom_count", "num_bedrooms"],
    "bathrooms": ["bathrooms", "baths", "bathroom_count", "num_bathrooms"],

    # Education
    "student_id": ["student_id", "studentid", "pupil_id", "enrollment_id", "student_number"],
    "course_id": ["course_id", "course_code", "class_id", "section_id", "crn"],
    "grade": ["grade", "letter_grade", "final_grade", "gpa", "score_pct"],
    "enrollment_date": ["enrollment_date", "registration_date", "enroll_date", "admission_date"],
}

# Common abbreviation tokens used as column name parts
TOKEN_ABBREVIATIONS: dict[str, str] = {
    "amt": "amount", "amnt": "amount", "val": "amount", "valu": "value",
    "cust": "customer", "client": "customer", "buyer": "customer",
    "qty": "quantity", "quant": "quantity", "cnt": "count",
    "prod": "product", "item": "product", "sku": "product",
    "emp": "employee", "staff": "employee",
    "dept": "department", "div": "division",
    "addr": "address", "loc": "location",
    "desc": "description", "descr": "description",
    "num": "number", "no": "number", "nr": "number",
    "dt": "date", "tm": "time", "ts": "timestamp",
    "tel": "phone", "mob": "mobile", "cell": "mobile",
    "fname": "first_name", "lname": "last_name",
    "mname": "middle_name", "dob": "date_of_birth",
    "ssn": "social_security_number",
    "acct": "account", "acc": "account",
    "trans": "transaction", "txn": "transaction",
    "inv": "invoice", "ord": "order",
    "ship": "shipment", "dest": "destination",
    "orig": "origin", "src": "source", "tgt": "target",
    "pct": "percentage", "perc": "percentage",
    "avg": "average", "tot": "total", "sum": "total",
    "min": "minimum", "max": "maximum",
    "ref": "reference", "id": "identifier",
    "stat": "status", "sts": "status",
    "cat": "category", "cls": "class",
    "org": "organization", "co": "company",
    "mfr": "manufacturer", "vendor": "supplier",
    "wh": "warehouse", "dc": "distribution_center",
    "pkg": "package", "pkg_wt": "package_weight",
    "rev": "revenue", "cost": "price",
    "bal": "balance", "cr": "credit", "dr": "debit",
    "tax": "tax_amount", "disc": "discount",
    "curr": "currency", "ccy": "currency",
    "lat": "latitude", "lon": "longitude", "lng": "longitude",
    "geo": "geographic", "zip": "postal_code",
    "st": "state", "ctry": "country",
    "yr": "year", "mo": "month", "wk": "week",
    "hr": "hour", "min_dur": "minute", "sec": "second",
    "mgr": "manager", "supv": "supervisor",
    "diag": "diagnosis", "proc": "procedure", "med": "medication",
    "ins": "insurance", "pol": "policy",
    "mrn": "medical_record_number",
    "npi": "provider_npi",
}


# Reverse lookup: any synonym -> canonical form
CANONICAL_FORMS: dict[str, str] = {}

# Also index token abbreviations
for abbr, full in TOKEN_ABBREVIATIONS.items():
    CANONICAL_FORMS[abbr] = full


for canonical, synonyms in SYNONYM_DICTIONARY.items():
    CANONICAL_FORMS[canonical] = canonical
    for syn in synonyms:
        CANONICAL_FORMS[syn.lower()] = canonical


def resolve_canonical(name: str) -> str:
    """Resolve a column name to its canonical form."""
    normalized = name.lower().strip().replace("-", "_").replace(" ", "_")
    if normalized in CANONICAL_FORMS:
        return CANONICAL_FORMS[normalized]
    # Try token-by-token resolution
    tokens = normalized.split("_")
    resolved_tokens = [CANONICAL_FORMS.get(t, t) for t in tokens]
    return "_".join(resolved_tokens)


def expand_synonyms(canonical: str) -> list[str]:
    """Get all known synonyms for a canonical form."""
    return SYNONYM_DICTIONARY.get(canonical, [canonical])


def _expand_tokens(name: str) -> set[str]:
    """Expand a name into all token variants including abbreviations."""
    normalized = name.lower().replace("-", "_").replace(" ", "_")
    tokens = set(normalized.split("_"))
    expanded = set(tokens)
    for token in list(tokens):
        if token in TOKEN_ABBREVIATIONS:
            expanded.add(TOKEN_ABBREVIATIONS[token])
        for abbr, full in TOKEN_ABBREVIATIONS.items():
            if full == token or full.replace("_", "") == token:
                expanded.add(abbr)
    return expanded


def are_synonyms(name1: str, name2: str) -> bool:
    """Check if two column names are synonyms."""
    c1 = resolve_canonical(name1)
    c2 = resolve_canonical(name2)
    if c1 == c2:
        return True

    norm1 = name1.lower().replace("-", "_").replace(" ", "_")
    norm2 = name2.lower().replace("-", "_").replace(" ", "_")

    # Direct match in synonym groups
    for canonical, syns in SYNONYM_DICTIONARY.items():
        group = {s.lower() for s in syns} | {canonical}
        if norm1 in group and norm2 in group:
            return True

    # Token-level abbreviation matching
    tokens1 = _expand_tokens(name1)
    tokens2 = _expand_tokens(name2)
    if tokens1 & tokens2:
        return True

    # Check if one name is an abbreviation of the other
    if norm1 in TOKEN_ABBREVIATIONS and TOKEN_ABBREVIATIONS[norm1] == norm2:
        return True
    if norm2 in TOKEN_ABBREVIATIONS and TOKEN_ABBREVIATIONS[norm2] == norm1:
        return True
    if norm1 in TOKEN_ABBREVIATIONS and TOKEN_ABBREVIATIONS[norm1].replace("_", "") == norm2.replace("_", ""):
        return True
    if norm2 in TOKEN_ABBREVIATIONS and TOKEN_ABBREVIATIONS[norm2].replace("_", "") == norm1.replace("_", ""):
        return True

    return False


def get_synonym_count() -> int:
    """Total number of synonym entries."""
    return sum(len(v) for v in SYNONYM_DICTIONARY.values()) + len(TOKEN_ABBREVIATIONS)
