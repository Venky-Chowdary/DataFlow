"""Hybrid semantic column mapper — BM25 lexical retrieval + semantic token graph."""

from __future__ import annotations

import logging
import math
import pickle  # nosec B403
import re
import sys
from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path

_model_cache = None


def ml_baseline_status() -> dict:
    """Operator-facing status for Map UI / Pilot — never silent about ML availability."""
    model = _load_ml_baseline()
    path = Path(__file__).resolve().parents[3] / "packages" / "ml" / "models" / "baseline.pkl"
    return {
        "available": model is not None,
        "path": str(path),
        "path_exists": path.exists(),
        "role": "optional_boost",
        "note": (
            "ML baseline available — high-confidence predictions can boost matches."
            if model is not None
            else "ML baseline unavailable — automapping uses lexical + semantic + Hungarian only."
        ),
    }


def _load_ml_baseline():
    global _model_cache
    if _model_cache is not None:
        return _model_cache if _model_cache is not False else None

    # Try to load the ML baseline model if it exists
    try:
        model_path = Path(__file__).resolve().parents[3] / "packages" / "ml" / "models" / "baseline.pkl"
        if model_path.exists():
            # Adjust path so that baseline class can be loaded
            pkg_path = str(Path(__file__).resolve().parents[3] / "packages")
            if pkg_path not in sys.path:
                sys.path.append(pkg_path)
            # Ensure the picklable class is importable under a stable module path.
            try:
                import ml.baseline  # noqa: F401
            except Exception:
                pass
            with model_path.open("rb") as f:
                _model_cache = pickle.load(f)  # nosec B301
                return _model_cache
    except Exception as exc:
        # Cache negative result so a broken pickle does not spam every map_columns call.
        _model_cache = False
        logging.getLogger(__name__).warning(
            "ML baseline unavailable (%s); using lexical/semantic mapper only",
            type(exc).__name__,
        )
    return None


def _calibrated_confidence(
    score: float,
    *,
    score_gap: float,
    requires_review: bool,
    hard_cap: float = 0.99,
) -> float:
    """Keep near-tie mappings below auto-approve thresholds.

    G4 / Studio auto-approve uses ~0.85 in strict mode. A near-tie with
    confidence 0.93 would pass the gate despite ``requires_review``. Cap
    review rows at 0.84 and scale by gap so operators must confirm.
    """
    conf = min(float(score), hard_cap)
    if requires_review:
        # gap 0.00 → 0.70, gap 0.07 → ~0.805, never above 0.84
        conf = min(conf, round(0.70 + max(score_gap, 0.0) * 1.5, 3), 0.84)
    return round(conf, 3)


ABBREVIATIONS: dict[str, str] = {
    # Amounts and quantities
    "amt": "amount",
    "amount": "amount",
    "salary": "salary_amount",
    "salary_amt": "salary_amount",
    "salary_amount": "salary_amount",
    "pay": "payment",
    "pmt": "payment",
    "pymt": "payment",
    "pay_amt": "payment_amount",
    "payment_amount": "payment_amount",
    "tax": "tax",
    "tax_amt": "tax_amount",
    "tax_amount": "tax_amount",
    "net": "net",
    "net_amt": "net_amount",
    "net_amount": "net_amount",
    "gross": "gross",
    "gross_amt": "gross_amount",
    "gross_amount": "gross_amount",
    "line": "line",
    "line_amt": "line_amount",
    "line_amount": "line_amount",
    "bal": "balance",
    "balance": "balance",
    "tot": "total",
    "total": "total",
    "subtot": "subtotal",
    "subtotal": "subtotal",
    "disc": "discount",
    "discount": "discount",
    "qty": "quantity",
    "quantity": "quantity",
    "qty_ord": "quantity_ordered",
    "quantity_ordered": "quantity_ordered",
    "price": "price",
    "prc": "price",
    "unit_prc": "unit_price",
    "unit_price": "unit_price",
    "cost": "cost",
    "unit_cost": "unit_cost",
    # Dates and timestamps
    "dt": "date",
    "date": "date",
    "ts": "timestamp",
    "timestamp": "timestamp",
    "created": "created",
    "created_at": "created_at",
    "created_dt": "created_at",
    "created_date": "created_at",
    "created_ts": "created_timestamp",
    "created_timestamp": "created_timestamp",
    "updated": "updated",
    "updated_at": "updated_at",
    "updated_dt": "updated_at",
    "updated_date": "updated_at",
    "updated_ts": "updated_timestamp",
    "updated_timestamp": "updated_timestamp",
    "mod": "modified",
    "modified": "modified",
    "modified_at": "modified_at",
    "mod_at": "modified_at",
    "mod_dt": "modified_at",
    "modified_dt": "modified_at",
    "modified_date": "modified_at",
    "modified_ts": "modified_timestamp",
    "modified_timestamp": "modified_timestamp",
    "txn": "transaction",
    "transaction": "transaction",
    "txn_dt": "transaction_date",
    "transaction_date": "transaction_date",
    "txn_id": "transaction_id",
    "transaction_id": "transaction_id",
    "trans_dt": "transaction_date",
    "trans_date": "transaction_date",
    "hire_dt": "hire_date",
    "hire_date": "hire_date",
    "ship_dt": "ship_date",
    "ship_date": "ship_date",
    "del": "delivery",
    "delivery": "delivery",
    "del_dt": "delivery_date",
    "delivery_date": "delivery_date",
    "pay_dt": "payment_date",
    "payment_dt": "payment_date",
    "payment_date": "payment_date",
    # Identifiers and customers
    "no": "number",
    "num": "number",
    "nbr": "number",
    "nr": "number",
    "number": "number",
    "ref": "reference",
    "reference": "reference",
    "ref_no": "reference_number",
    "reference_number": "reference_number",
    "inv": "invoice",
    "invoice": "invoice",
    "inv_no": "invoice_number",
    "invoice_number": "invoice_number",
    "ord": "order",
    "order": "order",
    "ord_id": "order_id",
    "order_id": "order_id",
    "order_no": "order_number",
    "order_number": "order_number",
    "cust": "customer",
    "customer": "customer",
    "cust_id": "customer_id",
    "customer_id": "customer_id",
    "cust_nm": "customer_name",
    "customer_name": "customer_name",
    "acct": "account",
    "account": "account",
    "acct_no": "account_number",
    "acct_num": "account_number",
    "account_number": "account_number",
    "emp": "employee",
    "employee": "employee",
    "emp_id": "employee_id",
    "employee_id": "employee_id",
    "dept": "department",
    "department": "department",
    "dept_code": "department_code",
    "dept_cd": "department_code",
    "department_code": "department_code",
    "ssn": "social_security_number",
    "social_security_number": "social_security_number",
    "ssn_num": "social_security_number",
    "cc_num": "credit_card_number",
    "cc_no": "credit_card_number",
    "credit_card_number": "credit_card_number",
    "card_num": "credit_card_number",
    "gstin": "gst_number",
    "gst_num": "gst_number",
    "gst_number": "gst_number",
    "pan_num": "pan_number",
    "pan_number": "pan_number",
    "iban": "iban",
    "swift": "swift_code",
    "swift_code": "swift_code",
    # Warehouse / lake / object-store / CRM SaaS (demo-critical cloud routes)
    "acct_id": "account_id",
    "account_id": "account_id",
    "org": "organization",
    "org_id": "organization_id",
    "organization_id": "organization_id",
    "opp": "opportunity",
    "opp_amt": "opportunity_amount",
    "opportunity_amount": "opportunity_amount",
    "lead_src": "lead_source",
    "lead_source": "lead_source",
    "close_dt": "close_date",
    "close_date": "close_date",
    "stage_nm": "stage_name",
    "stage_name": "stage_name",
    "rec_type": "record_type",
    "record_type": "record_type",
    "ext_id": "external_id",
    "external_id": "external_id",
    # Prefer compound abbreviations — bare wh/db/pk/fk/arr collide with
    # unrelated columns (array payloads, key values, catalog shorthand).
    "wh_id": "warehouse_id",
    "warehouse_id": "warehouse_id",
    "db_nm": "database_name",
    "database_name": "database_name",
    "sch_nm": "schema_name",
    "schema_name": "schema_name",
    "tbl_nm": "table_name",
    "table_name": "table_name",
    "col_nm": "column_name",
    "column_name": "column_name",
    "pk_col": "primary_key_column",
    "primary_key_column": "primary_key_column",
    "fk_col": "foreign_key_column",
    "foreign_key_column": "foreign_key_column",
    "row_cnt": "row_count",
    "row_count": "row_count",
    "byte_sz": "byte_size",
    "byte_size": "byte_size",
    "obj_key": "object_key",
    "object_key": "object_key",
    "bkt_nm": "bucket_name",
    "bucket_name": "bucket_name",
    "cont_nm": "container_name",
    "container_name": "container_name",
    "last_mod": "last_modified",
    "last_modified": "last_modified",
    "mrr": "monthly_recurring_revenue",
    "monthly_recurring_revenue": "monthly_recurring_revenue",
    "ann_rev": "annual_recurring_revenue",
    "annual_recurring_revenue": "annual_recurring_revenue",
    "tz_nm": "timezone_name",
    "timezone_name": "timezone_name",
    "churn_dt": "churn_date",
    "churn_date": "churn_date",
    "utm_src": "utm_source",
    "utm_source": "utm_source",
    "utm_med": "utm_medium",
    "utm_medium": "utm_medium",
    "utm_camp": "utm_campaign",
    "utm_campaign": "utm_campaign",
    "ip_addr": "ip_address",
    "ip_address": "ip_address",
    "mac_addr": "mac_address",
    "mac_address": "mac_address",
    "geo_cd": "geo_code",
    "geo_code": "geo_code",
    "reg_cd": "region_code",
    "region_code": "region_code",
    "lang_cd": "language_code",
    "language_code": "language_code",
    "locale_cd": "locale_code",
    "locale_code": "locale_code",
    "vat_num": "vat_number",
    "vat_number": "vat_number",
    "tin": "tax_identification_number",
    "tax_identification_number": "tax_identification_number",
    "ein": "employer_identification_number",
    "employer_identification_number": "employer_identification_number",
    "routing_num": "routing_number",
    "routing_number": "routing_number",
    "chk_num": "check_number",
    "check_number": "check_number",
    "wire_ref": "wire_reference",
    "wire_reference": "wire_reference",
    "fx_rate": "exchange_rate",
    "exchange_rate": "exchange_rate",
    "base_ccy": "base_currency",
    "base_currency": "base_currency",
    "quote_ccy": "quote_currency",
    "quote_currency": "quote_currency",
    "product": "product",
    "prod": "product",
    "prod_id": "product_id",
    "product_id": "product_id",
    "sku": "product_sku",
    "product_sku": "product_sku",
    "src": "source",
    "source": "source",
    "tgt": "target",
    "target": "target",
    "loc": "location",
    "location": "location",
    # Names and contact
    "nm": "name",
    "name": "name",
    "fname": "first_name",
    "first_name": "first_name",
    "lname": "last_name",
    "last_name": "last_name",
    "full_name": "full_name",
    "desc": "description",
    "descr": "description",
    "description": "description",
    "addr": "address",
    "address": "address",
    "addr1": "address_line_1",
    "addr_1": "address_line_1",
    "address_line_1": "address_line_1",
    "address1": "address_line_1",
    "addr2": "address_line_2",
    "addr_2": "address_line_2",
    "address_line_2": "address_line_2",
    "address2": "address_line_2",
    "city_nm": "city_name",
    "state_cd": "state_code",
    "zip_cd": "postal_code",
    "lat": "latitude",
    "latitude": "latitude",
    "lng": "longitude",
    "lon": "longitude",
    "longitude": "longitude",
    "is_actv": "is_active",
    "is_active": "is_active",
    "amt_usd": "amount_usd",
    "amount_usd": "amount_usd",
    "disc_pct": "discount_percent",
    "discount_percent": "discount_percent",
    "discount_pct": "discount_percent",
    "line_tot": "line_total",
    "line_total": "line_total",
    "cust_seg": "customer_segment",
    "customer_segment": "customer_segment",
    "po_num": "purchase_order_number",
    "po_no": "purchase_order_number",
    "purchase_order_number": "purchase_order_number",
    "inv_num": "invoice_number",
    "email": "email",
    "e_mail": "email",
    "email_addr": "email_address",
    "email_address": "email_address",
    "usr_email": "user_email",
    "user_email": "user_email",
    "usr": "user",
    "user": "user",
    "ship": "shipping",
    "ship_addr": "shipping_address",
    "ship_address": "shipping_address",
    "shipping_addr": "shipping_address",
    "shipping_address": "shipping_address",
    "bill_addr": "billing_address",
    "billing_addr": "billing_address",
    "billing_address": "billing_address",
    "mail_addr": "mailing_address",
    "dob": "date_of_birth",
    "date_of_birth": "date_of_birth",
    "birth_date": "date_of_birth",
    "birthdate": "date_of_birth",
    "d_o_b": "date_of_birth",
    "phone": "phone",
    "tel": "phone",
    "ph": "phone",
    "ph_num": "phone",
    "ph_no": "phone",
    "ph_nbr": "phone",
    "tel_num": "phone_number",
    "tel_no": "phone_number",
    "phone_number": "phone_number",
    "phone_num": "phone_number",
    "mobile": "mobile",
    "mob": "mobile",
    "cell": "mobile",
    "mobile_phone": "mobile_phone",
    "mobile_number": "mobile_number",
    "mobile_num": "mobile_number",
    "mob_num": "mobile_number",
    "mob_phone": "mobile_phone",
    "cell_phone": "mobile_phone",
    "cell_num": "mobile_number",
    # Status and location
    "sts": "status",
    "stat": "status",
    "status": "status",
    "zip": "postal_code",
    "zipcode": "postal_code",
    "postal": "postal_code",
    "postal_code": "postal_code",
    "country": "country",
    "country_cd": "country_code",
    "country_code": "country_code",
    "cntry": "country",
    "state": "state",
    "state_code": "state_code",
    "province": "province",
    "province_code": "province_code",
    "city": "city",
    "city_name": "city_name",
    "region": "region",
    "region_code": "region_code",
    "curr": "currency",
    "currency": "currency",
    "ccy": "currency_code",
    "curr_cd": "currency_code",
    "iso_curr": "currency_code",
    "currency_code": "currency_code",
    # --- Enterprise domain phrases (finance / healthcare / HR / logistics) ---
    "status_cd": "order_status",
    "order_status": "order_status",
    "fx_rate": "exchange_rate",
    "exchange_rate": "exchange_rate",
    "value_dt": "value_date",
    "value_date": "value_date",
    "stmt_dt": "statement_date",
    "statement_date": "statement_date",
    "statement_dt": "statement_date",
    "mrn": "medical_record_number",
    "medical_record_number": "medical_record_number",
    "admit_dt": "admission_date",
    "admission_date": "admission_date",
    "admission_dt": "admission_date",
    "disch_dt": "discharge_date",
    "discharge_date": "discharge_date",
    "vitals_hr": "heart_rate",
    "heart_rate": "heart_rate",
    "vitals_bp_sys": "bp_systolic",
    "bp_systolic": "bp_systolic",
    "vitals_bp_dia": "bp_diastolic",
    "bp_diastolic": "bp_diastolic",
    "rx_norm": "rxnorm_code",
    "rxnorm": "rxnorm_code",
    "rxnorm_code": "rxnorm_code",
    "pcp_id": "primary_care_provider_id",
    "primary_care_provider_id": "primary_care_provider_id",
    "mgr_id": "manager_id",
    "manager_id": "manager_id",
    "emp_status": "employment_status",
    "employment_status": "employment_status",
    "emp_type": "employment_type",
    "employment_type": "employment_type",
    "ship_id": "shipment_id",
    "shipment_id": "shipment_id",
    "eta_dt": "estimated_arrival_date",
    "eta": "estimated_arrival_date",
    "estimated_arrival_date": "estimated_arrival_date",
    "dim_l": "dimension_length",
    "dim_w": "dimension_width",
    "dim_h": "dimension_height",
    "dimension_length": "dimension_length",
    "dimension_width": "dimension_width",
    "dimension_height": "dimension_height",
    "pkg_cnt": "package_count",
    "package_count": "package_count",
    "bol_no": "bill_of_lading_number",
    "bol_num": "bill_of_lading_number",
    "bill_of_lading_number": "bill_of_lading_number",
    "po_no": "purchase_order_number",
    "po_num": "purchase_order_number",
    "purchase_order_number": "purchase_order_number",
    "pat_id": "patient_id",
    "patient_id": "patient_id",
    "pat_dob": "date_of_birth",
    "enc_id": "encounter_id",
    "encounter_id": "encounter_id",
    "diag_cd": "diagnosis_code",
    "diagnosis_code": "diagnosis_code",
    "proc_cd": "procedure_code",
    "procedure_code": "procedure_code",
    "prov_id": "provider_id",
    "provider_id": "provider_id",
    "npi": "npi_number",
    "npi_number": "npi_number",
    "ins_id": "insurance_id",
    "insurance_id": "insurance_id",
    "policy_no": "policy_number",
    "policy_number": "policy_number",
    "claim_amt": "claim_amount",
    "claim_amount": "claim_amount",
    "allowed_amt": "allowed_amount",
    "paid_amt": "paid_amount",
    "copay_amt": "copay_amount",
    "deduct_amt": "deductible_amount",
    "deductible_amount": "deductible_amount",
    "icd10": "icd10_code",
    "icd10_code": "icd10_code",
    "cpt_cd": "cpt_code",
    "cpt_code": "cpt_code",
    "loinc": "loinc_code",
    "loinc_code": "loinc_code",
    "lab_result": "lab_result_value",
    "lab_result_value": "lab_result_value",
    "lab_unit": "lab_result_unit",
    "lab_result_unit": "lab_result_unit",
    "allergy_cd": "allergy_code",
    "allergy_code": "allergy_code",
    "med_name": "medication_name",
    "medication_name": "medication_name",
    "rx_norm_code": "rxnorm_code",
    "emerg_contact": "emergency_contact_name",
    "emergency_contact_name": "emergency_contact_name",
    "emerg_phone": "emergency_contact_phone",
    "emergency_contact_phone": "emergency_contact_phone",
    "hipaa_flg": "hipaa_authorized",
    "hipaa_authorized": "hipaa_authorized",
    "consent_flg": "consent_flag",
    "consent_flag": "consent_flag",
    "emp_no": "employee_number",
    "employee_number": "employee_number",
    "dept_cd": "department_code",
    "dept_nm": "department_name",
    "department_name": "department_name",
    "bonus_amt": "bonus_amount",
    "comm_amt": "commission_amount",
    "commission_amount": "commission_amount",
    "work_email": "work_email",
    "work_phone": "work_phone",
    "loc_cd": "location_code",
    "location_code": "location_code",
    "fte": "fte_ratio",
    "fte_ratio": "fte_ratio",
    "bank_acct": "bank_account_number",
    "bank_account_number": "bank_account_number",
    "direct_dep_flg": "direct_deposit_flag",
    "direct_deposit_flag": "direct_deposit_flag",
    "pto_bal": "pto_balance",
    "pto_balance": "pto_balance",
    "sick_bal": "sick_balance",
    "sick_balance": "sick_balance",
    "perf_score": "performance_score",
    "performance_score": "performance_score",
    "last_review_dt": "last_review_date",
    "last_review_date": "last_review_date",
    "badge_no": "badge_number",
    "badge_number": "badge_number",
    "shift_cd": "shift_code",
    "shift_code": "shift_code",
    "union_flg": "union_member_flag",
    "union_member_flag": "union_member_flag",
    "remote_flg": "remote_worker_flag",
    "remote_worker_flag": "remote_worker_flag",
    "start_tm": "shift_start_time",
    "shift_start_time": "shift_start_time",
    "end_tm": "shift_end_time",
    "shift_end_time": "shift_end_time",
    "overtime_hrs": "overtime_hours",
    "overtime_hours": "overtime_hours",
    "regular_hrs": "regular_hours",
    "regular_hours": "regular_hours",
    "pay_period_end": "pay_period_end_date",
    "pay_period_end_date": "pay_period_end_date",
    "benefits_elig": "benefits_eligible",
    "benefits_eligible": "benefits_eligible",
    "rehire_flg": "rehire_eligible_flag",
    "rehire_eligible_flag": "rehire_eligible_flag",
    "tracking_no": "tracking_number",
    "tracking_number": "tracking_number",
    "carrier_cd": "carrier_code",
    "carrier_code": "carrier_code",
    "origin_zip": "origin_postal_code",
    "origin_postal_code": "origin_postal_code",
    "dest_zip": "destination_postal_code",
    "destination_postal_code": "destination_postal_code",
    "freight_amt": "freight_amount",
    "freight_amount": "freight_amount",
    "fuel_surcharge": "fuel_surcharge_amount",
    "fuel_surcharge_amount": "fuel_surcharge_amount",
    "bin_loc": "bin_location",
    "bin_location": "bin_location",
    "lot_no": "lot_number",
    "lot_number": "lot_number",
    "serial_no": "serial_number",
    "serial_number": "serial_number",
    "qty_shipped": "quantity_shipped",
    "quantity_shipped": "quantity_shipped",
    "qty_ordered": "quantity_ordered",
    "qty_received": "quantity_received",
    "quantity_received": "quantity_received",
    "asn_id": "asn_id",
    "trailer_no": "trailer_number",
    "trailer_number": "trailer_number",
    "seal_no": "seal_number",
    "seal_number": "seal_number",
    "hazmat_flg": "hazmat_flag",
    "hazmat_flag": "hazmat_flag",
    "temp_min": "temperature_min",
    "temperature_min": "temperature_min",
    "temp_max": "temperature_max",
    "temperature_max": "temperature_max",
    "stop_seq": "stop_sequence",
    "stop_sequence": "stop_sequence",
    "miles": "distance_miles",
    "distance_miles": "distance_miles",
    "delay_mins": "delay_minutes",
    "delay_minutes": "delay_minutes",
    "proof_del_flg": "proof_of_delivery_flag",
    "proof_of_delivery_flag": "proof_of_delivery_flag",
    "princ_amt": "principal_amount",
    "principal_amount": "principal_amount",
    "fee_amt": "fee_amount",
    "fee_amount": "fee_amount",
    "gl_cd": "gl_code",
    "gl_code": "gl_code",
    "cost_ctr": "cost_center",
    "cost_center": "cost_center",
    "debit_amt": "debit_amount",
    "debit_amount": "debit_amount",
    "credit_amt": "credit_amount",
    "credit_amount": "credit_amount",
    "posting_dt": "posting_date",
    "posting_date": "posting_date",
    "settlement_dt": "settlement_date",
    "settlement_date": "settlement_date",
    "bic": "bic_code",
    "bic_code": "bic_code",
    "routing_no": "routing_number",
    "card_no": "card_number",
    "card_number": "card_number",
    "auth_cd": "auth_code",
    "auth_code": "auth_code",
    "ledger_bal": "ledger_balance",
    "ledger_balance": "ledger_balance",
    "avail_bal": "available_balance",
    "available_balance": "available_balance",
    "overdraft_lim": "overdraft_limit",
    "overdraft_limit": "overdraft_limit",
    "recon_flg": "reconciled_flag",
    "reconciled_flag": "reconciled_flag",
    "fiscal_yr": "fiscal_year",
    "fiscal_year": "fiscal_year",
    "fiscal_qtr": "fiscal_quarter",
    "fiscal_quarter": "fiscal_quarter",
    "aml_flg": "aml_flag",
    "aml_flag": "aml_flag",
    "wire_ref": "wire_reference",
    "wire_reference": "wire_reference",
    "ach_trace": "ach_trace_number",
    "ach_trace_number": "ach_trace_number",
    "vat_amt": "vat_amount",
    "vat_amount": "vat_amount",
    "invoice_amt": "invoice_amount",
    "invoice_amount": "invoice_amount",
    "int_rate": "interest_rate",
    "disc_amt": "discount_amount",
    "discount_amount": "discount_amount",
    "promo_cd": "promo_code",
    "promo_code": "promo_code",
    "refund_amt": "refund_amount",
    "refund_amount": "refund_amount",
    "ship_method": "shipping_method",
    "shipping_method": "shipping_method",
    "gift_msg": "gift_message",
    "gift_message": "gift_message",
    "item_cnt": "item_count",
    "item_count": "item_count",
    "channel": "sales_channel",
    "sales_channel": "sales_channel",
    "gender_cd": "gender",
    "ward_cd": "ward_code",
    "ward_code": "ward_code",
    "bed_no": "bed_number",
    "bed_number": "bed_number",
    # Evidence-grown from enterprise golden unresolved tokens (abbrev coverage CI).
    "cd": "code",
    "flg": "flag",
    "flag": "flag",
    "cnt": "count",
    "count": "count",
    "ctr": "center",
    "msg": "message",
    "message": "message",
    "lim": "limit",
    "limit": "limit",
    "yr": "year",
    "year": "year",
    "qtr": "quarter",
    "quarter": "quarter",
    "hrs": "hours",
    "hours": "hours",
    "tm": "time",
    "kg": "kilogram",
    "kilogram": "kilogram",
    "lb": "pound",
    "pound": "pound",
    "cvv": "card_verification_value",
    "card_verification_value": "card_verification_value",
    "bp": "blood_pressure",
    "blood_pressure": "blood_pressure",
    "vitals_hr": "vitals_heart_rate",
    "vitals_heart_rate": "vitals_heart_rate",
    "bill": "billing",
    "billing": "billing",
}


def _normalize(name: str) -> str:
    s = name.strip()
    s = re.sub(r"([a-z])([A-Z])", r"\1_\2", s)
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).rstrip("_")


def _expand_abbrev(token: str) -> str:
    return ABBREVIATIONS.get(token, token)


def _semantic_tokens(name: str) -> list[str]:
    norm = _normalize(name)
    parts = [p for p in norm.split("_") if p]
    tokens: list[str] = []
    i = 0
    # Match longest abbreviation phrase first so multi-token abbreviations like
    # "txn_dt" or "created_at" resolve to their canonical form.
    while i < len(parts):
        matched = False
        for j in range(len(parts), i, -1):
            phrase = "_".join(parts[i:j])
            if phrase in ABBREVIATIONS:
                expansion = ABBREVIATIONS[phrase]
                exp_parts = [p for p in expansion.split("_") if p]
                tokens.extend(exp_parts)
                i = j
                # Skip trailing parts already covered by the expansion
                # (email → email_address then addr → address must not double).
                already = set(tokens)
                while i < len(parts):
                    nxt = _expand_abbrev(parts[i])
                    nxt_parts = [p for p in nxt.split("_") if p]
                    if nxt_parts and all(p in already for p in nxt_parts):
                        i += 1
                        already.update(nxt_parts)
                        continue
                    break
                matched = True
                break
        if not matched:
            expansion = _expand_abbrev(parts[i])
            tokens.extend([p for p in expansion.split("_") if p])
            i += 1
    # Collapse adjacent duplicates from flattened multi-token expansions.
    deduped: list[str] = []
    for t in tokens:
        if not deduped or deduped[-1] != t:
            deduped.append(t)
    return deduped


def _semantic_form(name: str) -> str:
    return "_".join(_semantic_tokens(name))


def _canonical_form(name: str) -> str:
    """Resolve enterprise schematic variant → canonical semantic form."""
    try:
        from services.schematic_index import lookup_schematic

        canon = lookup_schematic(name)
        if canon:
            return canon
    except ImportError:
        pass
    return _semantic_form(name)


def _tokenize(name: str) -> list[str]:
    """Abbreviation-expanded tokens for BM25 / IDF — not schematic-collapsed."""
    return [t for t in _semantic_form(name).split("_") if t]


def _build_idf(corpus: list[str]) -> dict[str, float]:
    n = len(corpus)
    df: Counter[str] = Counter()
    for doc in corpus:
        for tok in set(_tokenize(doc)):
            df[tok] += 1
    return {tok: math.log((n + 1) / (freq + 1)) + 1.0 for tok, freq in df.items()}


def _bm25_score(query_tokens: list[str], doc_tokens: list[str], idf: dict[str, float], avgdl: float, k1: float = 1.5, b: float = 0.75) -> float:
    if not query_tokens or not doc_tokens:
        return 0.0
    doc_len = len(doc_tokens)
    avgdl = max(avgdl, 1.0)
    tf = Counter(doc_tokens)
    score = 0.0
    for qt in query_tokens:
        if qt not in tf:
            continue
        freq = tf[qt]
        idf_val = idf.get(qt, 1.0)
        denom = freq + k1 * (1 - b + b * doc_len / avgdl)
        score += idf_val * (freq * (k1 + 1)) / denom
    return score


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


# Leaf tokens too generic to prove columns are the same when qualifiers conflict.
_ENTITY_STOPWORDS = frozenset({
    "at", "of", "the", "a", "an", "to", "for", "by", "and", "or", "on", "in",
})
_DOMAIN_LEAVES = frozenset({
    "amount", "id", "name", "date", "code", "status", "type", "number",
    "value", "count", "flag", "key", "timestamp", "balance", "price",
    "quantity", "total", "rate", "pct", "percent", "description", "text",
    "address", "email", "phone", "time", "uuid", "hash", "index", "seq",
})
_GENERIC_LEAVES = _DOMAIN_LEAVES | _ENTITY_STOPWORDS


def _qualifier_tokens(name: str) -> set[str]:
    return {t for t in _semantic_form(name).split("_") if t} - _DOMAIN_LEAVES - _ENTITY_STOPWORDS


# Lightweight English stem rules for entity qualifiers (paid≈payment, create≈creation).
# Not a full Porter stemmer — just high-frequency migration stems.
_STEM_SUFFIXES: tuple[tuple[str, str], ...] = (
    ("izations", ""),
    ("isation", ""),
    ("ations", ""),
    ("ation", ""),
    ("ments", ""),
    ("ment", ""),
    ("ings", ""),
    ("ing", ""),
    ("tions", ""),
    ("tion", ""),
    ("sion", ""),
    ("ness", ""),
    ("ities", "ity"),
    ("ity", ""),
    ("iers", "y"),
    ("ies", "y"),
    ("ied", "y"),
    ("ously", ""),
    ("ally", ""),
    ("ers", ""),
    ("ors", ""),
    ("er", ""),
    ("or", ""),
    ("als", "al"),
    ("al", ""),
    ("eds", ""),
    ("ed", ""),
    ("es", ""),
    ("s", ""),
)

_STEM_IRREGULAR: dict[str, str] = {
    "paid": "pay",
    "payment": "pay",
    "payments": "pay",
    "bought": "buy",
    "purchase": "buy",
    "purchased": "buy",
    "created": "create",
    "creation": "create",
    "creator": "create",
    "updated": "update",
    "updation": "update",  # common misspelling in schemas
    "shipped": "ship",
    "shipping": "ship",
    "shipment": "ship",
    "customer": "cust",
    "customers": "cust",
    "transaction": "txn",
    "transactions": "txn",
    "amount": "amt",
    "amounts": "amt",
}


def _light_stem(token: str) -> str:
    t = (token or "").strip().lower()
    if not t:
        return ""
    if t in _STEM_IRREGULAR:
        return _STEM_IRREGULAR[t]
    for suf, repl in _STEM_SUFFIXES:
        if len(t) > len(suf) + 2 and t.endswith(suf):
            return t[: -len(suf)] + repl
    return t


def _qualifier_stems_overlap(a: set[str], b: set[str]) -> bool:
    """True when qualifiers share a stem (ship ≈ shipping, paid ≈ payment)."""
    stems_a = {_light_stem(x) for x in a} | set(a)
    stems_b = {_light_stem(y) for y in b} | set(b)
    if stems_a & stems_b:
        return True
    for x in a:
        for y in b:
            if x == y:
                return True
            if len(x) >= 3 and len(y) >= 3 and (x.startswith(y) or y.startswith(x)):
                return True
            sx, sy = _light_stem(x), _light_stem(y)
            if sx and sy and (sx == sy or (len(sx) >= 3 and len(sy) >= 3 and (sx.startswith(sy) or sy.startswith(sx)))):
                return True
    return False


def _qualifiers_compatible(source: str, target: str) -> bool:
    """False when both sides carry conflicting entity prefixes."""
    src_q = _qualifier_tokens(source)
    tgt_q = _qualifier_tokens(target)
    if not src_q or not tgt_q:
        return True
    if not src_q.isdisjoint(tgt_q):
        return True
    return _qualifier_stems_overlap(src_q, tgt_q)


def _entity_agreement(source: str, target: str) -> float:
    """1.0 shared entity, 0.5 asymmetric/generic, 0.0 hard conflict."""
    src_q = _qualifier_tokens(source)
    tgt_q = _qualifier_tokens(target)
    if src_q and tgt_q:
        if not src_q.isdisjoint(tgt_q):
            return len(src_q & tgt_q) / len(src_q | tgt_q)
        if _qualifier_stems_overlap(src_q, tgt_q):
            return 0.75
        return 0.0
    if not src_q and not tgt_q:
        return 0.55
    return 0.35


def _is_bare_domain_leaf(name: str) -> bool:
    toks = {t for t in _semantic_form(name).split("_") if t} - _ENTITY_STOPWORDS
    return len(toks) == 1 and toks <= _DOMAIN_LEAVES


def _near_target_by_form(
    source: str,
    target_columns: list[str],
    *,
    used_targets: set[str] | None = None,
) -> tuple[str, float]:
    """Best unused destination by abbreviation-expanded form similarity."""
    used = {t.lower() for t in (used_targets or set())}
    src_form = _semantic_form(source)
    best_tgt = ""
    best = 0.0
    for tgt in target_columns:
        if tgt.lower() in used:
            continue
        if not _qualifiers_compatible(source, tgt):
            continue
        ratio = _similarity(src_form, _semantic_form(tgt))
        agreement = _entity_agreement(source, tgt)
        ratio = ratio * (0.55 + 0.45 * max(agreement, 0.15))
        # Containment bonus: phone ⊂ phone_number when entities agree
        tgt_form = _semantic_form(tgt)
        if agreement > 0 and src_form and tgt_form and (src_form in tgt_form or tgt_form in src_form):
            ratio = max(ratio, 0.72 * (0.6 + 0.4 * agreement))
        if ratio > best:
            best, best_tgt = ratio, tgt
    return best_tgt, best


def _identity_onto_numeric_landmine(source: str, src_type: str, tgt_type: str) -> bool:
    """True when Mongo/document identity would land on a numeric warehouse PK.

    Without samples the old mapper bound ``_id``→NUMBER ``id`` (~0.73). Hex
    ObjectIds never fit INTEGER/NUMBER — refuse that Map landmine up front.
    """
    from services.type_system import normalize_logical_type, specialty_carrier_base

    tgt = normalize_logical_type(tgt_type)
    if tgt not in {"integer", "decimal", "float"}:
        return False
    if specialty_carrier_base(src_type) == "OBJECTID":
        return True
    src = normalize_logical_type(src_type)
    if src not in {"string", "text", "unknown"}:
        return False
    form = _normalize(source).replace(" ", "")
    return form in {"_id", "id", "objectid", "object_id", "oid", "mongo_id"}


def _type_compat_penalty(src_type: str, tgt_type: str, *, source_name: str = "") -> float:
    """Reduce score for incompatible type pairs using the canonical type-system rules.

    Lossy pairs must not clear Map auto-approve / G4 after an Exact-name boost —
    demote hard enough that calibrated confidence stays ≤0.84.
    """
    from services.type_system import is_lossy_coercion, normalize_logical_type

    if not src_type or not tgt_type:
        return 0.0
    if source_name and _identity_onto_numeric_landmine(source_name, src_type, tgt_type):
        # Stronger than generic lossy — must lose to create-new text path.
        return 0.92
    if is_lossy_coercion(src_type, tgt_type):
        src = normalize_logical_type(src_type)
        tgt = normalize_logical_type(tgt_type)
        if src == "binary" and tgt != "binary":
            return 0.8
        if src in ("json", "array") and tgt in ("integer", "decimal", "boolean", "date", "datetime", "time", "binary", "uuid"):
            return 0.7
        if src in ("decimal", "float", "double") and tgt == "integer":
            return 0.55
        if src in ("datetime", "timestamp") and tgt == "date":
            return 0.5
        return 0.5
    return 0.0

def _type_aware_boost(src_type: str, tgt_type: str) -> float:
    """Boost score for exact or highly compatible type matches."""
    from services.type_system import is_lossy_coercion, normalize_logical_type

    if not src_type or not tgt_type:
        return 0.0
    src = normalize_logical_type(src_type)
    tgt = normalize_logical_type(tgt_type)
    if src == tgt:
        return 0.05
    if is_lossy_coercion(src_type, tgt_type):
        return 0.0
    # Safe widening / cross-cast pairs that are not lossy.
    safe_pairs: set[tuple[str, str]] = {
        ("integer", "decimal"), ("boolean", "integer"), ("boolean", "decimal"),
        ("date", "datetime"), ("string", "text"), ("uuid", "string"), ("uuid", "text"),
        ("json", "text"), ("array", "text"), ("json", "string"), ("array", "string"),
    }
    if (src, tgt) in safe_pairs:
        return 0.03
    if src in ("string", "text", "uuid") and tgt in ("string", "text", "uuid"):
        return 0.02
    return 0.0


def _sample_consistency_boost(samples: list[str] | None, source_type: str, target_type: str) -> float:
    """Boost score when sample values parse cleanly for target logical type."""
    if not samples or len(samples) < 2:
        return 0.0
    from services.transform_engine import apply_transform, infer_transform_for_mapping

    transform = infer_transform_for_mapping(
        "col", "col", source_type, target_type, source_samples=samples,
    )
    ok = 0
    checked = 0
    for raw in samples[:8]:
        if raw is None or str(raw).strip() == "":
            continue
        checked += 1
        _, err = apply_transform(str(raw), transform)
        if not err:
            ok += 1
    if checked < 2:
        return 0.0
    rate = ok / checked
    if rate >= 0.9:
        return 0.06
    if rate >= 0.7:
        return 0.03
    if rate < 0.2:
        # Hard demote: ObjectId/hex → DECIMAL (etc.) must lose to type-compatible targets.
        return -0.90
    if rate < 0.4:
        return -0.15
    return 0.0


def _score_pair(
    source: str,
    target: str,
    idf: dict[str, float],
    avgdl: float,
    source_role: str | None = None,
    target_role: str | None = None,
    source_type: str = "VARCHAR",
    target_type: str = "VARCHAR",
    source_samples: list[str] | None = None,
) -> tuple[float, str]:
    from services.semantic_analyzer import role_match_boost
    from services.training_lexicon import lexicon_boost

    src_norm = _normalize(source)
    tgt_norm = _normalize(target)
    src_sem = _semantic_form(source)
    tgt_sem_raw = _semantic_form(target)

    type_penalty = _type_compat_penalty(source_type, target_type, source_name=source)
    type_boost = _type_aware_boost(source_type, target_type)
    sample_boost = _sample_consistency_boost(source_samples, source_type, target_type)
    if (
        sample_boost > -0.5
        and _identity_onto_numeric_landmine(source, source_type, target_type)
    ):
        # No samples: still refuse ObjectId/text identity → NUMBER (Validate would
        # only catch it later after Map already offered the landmine).
        sample_boost = -0.90

    def _finish(score: float, reason: str) -> tuple[float, str]:
        adjusted = max(0.0, min(0.995, float(score) - type_penalty + type_boost + sample_boost))
        return adjusted, reason

    if src_norm == tgt_norm:
        return _finish(0.995, "Exact name match")
    if src_sem == tgt_sem_raw:
        return _finish(0.975, "Exact semantic token match")

    schematic = None
    try:
        from services.schematic_index import schematic_match_boost
        schematic = schematic_match_boost(source, target)
    except ImportError:
        pass
    if schematic is not None:
        # Blend a touch of form similarity so equal schematic hits
        # (mobile_phone → phone vs phone_number) still differentiate.
        form_ratio = _similarity(src_sem, tgt_sem_raw)
        blended = min(0.995, schematic * 0.92 + form_ratio * 0.08)
        return _finish(blended, "Schematic index match (1M+ variants)")

    # Hard entity conflict (created vs updated, order vs transaction): demote early.
    agreement = _entity_agreement(source, target)
    if agreement == 0.0:
        form_ratio = _similarity(src_sem, tgt_sem_raw)
        return _finish(min(0.42, form_ratio * 0.55), "Conflicting entity qualifiers")

    src_canon = _canonical_form(source)
    tgt_canon = _canonical_form(target)
    expanded = _semantic_form(source)
    if src_canon and _normalize(target) == src_canon:
        if _qualifier_tokens(source):
            return _finish(0.76, "Canonical schematic resolution (specific→bare leaf)")
        return _finish(0.99, "Canonical schematic resolution (exact target)")
    if src_canon and tgt_canon and src_canon == tgt_canon and _qualifiers_compatible(source, target):
        src_q = _qualifier_tokens(source)
        tgt_q = _qualifier_tokens(target)
        if not src_q and tgt_q:
            pass  # generic → specific: fall through
        elif src_q and _is_bare_domain_leaf(target):
            return _finish(0.76, "Canonical schematic resolution (specific→generic)")
        elif src_q and not tgt_q:
            pass  # compound domain target — fall through
        elif _normalize(target) == _normalize(expanded):
            return _finish(0.985, "Canonical schematic resolution (expanded form)")
        else:
            return _finish(0.93, "Canonical schematic resolution")

    if _normalize(target) == _normalize(expanded):
        return _finish(0.94, "Abbreviation expansion match")

    # Domain expansions: mobile_phone → phone_number, etc.
    src_parts = set(src_sem.split("_")) - {""}
    if "mobile" in src_parts and "phone" in src_parts and tgt_sem_raw == "phone_number":
        return _finish(0.965, "Mobile phone → phone_number expansion")
    if "mobile" in src_parts and "phone" in src_parts and tgt_sem_raw == "phone":
        return _finish(0.86, "Mobile phone → short phone form")

    if source_role and target_role:
        boost = role_match_boost(source_role, target_role)
        if boost is not None:
            # Tie-break same-role collisions (email_addr vs usr_email) with lexical form.
            from difflib import SequenceMatcher

            lex = SequenceMatcher(None, src_norm, tgt_norm).ratio()
            adjusted = min(0.995, float(boost) * 0.82 + lex * 0.18)
            if lex >= 0.72:
                adjusted = max(adjusted, min(0.97, float(boost)))
            return _finish(adjusted, f"Semantic role match: {source_role} → {target_role} (lex={lex:.2f})")

    boosted = lexicon_boost(source, target)
    if boosted is not None:
        return _finish(boosted, "Training lexicon match (synthetic_v1)")

    # Lexical stage uses abbreviation-expanded forms — not schematic canonical
    # collapse — so order_amount vs total_amount can outrank transaction_amount.
    src_form = _semantic_form(source)
    tgt_form = _semantic_form(target)
    form_ratio = _similarity(src_form, tgt_form)

    if src_form == tgt_form:
        return _finish(0.96, "Semantic token match")

    bm25 = _bm25_score(
        src_form.split("_"),
        tgt_form.split("_"),
        idf,
        avgdl,
    )
    bm25_norm = min(bm25 / 8.0, 1.0)

    # Advanced heuristic: ML Baseline prediction
    ml_model = _load_ml_baseline()
    ml_boost = 0.0
    if ml_model:
        pred_tgt, pred_score = ml_model.predict_target(source)
        if _normalize(pred_tgt) == tgt_norm and pred_score > 0.5:
            ml_boost = min(pred_score * 0.15, 0.15)
            if pred_score > 0.8:
                return _finish(0.95, "ML Baseline highly confident match")

    src_toks = set(src_form.split("_")) - {""}
    tgt_toks = set(tgt_form.split("_")) - {""}
    overlap = len(src_toks & tgt_toks)
    shared = src_toks & tgt_toks
    only_generic_overlap = overlap > 0 and shared <= _DOMAIN_LEAVES

    # Prefer specific amount targets when source has an entity prefix
    # (order_amount → total_amount over bare amount).
    src_q = _qualifier_tokens(source)
    if src_q and _is_bare_domain_leaf(target) and only_generic_overlap:
        return _finish(
            min(0.80, 0.62 + form_ratio * 0.20 + bm25_norm * 0.05),
            "Specific source → bare domain leaf",
        )

    # Compound domain targets (total_amount) with a matching leaf + form similarity.
    if src_q and only_generic_overlap and not _is_bare_domain_leaf(target):
        return _finish(
            min(0.94, 0.72 + form_ratio * 0.22 + bm25_norm * 0.04),
            "Specific source → compound domain target",
        )

    if src_form in tgt_form or tgt_form in src_form:
        if min(len(src_form), len(tgt_form)) >= 4:
            base = 0.80 + form_ratio * 0.14 + bm25_norm * 0.04 + agreement * 0.04
            return _finish(min(0.97, max(0.86, base)), "Partial semantic overlap + form similarity")

    if overlap >= 2:
        return _finish(
            0.82 + overlap * 0.03 + bm25_norm * 0.05 + form_ratio * 0.04 + agreement * 0.03 + ml_boost,
            f"Shared tokens ({overlap}) + BM25",
        )

    fuzzy = form_ratio

    def ngrams(s, n):
        return set(s[i:i+n] for i in range(max(1, len(s)-n+1)))
    jaccard = 0.0
    s_ngrams, t_ngrams = ngrams(src_form, 3), ngrams(tgt_form, 3)
    if s_ngrams or t_ngrams:
        jaccard = len(s_ngrams & t_ngrams) / len(s_ngrams | t_ngrams)

    if only_generic_overlap:
        combined = 0.55 + fuzzy * 0.35 + jaccard * 0.08 + agreement * 0.05 + ml_boost
        return _finish(min(combined, 0.92), "Generic leaf + form similarity")

    combined = max(fuzzy * 0.75, bm25_norm * 0.88, jaccard * 0.82) + ml_boost + agreement * 0.05

    if combined >= 0.78:
        return _finish(min(combined, 0.99), "BM25 / Jaccard lexical retrieval")
    if overlap == 1 and len(src_form.split("_")) > 1:
        return _finish(min(0.70 + fuzzy * 0.22 + agreement * 0.05 + ml_boost, 0.95), "Single token overlap + form similarity")
    return _finish(min(combined, 0.99), "Character similarity")

def _hungarian_minimize(cost: list[list[float]]) -> list[int]:
    """Return row -> column assignment for rows <= columns."""
    if not cost:
        return []
    n = len(cost)
    m = len(cost[0])
    if n > m:
        raise ValueError("Hungarian solver requires rows <= columns")

    u = [0.0] * (n + 1)
    v = [0.0] * (m + 1)
    p = [0] * (m + 1)
    way = [0] * (m + 1)

    for i in range(1, n + 1):
        p[0] = i
        j0 = 0
        minv = [float("inf")] * (m + 1)
        used = [False] * (m + 1)
        while True:
            used[j0] = True
            i0 = p[j0]
            delta = float("inf")
            j1 = 0
            for j in range(1, m + 1):
                if used[j]:
                    continue
                cur = cost[i0 - 1][j - 1] - u[i0] - v[j]
                if cur < minv[j]:
                    minv[j] = cur
                    way[j] = j0
                if minv[j] < delta:
                    delta = minv[j]
                    j1 = j
            for j in range(0, m + 1):
                if used[j]:
                    u[p[j]] += delta
                    v[j] -= delta
                else:
                    minv[j] -= delta
            j0 = j1
            if p[j0] == 0:
                break

        while True:
            j1 = way[j0]
            p[j0] = p[j1]
            j0 = j1
            if j0 == 0:
                break

    assignment = [-1] * n
    for j in range(1, m + 1):
        if p[j] > 0:
            assignment[p[j] - 1] = j - 1
    return assignment


def _optimal_assignment(
    source_columns: list[str],
    target_columns: list[str],
    scores: dict[tuple[str, str], tuple[float, str]],
) -> dict[str, tuple[str, float, str]]:
    """Maximum-weight one-to-one assignment across source/target columns."""
    if not source_columns or not target_columns:
        return {}

    max_score = 1.0
    assigned: dict[str, tuple[str, float, str]] = {}

    if len(source_columns) <= len(target_columns):
        cost = [
            [max_score - scores[(src, tgt)][0] for tgt in target_columns]
            for src in source_columns
        ]
        assignment = _hungarian_minimize(cost)
        for src_idx, tgt_idx in enumerate(assignment):
            if tgt_idx < 0:
                continue
            src = source_columns[src_idx]
            tgt = target_columns[tgt_idx]
            score, reason = scores[(src, tgt)]
            assigned[src] = (tgt, score, reason)
        return assigned

    # Transpose when sources outnumber targets so every target is used at most once.
    cost = [
        [max_score - scores[(src, tgt)][0] for src in source_columns]
        for tgt in target_columns
    ]
    assignment = _hungarian_minimize(cost)
    for tgt_idx, src_idx in enumerate(assignment):
        if src_idx < 0:
            continue
        src = source_columns[src_idx]
        tgt = target_columns[tgt_idx]
        score, reason = scores[(src, tgt)]
        assigned[src] = (tgt, score, reason)
    return assigned


def _alternatives(
    source: str,
    target_columns: list[str],
    scores: dict[tuple[str, str], tuple[float, str]],
    *,
    limit: int = 3,
) -> list[dict]:
    ranked = sorted(
        (
            {
                "target": target,
                "confidence": round(min(scores[(source, target)][0], 0.99), 3),
                "reasoning": scores[(source, target)][1],
            }
            for target in target_columns
        ),
        key=lambda item: item["confidence"],
        reverse=True,
    )
    return ranked[:limit]


# Create-new / identity passthrough is "will CREATE", not "proven against existing dest".
# Reserve 0.99 for existing-dest exact+sample match.
IDENTITY_PASSTHROUGH_CONFIDENCE = 0.92


def map_columns(
    source_columns: list[str],
    target_columns: list[str],
    *,
    source_schemas: list[dict] | None = None,
    target_schemas: list[dict] | None = None,
    threshold: float = 0.85,
    destination_db_type: str = "",
    destination_table_exists: bool | None = None,
) -> list[dict]:
    from services.semantic_analyzer import analyze_column
    from services.type_system import create_new_mapping_target_type, ddl_type

    floor = max(0.55, threshold - 0.3)
    src_roles: dict[str, str] = {}
    tgt_roles: dict[str, str] = {}
    src_types: dict[str, str] = {}
    tgt_types: dict[str, str] = {}
    src_samples: dict[str, list[str]] = {}
    dest_db = (destination_db_type or "").strip().lower()

    if source_schemas:
        for s in source_schemas:
            analyzed = analyze_column(s.get("name", ""), s.get("inferred_type", "VARCHAR"), s.get("samples", []))
            src_roles[s["name"]] = analyzed["semantic_role"]
            src_types[s["name"]] = s.get("inferred_type", "VARCHAR")
            if s.get("samples"):
                src_samples[s["name"]] = [str(x) for x in s["samples"][:8]]
    if target_schemas:
        for t in target_schemas:
            analyzed = analyze_column(t.get("name", ""), t.get("inferred_type", "VARCHAR"), t.get("samples", []))
            tgt_roles[t["name"]] = analyzed["semantic_role"]
            tgt_types[t["name"]] = t.get("inferred_type", "VARCHAR")
    elif target_columns:
        for t in target_columns:
            analyzed = analyze_column(t, "VARCHAR", [])
            tgt_roles[t] = analyzed["semantic_role"]
            tgt_types[t] = "VARCHAR"

    if not target_columns:
        # Empty targets are NOT automatically create-new. Only invent CREATE when
        # the destination object is confirmed missing. Existing/unknown + empty
        # columns = pending schema (shared SQL/warehouse failure mode).
        out: list[dict] = []
        confirmed_missing = destination_table_exists is False
        for src in source_columns:
            src_type = src_types.get(src, "VARCHAR")
            dest_native = ddl_type(dest_db, src_type) if dest_db else src_type
            map_target_type = create_new_mapping_target_type(src_type, dest_db)
            if confirmed_missing:
                out.append(
                    {
                        "source": src,
                        "target": _semantic_form(src),
                        "confidence": IDENTITY_PASSTHROUGH_CONFIDENCE,
                        "reasoning": (
                            f"New destination table — identity mapping; "
                            f"types will CREATE on first write as {dest_native}"
                        ),
                        "user_override": False,
                        "source_type": src_type,
                        "target_type": map_target_type,
                        "assignment_strategy": "identity_passthrough",
                        "create_new": True,
                    }
                )
            else:
                exists_note = (
                    "Destination table exists but column metadata did not load"
                    if destination_table_exists is True
                    else "Destination schema unavailable — not treating as create-new"
                )
                out.append(
                    {
                        "source": src,
                        "target": _semantic_form(src),
                        "confidence": 0.55,
                        "reasoning": (
                            f"{exists_note}. Retry destination schema load before Map "
                            f"invents CREATE TABLE / identity passthrough "
                            f"(projected type {dest_native})."
                        ),
                        "user_override": False,
                        "source_type": src_type,
                        "target_type": dest_native,
                        "assignment_strategy": "pending_dest_schema",
                        "create_new": False,
                        "requires_review": True,
                    }
                )
        return _apply_create_new_risk_stamps(out, dest_db)

    idf = _build_idf(source_columns + target_columns)
    all_doc_lens = [len(_tokenize(c)) for c in source_columns + target_columns]
    avgdl = sum(all_doc_lens) / max(len(all_doc_lens), 1)
    used_targets: set[str] = set()
    mappings: list[dict] = []

    pair_scores: dict[tuple[str, str], tuple[float, str]] = {}
    for source in source_columns:
        for target in target_columns:
            score, reason = _score_pair(
                source,
                target,
                idf,
                avgdl,
                src_roles.get(source),
                tgt_roles.get(target),
                src_types.get(source, "VARCHAR"),
                tgt_types.get(target, "VARCHAR"),
                src_samples.get(source),
            )
            pair_scores[(source, target)] = (score, reason)

    assigned_sources: set[str] = set()
    optimal = _optimal_assignment(source_columns, target_columns, pair_scores)
    for source in source_columns:
        assigned = optimal.get(source)
        if not assigned:
            continue
        target, score, reason = assigned
        if score < floor:
            continue
        alternatives = _alternatives(source, target_columns, pair_scores)
        winner = alternatives[0]["confidence"] if alternatives else score
        runner_up = alternatives[1]["confidence"] if len(alternatives) > 1 else 0.0
        score_gap = round(max(winner - runner_up, 0.0), 3)
        requires_review = score_gap < 0.08
        src_type = src_types.get(source, "VARCHAR")
        tgt_type = tgt_types.get(target, "VARCHAR")
        try:
            from services.type_system import is_lossy_coercion
            lossy_pair = is_lossy_coercion(src_type, tgt_type)
        except Exception:
            # Fail closed — unknown type authority must not green-path remaps.
            lossy_pair = True
        if lossy_pair:
            # Exact-name must not exempt DECIMAL→INTEGER / DATETIME→DATE remaps.
            requires_review = True
            score = min(float(score), 0.84)
            reason = f"{reason} · lossy type pair"
        elif reason.startswith("Exact") and score_gap >= 0.08:
            # Decisive Exact with compatible types — review not required.
            requires_review = False
        assigned_sources.add(source)
        used_targets.add(target)
        mappings.append(
            {
                "source": source,
                "target": target,
                "confidence": _calibrated_confidence(
                    score, score_gap=score_gap, requires_review=requires_review,
                ),
                "reasoning": reason,
                "user_override": False,
                "assignment_strategy": "optimal_bipartite_hungarian",
                "alternatives": alternatives,
                "score_gap": score_gap,
                "requires_review": requires_review,
                "source_type": src_type,
                "target_type": tgt_type,
            }
        )

    for source in source_columns:
        if source in assigned_sources:
            continue
        best_target = ""
        best_score = 0.0
        best_reason = ""
        for target in target_columns:
            if target in used_targets:
                continue
            score, reason = _score_pair(
                source,
                target,
                idf,
                avgdl,
                src_roles.get(source),
                tgt_roles.get(target),
                src_types.get(source, "VARCHAR"),
                tgt_types.get(target, "VARCHAR"),
                src_samples.get(source),
            )
            if score > best_score:
                best_score, best_target, best_reason = score, target, reason
        alternatives = _alternatives(source, target_columns, pair_scores)
        src_type = src_types.get(source, "VARCHAR")
        # Prefer a near-matching existing destination over inventing a column
        # when abbreviation/form similarity is strong enough (ph_num → phone).
        # Never override a hard type/sample demotion (ObjectId → DECIMAL id).
        near_tgt, near_ratio = _near_target_by_form(source, target_columns, used_targets=used_targets)
        if near_tgt:
            near_tgt_type = tgt_types.get(near_tgt, "VARCHAR")
            near_penalty = _type_compat_penalty(
                src_type, near_tgt_type, source_name=source
            )
            near_sample = _sample_consistency_boost(
                src_samples.get(source), src_type, near_tgt_type,
            )
            if near_penalty >= 0.20 or near_sample <= -0.50:
                near_tgt, near_ratio = "", 0.0
        if near_tgt and near_ratio >= 0.62 and (not best_target or best_score < floor or near_ratio > best_score):
            # Promote near form match into the assignment set.
            near_score = max(best_score, 0.55 + near_ratio * 0.40)
            if near_score >= floor or near_ratio >= 0.70:
                used_targets.add(near_tgt)
                assigned_sources.add(source)
                winner = alternatives[0]["confidence"] if alternatives else near_score
                runner_up = alternatives[1]["confidence"] if len(alternatives) > 1 else 0.0
                score_gap = round(max(winner - runner_up, 0.0), 3)
                requires_review = near_ratio < 0.85
                near_tgt_type = tgt_types.get(near_tgt, "VARCHAR")
                try:
                    from services.type_system import is_lossy_coercion
                    near_lossy = is_lossy_coercion(src_type, near_tgt_type)
                except Exception:
                    near_lossy = True
                if near_lossy:
                    requires_review = True
                    near_score = min(float(near_score), 0.84)
                mappings.append(
                    {
                        "source": source,
                        "target": near_tgt,
                        "confidence": _calibrated_confidence(
                            near_score,
                            score_gap=score_gap,
                            requires_review=requires_review,
                            hard_cap=0.97,
                        ),
                        "reasoning": (
                            f"Near-form match to existing destination "
                            f"(similarity={near_ratio:.2f}); prefer over inventing a column"
                            + (" · lossy type pair" if near_lossy else "")
                        ),
                        "user_override": False,
                        "assignment_strategy": "near_form_existing",
                        "alternatives": alternatives,
                        "score_gap": score_gap,
                        "requires_review": requires_review,
                        "source_type": src_type,
                        "target_type": near_tgt_type,
                    }
                )
                continue
        # Prefer create-new text column over a lossy existing target (e.g. ObjectId → DECIMAL).
        if (not best_target or best_score < floor) and target_columns:
            # Final gate: if any unused dest is a reasonable form match, map there
            # with review instead of inventing (avoids ph_number when phone exists).
            if near_tgt and near_ratio >= 0.50:
                used_targets.add(near_tgt)
                near_tgt_type = tgt_types.get(near_tgt, "VARCHAR")
                try:
                    from services.type_system import is_lossy_coercion
                    near_lossy = is_lossy_coercion(src_type, near_tgt_type)
                except Exception:
                    near_lossy = True
                mappings.append(
                    {
                        "source": source,
                        "target": near_tgt,
                        "confidence": round(
                            min(0.55 + near_ratio * 0.35, 0.84 if near_lossy else 0.88),
                            3,
                        ),
                        "reasoning": (
                            f"Sub-threshold score but near existing column "
                            f"(similarity={near_ratio:.2f}) — review before create-new"
                            + (" · lossy type pair" if near_lossy else "")
                        ),
                        "user_override": False,
                        "assignment_strategy": "near_form_review",
                        "alternatives": alternatives,
                        "score_gap": 0.0,
                        "requires_review": True,
                        "source_type": src_type,
                        "target_type": near_tgt_type,
                    }
                )
                continue
            dest_native = ddl_type(dest_db, src_type) if dest_db else src_type
            map_target_type = create_new_mapping_target_type(src_type, dest_db)
            # Prefer the original source name for ADD COLUMN (_id stays _id).
            # Semantic form alone collapses _id → id, then id_text — a name that
            # operators did not approve and that often never gets DDL.
            taken = {t.lower() for t in used_targets} | {t.lower() for t in target_columns}
            candidate = source.strip() or _semantic_form(source)
            if candidate.lower() in taken:
                sem = _semantic_form(source)
                candidate = sem if sem.lower() not in taken else candidate
            if candidate.lower() in taken:
                base = re.sub(r"[^A-Za-z0-9_]+", "_", candidate).strip("_") or "field"
                candidate = f"{base}_text" if f"{base}_text".lower() not in taken else f"src_{base}"
            used_targets.add(candidate)
            mappings.append(
                {
                    "source": source,
                    "target": candidate,
                    "confidence": IDENTITY_PASSTHROUGH_CONFIDENCE,
                    "reasoning": (
                        "No type-compatible destination column — map to a new field "
                        f"(create/ADD as {dest_native}); do not coerce into incompatible DDL"
                    ),
                    "user_override": False,
                    "source_type": src_type,
                    "target_type": map_target_type,
                    "assignment_strategy": "create_compatible_new",
                    "create_new": True,
                    "alternatives": alternatives,
                    "score_gap": 0.0,
                    "requires_review": True,
                }
            )
            continue
        if not best_target:
            best_target = _semantic_form(source)
            best_score = 0.55
            best_reason = "No target match — inferred semantic name (no destination schema)"
            alternatives = []
        else:
            used_targets.add(best_target)
        winner = alternatives[0]["confidence"] if alternatives else best_score
        runner_up = alternatives[1]["confidence"] if len(alternatives) > 1 else 0.0
        score_gap = round(max(winner - runner_up, 0.0), 3)
        requires_review = score_gap < 0.08
        src_type = src_types.get(source, "VARCHAR")
        tgt_type = tgt_types.get(best_target, "VARCHAR") if best_target else "VARCHAR"
        try:
            from services.type_system import is_lossy_coercion
            lossy_pair = bool(best_target and is_lossy_coercion(src_type, tgt_type))
        except Exception:
            lossy_pair = bool(best_target)
        if lossy_pair:
            requires_review = True
            best_score = min(float(best_score), 0.84)
            best_reason = f"{best_reason} · lossy type pair"
        mappings.append(
            {
                "source": source,
                "target": best_target,
                "confidence": _calibrated_confidence(
                    max(best_score, 0.55),
                    score_gap=score_gap,
                    requires_review=requires_review,
                ),
                "reasoning": best_reason,
                "user_override": False,
                "assignment_strategy": "fallback_best_available",
                "alternatives": alternatives,
                "score_gap": score_gap,
                "requires_review": requires_review,
                "source_type": src_type,
                "target_type": tgt_type,
            }
        )

    mappings.sort(key=lambda m: source_columns.index(m["source"]))
    return _apply_create_new_risk_stamps(mappings, dest_db)


def _apply_create_new_risk_stamps(
    mappings: list[dict],
    destination_db_type: str = "",
) -> list[dict]:
    """Stamp create-new type risks without importing mapping_pipeline (cycle-safe)."""
    from services.type_system import (
        assess_create_new_type_risk,
        create_new_mapping_target_type,
        is_lossy_coercion,
        is_precision_collapse_coercion,
    )

    out: list[dict] = []
    for m in mappings:
        row = dict(m)
        # Only confirmed create-new strategies — never stamp pending schema as
        # create-new (UI keeps createNew=false; inventing risks/DDL contradicts that).
        strategy = str(row.get("assignment_strategy") or "")
        is_create = bool(
            row.get("create_new")
            or strategy in {
                "create_compatible_new",
                "identity_passthrough",
            }
        )
        if not is_create or strategy == "pending_dest_schema":
            out.append(row)
            continue
        src = str(row.get("source_type") or "VARCHAR")
        db = destination_db_type or str(row.get("dest_db_type") or "")
        stamped = str(row.get("target_type") or "").strip()
        physical = create_new_mapping_target_type(src, db) if db else stamped or src
        tgt = physical or stamped or src
        if physical and physical != stamped:
            row["target_type"] = physical
            tgt = physical
        risks = assess_create_new_type_risk(src, tgt, destination_db_type=db)
        if risks:
            row["create_new_risks"] = risks
            row["requires_review"] = True
            kinds = ", ".join(sorted({r.get("kind", "") for r in risks if r.get("kind")}))
            reason = str(row.get("reasoning") or "")
            note = f"create-new type risk: {kinds}"
            if note not in reason.lower():
                row["reasoning"] = f"{reason} · {note}".strip(" ·")
            if (
                (not row.get("fidelity") or row.get("fidelity") == "lossless")
                and (is_lossy_coercion(src, tgt) or is_precision_collapse_coercion(src, tgt))
            ):
                row["fidelity"] = "lossy_cast"
        out.append(row)
    return out
