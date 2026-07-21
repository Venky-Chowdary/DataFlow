#!/usr/bin/env python3
"""Generate enterprise mapping golden fixture (domain-batched, typed, adversarial)."""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "mapping_golden_enterprise.json"

# Each domain is evaluated as its own bipartite assignment (fair enterprise bar).
# Cases include source/target types and an optional adversarial tag.
DOMAINS: dict[str, list[dict]] = {
    "ecommerce": [
        ("cust_id", "customer_id", "VARCHAR", "VARCHAR"),
        ("cust_name", "customer_name", "VARCHAR", "VARCHAR"),
        ("fname", "first_name", "VARCHAR", "VARCHAR"),
        ("lname", "last_name", "VARCHAR", "VARCHAR"),
        ("email_addr", "email_address", "VARCHAR", "VARCHAR"),
        ("usr_email", "user_email", "VARCHAR", "VARCHAR"),
        ("ph_num", "phone", "VARCHAR", "VARCHAR"),
        ("mobile_phone", "phone_number", "VARCHAR", "VARCHAR"),
        ("ship_addr", "shipping_address", "VARCHAR", "VARCHAR"),
        ("bill_zip", "billing_postal_code", "VARCHAR", "VARCHAR"),
        ("ord_id", "order_id", "VARCHAR", "VARCHAR"),
        ("prod_id", "product_id", "VARCHAR", "VARCHAR"),
        ("qty", "quantity", "INTEGER", "INTEGER"),
        ("unit_prc", "unit_price", "DECIMAL", "DECIMAL"),
        ("order_amt", "total_amount", "DECIMAL", "DECIMAL"),
        ("txn_amt", "transaction_amount", "DECIMAL", "DECIMAL"),
        ("AMT", "amount", "DECIMAL", "DECIMAL"),
        ("disc_amt", "discount_amount", "DECIMAL", "DECIMAL"),
        ("tax_amt", "tax_amount", "DECIMAL", "DECIMAL"),
        ("sku", "product_sku", "VARCHAR", "VARCHAR"),
        ("created_at", "created_timestamp", "TIMESTAMP", "TIMESTAMP"),
        ("updated_ts", "updated_at", "TIMESTAMP", "TIMESTAMP"),
        ("order_dt", "order_date", "DATE", "DATE"),
        ("ship_dt", "ship_date", "DATE", "DATE"),
        ("status_cd", "order_status", "VARCHAR", "VARCHAR"),
        ("curr_cd", "currency_code", "VARCHAR", "VARCHAR"),
        ("line_amt", "line_amount", "DECIMAL", "DECIMAL"),
        ("net_amt", "net_amount", "DECIMAL", "DECIMAL"),
        ("gross_amt", "gross_amount", "DECIMAL", "DECIMAL"),
        ("promo_cd", "promo_code", "VARCHAR", "VARCHAR"),
        ("cart_id", "cart_id", "VARCHAR", "VARCHAR"),
        ("store_id", "store_id", "VARCHAR", "VARCHAR"),
        ("channel", "sales_channel", "VARCHAR", "VARCHAR"),
        ("refund_amt", "refund_amount", "DECIMAL", "DECIMAL"),
        ("ship_method", "shipping_method", "VARCHAR", "VARCHAR"),
        ("tracking_no", "tracking_number", "VARCHAR", "VARCHAR"),
        ("gift_msg", "gift_message", "VARCHAR", "VARCHAR"),
        ("is_gift", "is_gift", "BOOLEAN", "BOOLEAN"),
        ("item_cnt", "item_count", "INTEGER", "INTEGER"),
        ("weight_kg", "weight_kg", "DECIMAL", "DECIMAL"),
    ],
    "finance": [
        ("acct_no", "account_number", "VARCHAR", "VARCHAR"),
        ("acct_bal", "account_balance", "DECIMAL", "DECIMAL"),
        ("txn_id", "transaction_id", "VARCHAR", "VARCHAR"),
        ("txn_dt", "transaction_date", "DATE", "DATE"),
        ("pay_amt", "payment_amount", "DECIMAL", "DECIMAL"),
        ("inv_no", "invoice_number", "VARCHAR", "VARCHAR"),
        ("invoice_amt", "invoice_amount", "DECIMAL", "DECIMAL"),
        ("wire_ref", "wire_reference", "VARCHAR", "VARCHAR"),
        ("ach_trace", "ach_trace_number", "VARCHAR", "VARCHAR"),
        ("tax_amt", "tax_amount", "DECIMAL", "DECIMAL"),
        ("vat_amt", "vat_amount", "DECIMAL", "DECIMAL"),
        ("ccy", "currency_code", "VARCHAR", "VARCHAR"),
        ("fx_rate", "exchange_rate", "DECIMAL", "DECIMAL"),
        ("int_rate", "interest_rate", "DECIMAL", "DECIMAL"),
        ("princ_amt", "principal_amount", "DECIMAL", "DECIMAL"),
        ("fee_amt", "fee_amount", "DECIMAL", "DECIMAL"),
        ("gl_cd", "gl_code", "VARCHAR", "VARCHAR"),
        ("cost_ctr", "cost_center", "VARCHAR", "VARCHAR"),
        ("debit_amt", "debit_amount", "DECIMAL", "DECIMAL"),
        ("credit_amt", "credit_amount", "DECIMAL", "DECIMAL"),
        ("posting_dt", "posting_date", "DATE", "DATE"),
        ("value_dt", "value_date", "DATE", "DATE"),
        ("settlement_dt", "settlement_date", "DATE", "DATE"),
        ("iban", "iban", "VARCHAR", "VARCHAR"),
        ("bic", "bic_code", "VARCHAR", "VARCHAR"),
        ("routing_no", "routing_number", "VARCHAR", "VARCHAR"),
        ("card_no", "card_number", "VARCHAR", "VARCHAR"),
        ("cvv", "cvv", "VARCHAR", "VARCHAR"),
        ("merchant_id", "merchant_id", "VARCHAR", "VARCHAR"),
        ("auth_cd", "auth_code", "VARCHAR", "VARCHAR"),
        ("ledger_bal", "ledger_balance", "DECIMAL", "DECIMAL"),
        ("avail_bal", "available_balance", "DECIMAL", "DECIMAL"),
        ("overdraft_lim", "overdraft_limit", "DECIMAL", "DECIMAL"),
        ("stmt_dt", "statement_date", "DATE", "DATE"),
        ("recon_flg", "reconciled_flag", "BOOLEAN", "BOOLEAN"),
        ("batch_id", "batch_id", "VARCHAR", "VARCHAR"),
        ("journal_id", "journal_id", "VARCHAR", "VARCHAR"),
        ("fiscal_yr", "fiscal_year", "INTEGER", "INTEGER"),
        ("fiscal_qtr", "fiscal_quarter", "INTEGER", "INTEGER"),
        ("risk_score", "risk_score", "DECIMAL", "DECIMAL"),
        ("aml_flg", "aml_flag", "BOOLEAN", "BOOLEAN"),
    ],
    "healthcare": [
        ("pat_id", "patient_id", "VARCHAR", "VARCHAR"),
        ("mrn", "medical_record_number", "VARCHAR", "VARCHAR"),
        ("pat_dob", "date_of_birth", "DATE", "DATE"),
        ("dob", "date_of_birth", "DATE", "DATE"),
        ("gender_cd", "gender", "VARCHAR", "VARCHAR"),
        ("diag_cd", "diagnosis_code", "VARCHAR", "VARCHAR"),
        ("proc_cd", "procedure_code", "VARCHAR", "VARCHAR"),
        ("enc_id", "encounter_id", "VARCHAR", "VARCHAR"),
        ("admit_dt", "admission_date", "DATE", "DATE"),
        ("disch_dt", "discharge_date", "DATE", "DATE"),
        ("prov_id", "provider_id", "VARCHAR", "VARCHAR"),
        ("npi", "npi_number", "VARCHAR", "VARCHAR"),
        ("ins_id", "insurance_id", "VARCHAR", "VARCHAR"),
        ("policy_no", "policy_number", "VARCHAR", "VARCHAR"),
        ("claim_id", "claim_id", "VARCHAR", "VARCHAR"),
        ("claim_amt", "claim_amount", "DECIMAL", "DECIMAL"),
        ("allowed_amt", "allowed_amount", "DECIMAL", "DECIMAL"),
        ("paid_amt", "paid_amount", "DECIMAL", "DECIMAL"),
        ("copay_amt", "copay_amount", "DECIMAL", "DECIMAL"),
        ("deduct_amt", "deductible_amount", "DECIMAL", "DECIMAL"),
        ("icd10", "icd10_code", "VARCHAR", "VARCHAR"),
        ("cpt_cd", "cpt_code", "VARCHAR", "VARCHAR"),
        ("loinc", "loinc_code", "VARCHAR", "VARCHAR"),
        ("lab_result", "lab_result_value", "VARCHAR", "VARCHAR"),
        ("lab_unit", "lab_result_unit", "VARCHAR", "VARCHAR"),
        ("vitals_hr", "heart_rate", "INTEGER", "INTEGER"),
        ("vitals_bp_sys", "bp_systolic", "INTEGER", "INTEGER"),
        ("vitals_bp_dia", "bp_diastolic", "INTEGER", "INTEGER"),
        ("allergy_cd", "allergy_code", "VARCHAR", "VARCHAR"),
        ("med_name", "medication_name", "VARCHAR", "VARCHAR"),
        ("dosage", "dosage", "VARCHAR", "VARCHAR"),
        ("rx_norm", "rxnorm_code", "VARCHAR", "VARCHAR"),
        ("facility_id", "facility_id", "VARCHAR", "VARCHAR"),
        ("ward_cd", "ward_code", "VARCHAR", "VARCHAR"),
        ("bed_no", "bed_number", "VARCHAR", "VARCHAR"),
        ("consent_flg", "consent_flag", "BOOLEAN", "BOOLEAN"),
        ("hipaa_flg", "hipaa_authorized", "BOOLEAN", "BOOLEAN"),
        ("pcp_id", "primary_care_provider_id", "VARCHAR", "VARCHAR"),
        ("emerg_contact", "emergency_contact_name", "VARCHAR", "VARCHAR"),
        ("emerg_phone", "emergency_contact_phone", "VARCHAR", "VARCHAR"),
    ],
    "hr": [
        ("emp_id", "employee_id", "VARCHAR", "VARCHAR"),
        ("emp_no", "employee_number", "VARCHAR", "VARCHAR"),
        ("fname", "first_name", "VARCHAR", "VARCHAR"),
        ("lname", "last_name", "VARCHAR", "VARCHAR"),
        ("hire_dt", "hire_date", "DATE", "DATE"),
        ("term_dt", "termination_date", "DATE", "DATE"),
        ("dept_cd", "department_code", "VARCHAR", "VARCHAR"),
        ("dept_nm", "department_name", "VARCHAR", "VARCHAR"),
        ("job_title", "job_title", "VARCHAR", "VARCHAR"),
        ("mgr_id", "manager_id", "VARCHAR", "VARCHAR"),
        ("salary_amt", "salary_amount", "DECIMAL", "DECIMAL"),
        ("bonus_amt", "bonus_amount", "DECIMAL", "DECIMAL"),
        ("comm_amt", "commission_amount", "DECIMAL", "DECIMAL"),
        ("pay_grade", "pay_grade", "VARCHAR", "VARCHAR"),
        ("work_email", "work_email", "VARCHAR", "VARCHAR"),
        ("work_phone", "work_phone", "VARCHAR", "VARCHAR"),
        ("loc_cd", "location_code", "VARCHAR", "VARCHAR"),
        ("fte", "fte_ratio", "DECIMAL", "DECIMAL"),
        ("emp_status", "employment_status", "VARCHAR", "VARCHAR"),
        ("emp_type", "employment_type", "VARCHAR", "VARCHAR"),
        ("ssn", "ssn", "VARCHAR", "VARCHAR"),
        ("tax_id", "tax_id", "VARCHAR", "VARCHAR"),
        ("bank_acct", "bank_account_number", "VARCHAR", "VARCHAR"),
        ("direct_dep_flg", "direct_deposit_flag", "BOOLEAN", "BOOLEAN"),
        ("pto_bal", "pto_balance", "DECIMAL", "DECIMAL"),
        ("sick_bal", "sick_balance", "DECIMAL", "DECIMAL"),
        ("perf_score", "performance_score", "DECIMAL", "DECIMAL"),
        ("last_review_dt", "last_review_date", "DATE", "DATE"),
        ("cost_ctr", "cost_center", "VARCHAR", "VARCHAR"),
        ("badge_no", "badge_number", "VARCHAR", "VARCHAR"),
        ("shift_cd", "shift_code", "VARCHAR", "VARCHAR"),
        ("union_flg", "union_member_flag", "BOOLEAN", "BOOLEAN"),
        ("remote_flg", "remote_worker_flag", "BOOLEAN", "BOOLEAN"),
        ("start_tm", "shift_start_time", "TIME", "TIME"),
        ("end_tm", "shift_end_time", "TIME", "TIME"),
        ("overtime_hrs", "overtime_hours", "DECIMAL", "DECIMAL"),
        ("regular_hrs", "regular_hours", "DECIMAL", "DECIMAL"),
        ("pay_period_end", "pay_period_end_date", "DATE", "DATE"),
        ("benefits_elig", "benefits_eligible", "BOOLEAN", "BOOLEAN"),
        ("rehire_flg", "rehire_eligible_flag", "BOOLEAN", "BOOLEAN"),
    ],
    "logistics": [
        ("ship_id", "shipment_id", "VARCHAR", "VARCHAR"),
        ("tracking_no", "tracking_number", "VARCHAR", "VARCHAR"),
        ("carrier_cd", "carrier_code", "VARCHAR", "VARCHAR"),
        ("origin_zip", "origin_postal_code", "VARCHAR", "VARCHAR"),
        ("dest_zip", "destination_postal_code", "VARCHAR", "VARCHAR"),
        ("ship_dt", "ship_date", "DATE", "DATE"),
        ("del_dt", "delivery_date", "DATE", "DATE"),
        ("eta_dt", "estimated_arrival_date", "DATE", "DATE"),
        ("weight_lb", "weight_lb", "DECIMAL", "DECIMAL"),
        ("weight_kg", "weight_kg", "DECIMAL", "DECIMAL"),
        ("dim_l", "dimension_length", "DECIMAL", "DECIMAL"),
        ("dim_w", "dimension_width", "DECIMAL", "DECIMAL"),
        ("dim_h", "dimension_height", "DECIMAL", "DECIMAL"),
        ("pkg_cnt", "package_count", "INTEGER", "INTEGER"),
        ("freight_amt", "freight_amount", "DECIMAL", "DECIMAL"),
        ("fuel_surcharge", "fuel_surcharge_amount", "DECIMAL", "DECIMAL"),
        ("warehouse_id", "warehouse_id", "VARCHAR", "VARCHAR"),
        ("bin_loc", "bin_location", "VARCHAR", "VARCHAR"),
        ("sku", "product_sku", "VARCHAR", "VARCHAR"),
        ("lot_no", "lot_number", "VARCHAR", "VARCHAR"),
        ("serial_no", "serial_number", "VARCHAR", "VARCHAR"),
        ("qty_shipped", "quantity_shipped", "INTEGER", "INTEGER"),
        ("qty_ordered", "quantity_ordered", "INTEGER", "INTEGER"),
        ("qty_received", "quantity_received", "INTEGER", "INTEGER"),
        ("bol_no", "bill_of_lading_number", "VARCHAR", "VARCHAR"),
        ("po_no", "purchase_order_number", "VARCHAR", "VARCHAR"),
        ("asn_id", "asn_id", "VARCHAR", "VARCHAR"),
        ("dock_door", "dock_door", "VARCHAR", "VARCHAR"),
        ("trailer_no", "trailer_number", "VARCHAR", "VARCHAR"),
        ("seal_no", "seal_number", "VARCHAR", "VARCHAR"),
        ("hazmat_flg", "hazmat_flag", "BOOLEAN", "BOOLEAN"),
        ("temp_min", "temperature_min", "DECIMAL", "DECIMAL"),
        ("temp_max", "temperature_max", "DECIMAL", "DECIMAL"),
        ("route_id", "route_id", "VARCHAR", "VARCHAR"),
        ("stop_seq", "stop_sequence", "INTEGER", "INTEGER"),
        ("driver_id", "driver_id", "VARCHAR", "VARCHAR"),
        ("vehicle_id", "vehicle_id", "VARCHAR", "VARCHAR"),
        ("miles", "distance_miles", "DECIMAL", "DECIMAL"),
        ("delay_mins", "delay_minutes", "INTEGER", "INTEGER"),
        ("proof_del_flg", "proof_of_delivery_flag", "BOOLEAN", "BOOLEAN"),
    ],
}


def main() -> None:
    domains = []
    total = 0
    for name, rows in DOMAINS.items():
        cases = []
        seen_src: set[str] = set()
        seen_tgt: set[str] = set()
        for src, tgt, st, tt in rows:
            # Unique sources within a domain (required for bipartite 1:1).
            if src in seen_src:
                continue
            # Unique targets within a domain.
            if tgt in seen_tgt:
                continue
            seen_src.add(src)
            seen_tgt.add(tgt)
            cases.append({
                "source": src,
                "target": tgt,
                "source_type": st,
                "target_type": tt,
                "domain": name,
            })
        domains.append({"name": name, "cases": cases})
        total += len(cases)

    payload = {
        "version": 2,
        "description": (
            "Enterprise mapping golden — domain-batched bipartite eval with types. "
            "Accuracy is measured per domain then aggregated. Not a marketing claim."
        ),
        "min_cases": 200,
        "domains": domains,
        "total_cases": total,
    }
    assert total >= 200, f"only {total} unique cases — need ≥200"
    OUT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {OUT} with {total} cases across {len(domains)} domains")


if __name__ == "__main__":
    main()
