"""
DataTransfer.space — Industry Schema Templates

Pre-defined schema patterns for common industry data formats.
"""

from __future__ import annotations

INDUSTRY_SCHEMAS: dict[str, dict] = {
    "logistics": {
        "name": "Logistics & Supply Chain",
        "description": "Shipping, warehousing, and supply chain data",
        "columns": {
            "shipment_id": {"type": "string", "semantic": "Shipment ID", "required": True},
            "tracking_number": {"type": "string", "semantic": "Shipment ID", "required": True},
            "origin_warehouse": {"type": "string", "semantic": "Warehouse", "required": True},
            "destination_address": {"type": "string", "semantic": "Street Address", "required": True},
            "carrier": {"type": "string", "semantic": "Carrier", "required": True},
            "ship_date": {"type": "datetime", "semantic": "Timestamp", "required": True},
            "delivery_date": {"type": "datetime", "semantic": "Timestamp", "required": False},
            "weight": {"type": "decimal", "semantic": "Weight", "required": True},
            "status": {"type": "string", "semantic": "Status", "required": True},
            "freight_cost": {"type": "decimal", "semantic": "Currency Amount", "required": False},
        },
    },
    "finance": {
        "name": "Financial Services",
        "description": "Banking, payments, and financial transactions",
        "columns": {
            "transaction_id": {"type": "string", "semantic": "Order ID", "required": True},
            "account_number": {"type": "string", "semantic": "Bank Account Number", "required": True, "pii": True},
            "amount": {"type": "decimal", "semantic": "Currency Amount", "required": True},
            "currency": {"type": "string", "semantic": "Currency Code", "required": True},
            "transaction_date": {"type": "datetime", "semantic": "Timestamp", "required": True},
            "transaction_type": {"type": "string", "semantic": "Category", "required": True},
            "description": {"type": "string", "semantic": "Description", "required": False},
            "balance": {"type": "decimal", "semantic": "Currency Amount", "required": False},
            "routing_number": {"type": "string", "semantic": "Routing Number", "required": False, "pii": True},
        },
    },
    "healthcare": {
        "name": "Healthcare & Medical",
        "description": "Patient records, clinical data, and billing",
        "columns": {
            "patient_id": {"type": "string", "semantic": "Medical Record Number", "required": True, "pii": True},
            "mrn": {"type": "string", "semantic": "Medical Record Number", "required": True, "pii": True},
            "first_name": {"type": "string", "semantic": "First Name", "required": True, "pii": True},
            "last_name": {"type": "string", "semantic": "Last Name", "required": True, "pii": True},
            "date_of_birth": {"type": "date", "semantic": "Date of Birth", "required": True, "pii": True},
            "diagnosis_code": {"type": "string", "semantic": "Diagnosis Code", "required": False, "pii": True},
            "procedure_code": {"type": "string", "semantic": "Procedure", "required": False},
            "provider_npi": {"type": "string", "semantic": "Provider", "required": False},
            "insurance_id": {"type": "string", "semantic": "Health Insurance ID", "required": False, "pii": True},
            "visit_date": {"type": "datetime", "semantic": "Timestamp", "required": True},
        },
    },
    "retail": {
        "name": "Retail & E-commerce",
        "description": "Product catalog, orders, and customer data",
        "columns": {
            "order_id": {"type": "string", "semantic": "Order ID", "required": True},
            "customer_id": {"type": "string", "semantic": "Customer ID", "required": True},
            "product_sku": {"type": "string", "semantic": "Product ID", "required": True},
            "quantity": {"type": "integer", "semantic": "Quantity", "required": True},
            "unit_price": {"type": "decimal", "semantic": "Currency Amount", "required": True},
            "total_amount": {"type": "decimal", "semantic": "Currency Amount", "required": True},
            "order_date": {"type": "datetime", "semantic": "Timestamp", "required": True},
            "shipping_address": {"type": "string", "semantic": "Street Address", "required": True, "pii": True},
            "payment_status": {"type": "string", "semantic": "Status", "required": True},
            "channel": {"type": "string", "semantic": "Channel", "required": False},
        },
    },
    "hr": {
        "name": "Human Resources",
        "description": "Employee records and payroll data",
        "columns": {
            "employee_id": {"type": "string", "semantic": "Employee ID", "required": True},
            "first_name": {"type": "string", "semantic": "First Name", "required": True, "pii": True},
            "last_name": {"type": "string", "semantic": "Last Name", "required": True, "pii": True},
            "email": {"type": "string", "semantic": "Email Address", "required": True, "pii": True},
            "department": {"type": "string", "semantic": "Department", "required": True},
            "job_title": {"type": "string", "semantic": "Job Title", "required": True},
            "hire_date": {"type": "date", "semantic": "Hire Date", "required": True},
            "salary": {"type": "decimal", "semantic": "Salary", "required": False, "pii": True},
            "manager_id": {"type": "string", "semantic": "Employee ID", "required": False},
            "status": {"type": "string", "semantic": "Status", "required": True},
        },
    },
    "manufacturing": {
        "name": "Manufacturing",
        "description": "Production, quality, and inventory data",
        "columns": {
            "work_order_id": {"type": "string", "semantic": "Work Order", "required": True},
            "batch_number": {"type": "string", "semantic": "Batch", "required": True},
            "product_id": {"type": "string", "semantic": "Product ID", "required": True},
            "quantity_produced": {"type": "integer", "semantic": "Quantity", "required": True},
            "production_date": {"type": "datetime", "semantic": "Timestamp", "required": True},
            "quality_score": {"type": "decimal", "semantic": "Score", "required": False},
            "defect_count": {"type": "integer", "semantic": "Quantity", "required": False},
            "line_id": {"type": "string", "semantic": "Location", "required": True},
            "operator_id": {"type": "string", "semantic": "Employee ID", "required": False},
        },
    },
    "insurance": {
        "name": "Insurance",
        "description": "Policy, claims, and underwriting data",
        "columns": {
            "policy_id": {"type": "string", "semantic": "Policy ID", "required": True},
            "policyholder_name": {"type": "string", "semantic": "Full Name", "required": True, "pii": True},
            "premium_amount": {"type": "decimal", "semantic": "Currency Amount", "required": True},
            "coverage_type": {"type": "string", "semantic": "Category", "required": True},
            "effective_date": {"type": "date", "semantic": "Date", "required": True},
            "expiration_date": {"type": "date", "semantic": "Date", "required": True},
            "claim_id": {"type": "string", "semantic": "Order ID", "required": False},
            "claim_amount": {"type": "decimal", "semantic": "Currency Amount", "required": False},
            "risk_score": {"type": "decimal", "semantic": "Credit Score", "required": False},
        },
    },
    "telecom": {
        "name": "Telecommunications",
        "description": "Subscriber, usage, and billing data",
        "columns": {
            "subscriber_id": {"type": "string", "semantic": "Customer ID", "required": True},
            "phone_number": {"type": "string", "semantic": "Phone Number", "required": True, "pii": True},
            "plan_id": {"type": "string", "semantic": "Product ID", "required": True},
            "data_usage_gb": {"type": "decimal", "semantic": "Quantity", "required": False},
            "voice_minutes": {"type": "integer", "semantic": "Duration", "required": False},
            "billing_amount": {"type": "decimal", "semantic": "Currency Amount", "required": True},
            "activation_date": {"type": "datetime", "semantic": "Timestamp", "required": True},
            "device_imei": {"type": "string", "semantic": "Serial Number", "required": False},
        },
    },
    "education": {
        "name": "Education",
        "description": "Student records and academic data",
        "columns": {
            "student_id": {"type": "string", "semantic": "Student ID", "required": True},
            "first_name": {"type": "string", "semantic": "First Name", "required": True, "pii": True},
            "last_name": {"type": "string", "semantic": "Last Name", "required": True, "pii": True},
            "course_code": {"type": "string", "semantic": "Course ID", "required": True},
            "grade": {"type": "string", "semantic": "Grade", "required": False},
            "gpa": {"type": "decimal", "semantic": "Score", "required": False},
            "enrollment_date": {"type": "date", "semantic": "Enrollment Date", "required": True},
            "credits": {"type": "integer", "semantic": "Quantity", "required": False},
        },
    },
    "real_estate": {
        "name": "Real Estate",
        "description": "Property listings and transaction data",
        "columns": {
            "property_id": {"type": "string", "semantic": "Property ID", "required": True},
            "address": {"type": "string", "semantic": "Street Address", "required": True, "pii": True},
            "list_price": {"type": "decimal", "semantic": "Currency Amount", "required": True},
            "square_feet": {"type": "integer", "semantic": "Square Feet", "required": True},
            "bedrooms": {"type": "integer", "semantic": "Bedrooms", "required": True},
            "bathrooms": {"type": "decimal", "semantic": "Bathrooms", "required": True},
            "listing_date": {"type": "date", "semantic": "Date", "required": True},
            "agent_id": {"type": "string", "semantic": "Employee ID", "required": False},
            "status": {"type": "string", "semantic": "Status", "required": True},
        },
    },
}


def get_industry_schema(industry: str) -> dict | None:
    """Get schema template for an industry."""
    return INDUSTRY_SCHEMAS.get(industry.lower())


def list_industries() -> list[str]:
    """List all supported industries."""
    return list(INDUSTRY_SCHEMAS.keys())
