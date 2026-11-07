"""Jobs → Studio needs a job's route address to match an inline endpoint to a saved connector.

Stored connection strings are ciphertext; the API projects only type/host/port/database.
"""
from src.transfer.models import _encrypt_endpoint_secrets, sanitize_job_for_api


def _job(source: dict, destination: dict) -> dict:
    return {
        "_id": "j1",
        "transfer_request": {
            "source": _encrypt_endpoint_secrets(source),
            "destination": _encrypt_endpoint_secrets(destination),
        },
    }


def test_inline_encrypted_connection_string_projects_its_address_without_secrets():
    out = sanitize_job_for_api(_job(
        {"kind": "database", "format": "postgresql",
         "connection_string": "postgresql://app:s3cr3t-pw@DB.Internal:6543/shop?sslmode=prefer", "table": "orders"},
        {"kind": "file_export", "format": "csv"},
    ))
    src = out["transfer_request"]["source"]
    assert src["address"] == {"type": "postgresql", "host": "db.internal", "port": 6543, "database": "shop"}
    assert "s3cr3t-pw" not in repr(out)
    assert "enc:v1:" not in repr(src["address"])


def test_explicit_fields_win_and_driver_suffix_is_dropped():
    out = sanitize_job_for_api(_job(
        {"kind": "database", "host": "h1", "port": 3306, "database": "crm", "password": "pw-xyz",
         "connection_string": "mysql+pymysql://u:pw-xyz@other:1/x"},
        {"kind": "database", "connection_string": "mongodb://u:pw@m1:27017,m2:27017/app?replicaSet=rs0"},
    ))
    tr = out["transfer_request"]
    assert tr["source"]["address"] == {"type": "mysql", "host": "h1", "port": 3306, "database": "crm"}
    assert tr["destination"]["address"] == {"type": "mongodb", "host": "m1", "port": 27017, "database": "app"}
    assert "pw-xyz" not in repr(out)


def test_file_endpoints_get_an_empty_address():
    out = sanitize_job_for_api(_job({"kind": "file", "format": "csv"}, {"kind": "file_export", "format": "json"}))
    assert out["transfer_request"]["source"]["address"]["host"] == ""
