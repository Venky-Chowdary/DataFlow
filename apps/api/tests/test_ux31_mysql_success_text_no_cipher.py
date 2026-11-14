"""UX-31: a MySQL connection test's success text printed the TLS cipher suite."""

from connectors.mysql_tls import describe_session_encryption


def test_success_text_names_protocol_and_trust_not_the_cipher():
    text = describe_session_encryption(
        {"encrypted": True, "verified": False, "version": "TLSv1.3", "cipher": "TLS_AES_256_GCM_SHA384", "requested": ""}
    )
    assert text.startswith("session encrypted (TLSv1.3, certificate unverified)")
    assert "TLS_AES_256_GCM_SHA384" not in text


def test_unencrypted_session_still_says_so():
    assert describe_session_encryption({"encrypted": False, "requested": ""}).startswith("session NOT encrypted")
