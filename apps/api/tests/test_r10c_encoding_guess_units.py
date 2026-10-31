"""Unit pins: which guessed encodings hold, which are refused (e2e: test_r10c_guessed_encoding_e2e)."""

from __future__ import annotations

import pytest

from services.source_encoding_guess import apply_encoding_gate, assess_text_encoding

_RU = "id,name\n1,Привет мир\n2,Москва Санкт-Петербург\n"
_WEST = ("id,name\n1,café\n2,€ 12,50 “quoted” – …\n3,Grüße naïve\n4,1º andar 50 m²\n"
         "5,cœur Œuvre\n6,¿Qué? ¡Hola!\n7,Où est la bibliothèque? Très près, à côté.\n")


@pytest.mark.parametrize(
    ("text", "codec", "implausible"),
    [(_WEST, "cp1252", False), (_RU, "cp1251", True), (_RU, "koi8_r", True),
     ("id,n\n1,Καλημέρα κόσμε Αθήνα\n", "cp1253", True),
     ("id,n\n1,Zażółć gęślą jaźń\n", "cp1250", True)],
)
def test_guess_holds_only_for_windows_1252_text(text, codec, implausible):
    report = assess_text_encoding(text.encode(codec))
    assert report["encoding_guessed"] is True
    assert report["implausible"] is implausible, report


def test_c1_control_and_undefined_bytes_are_refused():
    assert assess_text_encoding(b"id,n\n1,a\x81b\n")["implausible"]  # latin-1 fallback
    assert not assess_text_encoding("id,n\n1,é\n".encode("utf-8"))["encoding_guessed"]


def test_declared_encoding_is_never_a_guess():
    report = assess_text_encoding(_RU.encode("cp1251"), "cp1251")
    assert report == {**report, "encoding_guessed": False, "implausible": False}


def test_gate_warns_once_or_blocks():
    out = {"gates": [], "blockers": [], "passed": True}
    apply_encoding_gate(out, assess_text_encoding(_WEST.encode("cp1252")))
    assert [g["status"] for g in out["gates"]] == ["warn"] and out["passed"]
    assert "café" in out["warnings"][0]
    out = {"gates": [], "blockers": [], "passed": True}
    apply_encoding_gate(out, assess_text_encoding(_RU.encode("cp1251")))
    assert [g["status"] for g in out["gates"]] == ["block"] and not out["passed"]
