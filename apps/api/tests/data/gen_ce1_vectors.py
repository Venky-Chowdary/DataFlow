"""Build tests/data/ce1_vectors.json. Expected bytes are written by hand here
(digests computed from the hand-written canonical text), not by ce1.py."""
import hashlib, json, sys
def D(text_or_hex, raw=False):
    b = bytes.fromhex(text_or_hex) if raw else text_or_hex.encode("utf-8")
    return "D" + hashlib.sha256(b).hexdigest()
def V(t):
    return "V" + str(len(t.encode("utf-8"))) + ":" + t
def X(c, t):
    return "X" + c + ":" + str(len(t.encode("utf-8"))) + ":" + t
cases = []
def add(name, kind, value, expected, side="destination", **spec):
    cases.append({"name": name, "kind": kind, "value": value, "side": side, "spec": spec, "expected": expected})
PG = {"dest_engine": "postgresql"}
add("null", "integer", None, "N", dest_type="bigint", **PG)
add("int zero", "integer", {"$i": "0"}, V("0"), dest_type="bigint", **PG)
add("int negative", "integer", {"$i": "-42"}, V("-42"), dest_type="bigint", **PG)
add("int big", "integer", {"$i": "9223372036854775807"}, V("9223372036854775807"), dest_type="bigint", **PG)
add("int bool true", "integer", True, V("1"), dest_type="bigint", **PG)
add("int as text (sqlite)", "integer", "5", X("text", "5"), dest_type="INTEGER", dest_engine="sqlite")
add("bool true", "boolean", True, V("1"), dest_type="boolean", **PG)
add("bool false", "boolean", False, V("0"), dest_type="boolean", **PG)
add("bool carrier int 2", "boolean", {"$i": "2"}, V("2"), dest_type="boolean", **PG)
add("decimal trailing zeros", "decimal", {"$d": "12.3400"}, V("12.34"), dest_type="numeric(12,4)", **PG)
add("decimal integral", "decimal", {"$d": "1E+2"}, V("100"), dest_type="numeric", **PG)
add("decimal negative zero", "decimal", {"$d": "-0.00"}, V("0"), dest_type="numeric(12,2)", **PG)
add("decimal small", "decimal", {"$d": "0.0000000150"}, V("0.000000015"), dest_type="numeric", **PG)
add("decimal negative", "decimal", {"$d": "-12.5"}, V("-12.5"), dest_type="numeric", **PG)
add("decimal NaN", "decimal", {"$d": "NaN"}, V("NaN"), dest_type="numeric", **PG)
add("decimal Infinity", "decimal", {"$d": "-Infinity"}, V("-Infinity"), dest_type="numeric", **PG)
add("decimal int value", "decimal", {"$i": "7"}, V("7"), dest_type="numeric", **PG)
add("decimal as REAL (sqlite)", "decimal", {"$f": "12.5"}, X("real", "12.5"), dest_type="NUMERIC(10,2)", dest_engine="sqlite")
add("text ascii", "text", "abc", V("abc"), dest_type="varchar(20)", **PG)
add("text empty", "text", "", V(""), dest_type="varchar(20)", **PG)
add("text trailing space kept", "text", "ab ", V("ab "), dest_type="varchar(20)", **PG)
add("text supplementary", "text", "a\U0001F600b", V("a\U0001F600b"), dest_type="varchar(20)", **PG)
add("text combining mark not normalised", "text", "é", V("é"), dest_type="varchar(20)", **PG)
add("text case kept", "text", "ABC", V("ABC"), dest_type="varchar(20)", **PG)
add("text digest width", "text", "hello", D("hello"), dest_type="text", width="digest", **PG)
add("text empty is null (oracle)", "text", "", "N", dest_type="VARCHAR2(20)", dest_engine="oracle", empty_is_null=True)
add("text int storage class (sqlite)", "text", {"$i": "5"}, X("integer", "5"), dest_type="TEXT", dest_engine="sqlite")
add("char blank pad trimmed", "char", "ab   ", V("ab"), dest_type="char(5)", **PG)
add("char leading space kept", "char", " ab", V(" ab"), dest_type="char(5)", **PG)
add("date epoch", "date", {"$date": "1970-01-01"}, V("0"), dest_type="date", **PG)
add("date pre-epoch", "date", {"$date": "1969-12-31"}, V("-1"), dest_type="date", **PG)
add("date leap day", "date", {"$date": "2024-02-29"}, V("19782"), dest_type="date", **PG)
add("date infinity", "date", "infinity", V("+inf"), dest_type="date", **PG)
add("timestamp us", "timestamp", {"$dt": "2024-01-01T00:00:00.123456"}, V("1704067200123456"), dest_type="timestamp(6)", **PG)
add("timestamp pre-epoch", "timestamp", {"$dt": "1969-12-31T23:59:59.999999"}, V("-1"), dest_type="timestamp(6)", **PG)
add("timestamp ms exact", "timestamp", {"$dt": "2024-01-01T00:00:00.123000"}, V("1704067200123"), dest_type="timestamp(3)", digits=3, **PG)
add("timestamp ms finer than precision", "timestamp", {"$dt": "2024-01-01T00:00:00.123456"}, X("us", "1704067200123456"), dest_type="timestamp(3)", digits=3, **PG)
add("timestamp ms projected half up", "timestamp", {"$dt": "2024-01-01T00:00:00.123500"}, V("1704067200124"), side="source", dest_type="timestamp(3)", digits=3, **PG)
add("timestamp pre-2000 projected for PostgreSQL rounds half away from 2000", "timestamp", {"$dt": "1969-12-31T23:59:59.999500"}, V("-1"), side="source", dest_type="timestamp(3)", digits=3, **PG)
add("timestamp pre-epoch projected for MySQL rounds half up", "timestamp", {"$dt": "1969-12-31T23:59:59.999500"}, V("0"), side="source", dest_type="DATETIME(3)", digits=3, dest_engine="mysql")
add("timestamp 2000 boundary projected for PostgreSQL", "timestamp", {"$dt": "2000-01-01T00:00:00.000500"}, V("946684800001"), side="source", dest_type="timestamp(3)", digits=3, **PG)
add("timestamptz to utc", "timestamptz", {"$dt": "2024-01-01T02:00:00+02:00"}, V("1704067200000000"), dest_type="timestamp with time zone", **PG)
add("timestamp infinity", "timestamp", "-infinity", V("-inf"), dest_type="timestamp", **PG)
add("time us", "time", {"$t": "10:00:00.500000"}, V("36000500000"), dest_type="time", **PG)
add("time projected no wrap", "time", {"$t": "23:59:59.999900"}, V("86399"), side="source", dest_type="time(0)", digits=0, **PG)
add("uuid object", "uuid", {"$u": "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"}, V("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), dest_type="uuid", **PG)
add("uuid text upper", "uuid", "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11", V("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"), dest_type="uuid", **PG)
add("uuid braces not canonical", "uuid", "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}", X("text", "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"), dest_type="uuid", **PG)
add("binary", "binary", {"$b": "AAEC/w=="}, D("000102ff", raw=True), dest_type="bytea", width="digest", **PG)
add("binary empty", "binary", {"$b": ""}, D("", raw=True), dest_type="bytea", width="digest", **PG)
add("native real (sqlite)", "native", {"$f": "-0.0"}, X("real", "-0.0"), dest_type="REAL", dest_engine="sqlite")
add("decimal fit exact", "decimal", {"$d": "12.3400"}, V("12.34"), side="source", dest_type="numeric(10,2)",
    decimal_fit={"precision": 10, "scale": 2, "dest_db": "postgresql", "actions": {"scale": "null", "int": "hold", "both": "hold", "nan": "null", "inf": "hold"}}, **PG)
add("decimal fit scale overflow nulls", "decimal", {"$d": "12.345"}, "N", side="source", dest_type="numeric(10,2)",
    decimal_fit={"precision": 10, "scale": 2, "dest_db": "postgresql", "actions": {"scale": "null", "int": "hold", "both": "hold", "nan": "null", "inf": "hold"}}, **PG)
add("decimal fit int overflow held", "decimal", {"$d": "123456789"}, "HELD", side="source", dest_type="numeric(10,2)",
    decimal_fit={"precision": 10, "scale": 2, "dest_db": "postgresql", "actions": {"scale": "null", "int": "hold", "both": "hold", "nan": "null", "inf": "hold"}}, **PG)
add("decimal fit zero into numeric(2,2) held (writer quirk)", "decimal", {"$d": "0"}, "HELD", side="source", dest_type="numeric(2,2)",
    decimal_fit={"precision": 2, "scale": 2, "dest_db": "postgresql", "actions": {"scale": "hold", "int": "hold", "both": "hold", "nan": "hold", "inf": "hold"}}, **PG)
add("json null on scalar column (D144)", "text", {"$json_null": True}, "N", dest_type="varchar(20)", dest_engine="s3")
add("json null on document column (D144)", "native", {"$json_null": True}, V("null"), dest_type="json", dest_engine="s3")
rows = [
    {"name": "row: (1, NULL, 'a')", "kinds": ["integer", "text", "text"], "values": [{"$i": "1"}, None, "a"],
     "expected": "V1:1" + "N" + "V1:a"},
    {"name": "row: field boundary cannot be forged", "kinds": ["text", "text"], "values": ["a", "V1:b"],
     "expected": "V1:a" + "V4:V1:b"},
]
for r in rows:
    raw = r["expected"].encode("utf-8")
    h = hashlib.sha256(raw).digest()
    r["sha256"] = h.hex()
    r["a"] = str(int.from_bytes(h[0:8], "big") & ((1 << 60) - 1))
    r["b"] = str(int.from_bytes(h[8:16], "big") & ((1 << 60) - 1))
out = {"_comment": "CE1 golden vectors. Expected fields are written by hand in "
       "tests/data/gen_ce1_vectors.py (digests from the hand-written canonical bytes), never "
       "produced by ce1.py. 'HELD' means the write contract held the row out.",
       "ce_version": "CE1", "fields": cases, "rows": rows}
json.dump(out, open(sys.argv[1], "w"), indent=1, ensure_ascii=False)
print(len(cases), len(rows))
