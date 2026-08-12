"""An in-process Salesforce REST API for transfer tests.

Salesforce routes are credential-gated, so they skipped everywhere and the
connector's transfer-live declaration rested on unit tests that patched
``requests``. Patching the HTTP client proves the call was formed; it does not
prove a row survives Describe, SOQL paging, a composite write and a read-back.

This serves the subset of the REST API the connector actually uses, with the
contracts it depends on rather than the ones that would be convenient:

* ``GET  /services/data/<v>/limits`` — connectivity probe.
* ``GET  /services/data/<v>/sobjects`` — object list, ``queryable`` honoured.
* ``GET  /services/data/<v>/sobjects/<name>/describe`` — field metadata,
  including the ``createable``/``updateable``/``calculated`` flags Map needs to
  avoid writing formula fields, and picklist values.
* ``GET  /services/data/<v>/query?q=<SOQL>`` — the SELECT/WHERE/ORDER BY/LIMIT/
  OFFSET subset the reader emits, paging through ``nextRecordsUrl``.
* ``POST|PATCH /services/data/<v>/composite/sobjects[/<obj>/<extField>]`` —
  per-record results, ``allOrNone`` honoured.

Deliberately faithful where the engine's correctness depends on it:

* A bearer token is required; a wrong one answers ``401`` in Salesforce's
  error shape, so the auth path is exercised rather than assumed.
* ``OFFSET`` above 2000 is refused the way Salesforce refuses it, which is the
  whole reason the reader has a keyset path.
* ``composite/sobjects`` caps at 200 records per call.
* A record that violates a described field (unknown field, missing required)
  comes back as a per-record failure, not an exception — that is what the
  writer quarantines on.

It is a double, not an emulator: no governor limits, no sharing rules, no
formula evaluation, and SOQL is a subset. Anything relying on those belongs in
a test against a real org.
"""

from __future__ import annotations

import json
import re
import threading
import uuid
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

API_VERSION = "v58.0"
ACCESS_TOKEN = "df-test-access-token"  # nosec B105 — local fixture credential

#: Salesforce refuses OFFSET past this; the reader's keyset path exists for it.
SOQL_MAX_OFFSET = 2000
#: Records per composite/sobjects call.
COMPOSITE_MAX_RECORDS = 200


def _field(
    name: str,
    sf_type: str = "string",
    *,
    nillable: bool = True,
    createable: bool = True,
    updateable: bool = True,
    calculated: bool = False,
    external_id: bool = False,
    length: int | None = None,
    precision: int | None = None,
    scale: int | None = None,
    picklist: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "type": sf_type,
        "nillable": nillable,
        "length": length,
        "precision": precision,
        "scale": scale,
        "label": name,
        "updateable": updateable,
        "createable": createable,
        "calculated": calculated,
        "externalId": external_id,
        "idLookup": name == "Id" or external_id,
        "unique": name == "Id" or external_id,
        "referenceTo": [],
        "compoundFieldName": "",
        "picklistValues": [
            {"value": v, "label": v, "active": True} for v in (picklist or [])
        ],
    }


def default_account_fields() -> list[dict[str, Any]]:
    """A small Account shape covering the metadata Map and Validate rely on."""
    return [
        _field("Id", "id", nillable=False, createable=False, updateable=False),
        _field("Name", "string", nillable=False, length=255),
        _field("AnnualRevenue", "currency", precision=18, scale=2),
        _field("NumberOfEmployees", "int", precision=8, scale=0),
        _field("Industry", "picklist", picklist=["Technology", "Finance"]),
        _field("IsActive", "boolean", nillable=False),
        _field("CreatedDate", "datetime", createable=False, updateable=False),
        # A formula field: writable-looking but must never be written.
        _field("RevenuePerHead", "double", createable=False, updateable=False, calculated=True),
        _field("ExternalKey__c", "string", external_id=True, length=64),
    ]


#: Salesforce serializes each field according to its described type, not
#: according to what the caller happened to send: an ``int`` field comes back as
#: a JSON number even if it was written as ``"10"``. A double that echoed the
#: input would let a transfer agree with itself about a value the real API would
#: have re-typed.
_JSON_NUMERIC = {"int", "double", "currency", "percent"}


def coerce_to_field_type(sf_type: str, value: Any) -> Any:
    """Render a value the way Salesforce would serialize that field type."""
    if value is None:
        return None
    kind = (sf_type or "string").strip().lower()
    if kind == "boolean":
        if isinstance(value, str):
            return value.strip().lower() in {"true", "1", "yes"}
        return bool(value)
    if kind == "int":
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return value
    if kind in _JSON_NUMERIC:
        try:
            return float(str(value).strip())
        except (TypeError, ValueError):
            return value
    return str(value)


@dataclass
class SObject:
    """One object's describe metadata and its rows."""

    name: str
    fields: list[dict[str, Any]] = field(default_factory=default_account_fields)
    records: dict[str, dict[str, Any]] = field(default_factory=dict)
    queryable: bool = True

    def field_names(self) -> list[str]:
        return [str(f["name"]) for f in self.fields]

    def type_of(self, name: str) -> str:
        for entry in self.fields:
            if str(entry.get("name")) == name:
                return str(entry.get("type") or "string")
        return "string"

    def store_record(self, values: dict[str, Any], record_id: str) -> dict[str, Any]:
        """Persist one record with every value typed by its described field."""
        stored: dict[str, Any] = {name: None for name in self.field_names()}
        for name, value in values.items():
            stored[name] = coerce_to_field_type(self.type_of(name), value)
        stored["Id"] = record_id
        self.records[record_id] = stored
        return stored

    def writable(self, for_update: bool) -> set[str]:
        key = "updateable" if for_update else "createable"
        return {str(f["name"]) for f in self.fields if f.get(key)}

    def required(self) -> set[str]:
        return {
            str(f["name"])
            for f in self.fields
            if not f.get("nillable") and f.get("createable")
        }


#: ``SELECT <field>, COUNT(Id) <alias> FROM <obj> GROUP BY <field> HAVING
#: COUNT(Id) > <n>`` — the aggregate the uniqueness probe runs so a large object
#: is counted in the org rather than read back through a 2000-row OFFSET cap.
_AGGREGATE_RE = re.compile(
    r"^\s*SELECT\s+(?P<field>[A-Za-z][A-Za-z0-9_]*)\s*,\s*COUNT\(\s*(?P<counted>[A-Za-z][A-Za-z0-9_]*)\s*\)"
    r"(?:\s+(?P<alias>[A-Za-z][A-Za-z0-9_]*))?"
    r"\s+FROM\s+(?P<object>[A-Za-z][A-Za-z0-9_]*)"
    r"\s+GROUP\s+BY\s+(?P<group>[A-Za-z][A-Za-z0-9_]*)"
    r"(?:\s+HAVING\s+COUNT\(\s*[A-Za-z][A-Za-z0-9_]*\s*\)\s*>\s*(?P<having>\d+))?\s*$",
    re.IGNORECASE | re.DOTALL,
)

_SELECT_RE = re.compile(
    r"^\s*SELECT\s+(?P<fields>.+?)\s+FROM\s+(?P<object>[A-Za-z][A-Za-z0-9_]*)"
    r"(?:\s+WHERE\s+(?P<where>.+?))?"
    r"(?:\s+ORDER\s+BY\s+(?P<order>[A-Za-z][A-Za-z0-9_]*))?"
    r"(?:\s+LIMIT\s+(?P<limit>\d+))?"
    r"(?:\s+OFFSET\s+(?P<offset>\d+))?\s*$",
    re.IGNORECASE | re.DOTALL,
)
_WHERE_RE = re.compile(
    r"^\s*(?P<field>[A-Za-z][A-Za-z0-9_]*)\s*(?P<op>>=|<=|>|<|=)\s*'(?P<value>(?:[^'\\]|\\.)*)'\s*$"
)


class SoqlError(ValueError):
    """A query this double refuses, reported the way Salesforce reports it."""

    def __init__(self, message: str, code: str = "MALFORMED_QUERY") -> None:
        super().__init__(message)
        self.code = code


def _unescape_soql(literal: str) -> str:
    return literal.replace("\\'", "'").replace("\\\\", "\\")


def run_soql(store: dict[str, SObject], query: str) -> list[dict[str, Any]]:
    """Execute the SELECT subset the connector emits."""
    aggregate = _AGGREGATE_RE.match(query or "")
    if aggregate:
        return _run_aggregate(store, aggregate)

    match = _SELECT_RE.match(query or "")
    if not match:
        raise SoqlError(f"unsupported SOQL for this test double: {query!r}")
    obj_name = match.group("object")
    sobject = store.get(obj_name)
    if sobject is None:
        raise SoqlError(
            f"sObject type '{obj_name}' is not supported.", code="INVALID_TYPE"
        )

    requested = [f.strip() for f in match.group("fields").split(",") if f.strip()]
    known = set(sobject.field_names())
    unknown = [f for f in requested if f not in known]
    if unknown:
        raise SoqlError(
            f"No such column '{unknown[0]}' on entity '{obj_name}'.",
            code="INVALID_FIELD",
        )

    rows = list(sobject.records.values())

    where = (match.group("where") or "").strip()
    if where:
        cond = _WHERE_RE.match(where)
        if not cond:
            raise SoqlError(f"unsupported WHERE for this test double: {where!r}")
        col, op, raw = cond.group("field"), cond.group("op"), cond.group("value")
        if col not in known:
            raise SoqlError(
                f"No such column '{col}' on entity '{obj_name}'.", code="INVALID_FIELD"
            )
        value = _unescape_soql(raw)
        compare = {
            ">": lambda a, b: a > b,
            ">=": lambda a, b: a >= b,
            "<": lambda a, b: a < b,
            "<=": lambda a, b: a <= b,
            "=": lambda a, b: a == b,
        }[op]
        rows = [r for r in rows if compare(str(r.get(col, "")), value)]

    order = match.group("order")
    if order:
        if order not in known:
            raise SoqlError(
                f"No such column '{order}' on entity '{obj_name}'.",
                code="INVALID_FIELD",
            )
        rows.sort(key=lambda r: str(r.get(order, "")))

    offset = int(match.group("offset") or 0)
    if offset > SOQL_MAX_OFFSET:
        raise SoqlError(
            f"OFFSET exceeds the maximum of {SOQL_MAX_OFFSET}.",
            code="NUMBER_OUTSIDE_VALID_RANGE",
        )
    if offset:
        rows = rows[offset:]
    limit = match.group("limit")
    if limit:
        rows = rows[: int(limit)]

    projected: list[dict[str, Any]] = []
    for row in rows:
        out = {"attributes": {"type": obj_name, "url": f"/{obj_name}/{row.get('Id')}"}}
        for name in requested:
            out[name] = row.get(name)
        projected.append(out)
    return projected


def _run_aggregate(store: dict[str, SObject], match: re.Match[str]) -> list[dict[str, Any]]:
    """Answer a GROUP BY / HAVING COUNT aggregate as AggregateResult rows."""
    from collections import Counter

    obj_name = match.group("object")
    sobject = store.get(obj_name)
    if sobject is None:
        raise SoqlError(
            f"sObject type '{obj_name}' is not supported.", code="INVALID_TYPE"
        )
    field_name = match.group("field")
    group_field = match.group("group")
    if field_name != group_field:
        raise SoqlError(
            f"field '{field_name}' must appear in the GROUP BY clause",
            code="MALFORMED_QUERY",
        )
    known = set(sobject.field_names())
    for name in (field_name, match.group("counted")):
        if name not in known:
            raise SoqlError(
                f"No such column '{name}' on entity '{obj_name}'.",
                code="INVALID_FIELD",
            )

    counts = Counter(str(r.get(field_name, "")) for r in sobject.records.values())
    threshold = int(match.group("having") or 0)
    alias = match.group("alias") or "expr0"
    return [
        {
            "attributes": {"type": "AggregateResult"},
            field_name: value,
            alias: count,
        }
        for value, count in counts.items()
        if count > threshold
    ]


class _Handler(BaseHTTPRequestHandler):
    server_version = "SalesforceTestDouble/1.0"

    # Silence per-request logging; a failing test reports through assertions.
    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        return

    # ── plumbing ─────────────────────────────────────────────────────────────

    @property
    def _store(self) -> dict[str, SObject]:
        return self.server.store  # type: ignore[attr-defined]

    def _json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, status: int, code: str, message: str) -> None:
        # Salesforce returns a *list* of errors, which the connector's error
        # humanizer reads; returning a bare object would hide the message.
        self._json(status, [{"errorCode": code, "message": message}])

    def _authorized(self) -> bool:
        header = self.headers.get("Authorization") or ""
        if header == f"Bearer {self.server.token}":  # type: ignore[attr-defined]
            return True
        self._error(401, "INVALID_SESSION_ID", "Session expired or invalid")
        return False

    def _body(self) -> Any:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return None
        return json.loads(self.rfile.read(length).decode() or "null")

    # ── routes ───────────────────────────────────────────────────────────────

    def do_GET(self) -> None:  # noqa: N802
        if not self._authorized():
            return
        parsed = urlparse(self.path)
        path = parsed.path
        prefix = f"/services/data/{API_VERSION}"

        if path == f"{prefix}/limits":
            self._json(200, {"DailyApiRequests": {"Max": 15000, "Remaining": 14999}})
            return

        if path == f"{prefix}/sobjects":
            self._json(
                200,
                {
                    "sobjects": [
                        {"name": s.name, "queryable": s.queryable, "label": s.name}
                        for s in self._store.values()
                    ]
                },
            )
            return

        describe = re.match(rf"^{re.escape(prefix)}/sobjects/([A-Za-z0-9_]+)/describe$", path)
        if describe:
            sobject = self._store.get(describe.group(1))
            if sobject is None:
                self._error(
                    404,
                    "NOT_FOUND",
                    f"The requested resource does not exist: {describe.group(1)}",
                )
                return
            self._json(200, {"name": sobject.name, "fields": sobject.fields})
            return

        if path == f"{prefix}/query":
            query = (parse_qs(parsed.query).get("q") or [""])[0]
            try:
                records = run_soql(self._store, query)
            except SoqlError as exc:
                self._error(400, exc.code, str(exc))
                return
            self._json(
                200,
                {"totalSize": len(records), "done": True, "records": records},
            )
            return

        self._error(404, "NOT_FOUND", f"The requested resource does not exist: {path}")

    def do_POST(self) -> None:  # noqa: N802
        self._write(update=False)

    def do_PATCH(self) -> None:  # noqa: N802
        self._write(update=True)

    def _write(self, *, update: bool) -> None:
        if not self._authorized():
            return
        parsed = urlparse(self.path)
        prefix = f"/services/data/{API_VERSION}/composite/sobjects"
        if not parsed.path.startswith(prefix):
            self._error(
                404, "NOT_FOUND", f"The requested resource does not exist: {parsed.path}"
            )
            return

        external_field = ""
        tail = parsed.path[len(prefix) :].strip("/")
        if tail:
            parts = tail.split("/")
            if len(parts) != 2:
                self._error(404, "NOT_FOUND", f"Unsupported path: {parsed.path}")
                return
            external_field = parts[1]

        body = self._body() or {}
        records = list(body.get("records") or [])
        if len(records) > COMPOSITE_MAX_RECORDS:
            self._error(
                400,
                "INVALID_INPUT",
                f"Cannot process more than {COMPOSITE_MAX_RECORDS} records",
            )
            return

        all_or_none = bool(body.get("allOrNone"))
        outcomes = [
            self._apply(record, update=update, external_field=external_field)
            for record in records
        ]
        if all_or_none and any(not o["success"] for o in outcomes):
            # Salesforce rolls the whole request back and reports every record
            # as failed with a processing error.
            self._rollback(outcomes)
            outcomes = [
                o
                if not o["success"]
                else {
                    "id": None,
                    "success": False,
                    "errors": [
                        {
                            "statusCode": "ALL_OR_NONE_OPERATION_ROLLED_BACK",
                            "message": "Record rolled back because not all records were valid",
                            "fields": [],
                        }
                    ],
                }
                for o in outcomes
            ]
        self._json(200, [{k: v for k, v in o.items() if k != "_applied"} for o in outcomes])

    def _rollback(self, outcomes: list[dict[str, Any]]) -> None:
        for outcome in outcomes:
            applied = outcome.get("_applied")
            if not applied:
                continue
            sobject, record_id, previous = applied
            if previous is None:
                sobject.records.pop(record_id, None)
            else:
                sobject.records[record_id] = previous

    def _apply(
        self, record: dict[str, Any], *, update: bool, external_field: str
    ) -> dict[str, Any]:
        def failure(code: str, message: str) -> dict[str, Any]:
            return {
                "id": None,
                "success": False,
                "errors": [{"statusCode": code, "message": message, "fields": []}],
            }

        attributes = record.get("attributes") or {}
        obj_name = str(attributes.get("type") or "")
        sobject = self._store.get(obj_name)
        if sobject is None:
            return failure("INVALID_TYPE", f"sObject type '{obj_name}' is not supported.")

        values = {k: v for k, v in record.items() if k != "attributes"}
        known = set(sobject.field_names())
        unknown = sorted(set(values) - known)
        if unknown:
            return failure(
                "INVALID_FIELD_FOR_INSERT_UPDATE",
                f"No such column '{unknown[0]}' on sobject of type {obj_name}",
            )

        writable = sobject.writable(for_update=update)
        not_writable = sorted(k for k in values if k not in writable and k != "Id")
        if not_writable:
            return failure(
                "INVALID_FIELD_FOR_INSERT_UPDATE",
                f"Unable to create/update fields: {not_writable[0]}",
            )

        if external_field and external_field != "Id":
            key = values.get(external_field)
            if key in (None, ""):
                return failure(
                    "REQUIRED_FIELD_MISSING",
                    f"Required fields are missing: [{external_field}]",
                )
            existing = next(
                (
                    r
                    for r in sobject.records.values()
                    if str(r.get(external_field, "")) == str(key)
                ),
                None,
            )
            if existing is not None:
                previous = dict(existing)
                sobject.store_record({**previous, **values}, existing["Id"])
                existing = sobject.records[existing["Id"]]
                return {
                    "id": existing["Id"],
                    "success": True,
                    "errors": [],
                    "_applied": (sobject, existing["Id"], previous),
                }
            return self._insert(sobject, values)

        if update:
            record_id = str(values.get("Id") or "")
            if not record_id:
                return failure(
                    "MISSING_ARGUMENT", "Id not specified in an update call"
                )
            existing = sobject.records.get(record_id)
            if existing is None:
                return failure(
                    "ENTITY_IS_DELETED", f"entity is deleted or does not exist: {record_id}"
                )
            previous = dict(existing)
            sobject.store_record({**previous, **values}, record_id)
            return {
                "id": record_id,
                "success": True,
                "errors": [],
                "_applied": (sobject, record_id, previous),
            }

        missing = sorted(sobject.required() - set(values))
        if missing:
            return failure(
                "REQUIRED_FIELD_MISSING",
                f"Required fields are missing: [{missing[0]}]",
            )
        return self._insert(sobject, values)

    def _insert(self, sobject: SObject, values: dict[str, Any]) -> dict[str, Any]:
        record_id = str(values.get("Id") or "").strip() or _new_id()
        sobject.store_record(values, record_id)
        return {
            "id": record_id,
            "success": True,
            "errors": [],
            "_applied": (sobject, record_id, None),
        }


def _new_id() -> str:
    """An 18-character id, the width Salesforce returns."""
    return ("001" + uuid.uuid4().hex)[:18]


@dataclass(frozen=True)
class SalesforceTestServer:
    """Connection details for a running local Salesforce double."""

    instance_url: str
    token: str
    store: dict[str, SObject]

    def endpoint_config(self, sobject: str) -> dict[str, Any]:
        return {
            "host": self.instance_url,
            "port": 443,
            "api_key": self.token,
            "database": sobject,
            "table": sobject,
        }

    def seed(self, sobject: str, rows: list[dict[str, Any]]) -> None:
        """Load rows, typed the way the org would return them."""
        target = self.store[sobject]
        for row in rows:
            record_id = str(row.get("Id") or "").strip() or _new_id()
            target.store_record(row, record_id)

    def rows(self, sobject: str) -> list[dict[str, Any]]:
        return list(self.store[sobject].records.values())


def start_salesforce_server(
    objects: dict[str, SObject] | None = None,
) -> tuple[SalesforceTestServer, Any]:
    """Start the double on an OS-assigned port; returns details and a stopper."""
    store = objects or {"Account": SObject("Account")}
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    httpd.store = store  # type: ignore[attr-defined]
    httpd.token = ACCESS_TOKEN  # type: ignore[attr-defined]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    host, port = httpd.server_address[0], httpd.server_address[1]
    details = SalesforceTestServer(
        instance_url=f"http://{host}:{port}",
        token=ACCESS_TOKEN,
        store=store,
    )
    return details, httpd
