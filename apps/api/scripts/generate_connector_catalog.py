"""Generate connector catalog entries for Datawrap (certified + roadmap tiles).

``data/connector_catalog.json`` is curated on top of this list (statuses and
descriptions earned by certification, hosted twins, file tiles). Running the
script merges: curated entries win, a tile this list adds is inserted next to
its neighbour, and a duplicate is dropped — never renamed. The generator used
to give a colliding id a random suffix (``snowflake_b1fcb2``), which put 81
duplicate tiles in the catalog (QA round 11).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

try:
    from services.value_serializer import json_default
except ImportError:
    json_default = str

OUTPUT = Path(__file__).resolve().parents[1] / "data" / "connector_catalog.json"

# Core live connectors
LIVE = [
    ("postgresql", "PostgreSQL", "database", "Full read/write with live schema probe"),
    ("snowflake", "Snowflake", "warehouse", "Cloud data warehouse with batched load"),
    ("csv", "CSV / TSV", "file", "Upload, parse, infer schema, semantic mapping"),
    ("json", "JSON", "file", "Nested document flattening and array expansion"),
    ("adls", "Azure Blob Storage / ADLS Gen2", "cloud_storage", "Azure Blob and ADLS Gen2 read/write with connection string or account key"),
]

BETA = [
    ("excel", "Excel", "file", "Multi-sheet workbooks with type detection"),
    ("mysql", "MySQL", "database", "Popular open-source OLTP database"),
    ("mongodb", "MongoDB", "database", "Document store with nested schema inference"),
]

# Enterprise databases
DATABASES = [
    "SQL Server", "Oracle", "DB2", "Sybase ASE", "Informix", "Teradata", "Netezza",
    "Greenplum", "Vertica", "SingleStore", "CockroachDB", "MariaDB",
    ("amazon_aurora", "Amazon Aurora MySQL"), "Amazon Aurora PostgreSQL",
    "Amazon RDS PostgreSQL", "Amazon RDS MySQL", "Amazon RDS SQL Server", "Amazon RDS Oracle",
    "Azure SQL Database", "Azure Database for PostgreSQL", "Azure Database for MySQL",
    "Google Cloud SQL PostgreSQL", "Google Cloud SQL MySQL", "Google Cloud SQL SQL Server",
    "AlloyDB", "TimescaleDB", "InfluxDB", "Cassandra", "ScyllaDB", "Couchbase",
    "Firebase Realtime DB", "DynamoDB", "DocumentDB", "Neptune", "SAP HANA",
    "SAP ASE", "SAP IQ", "Exasol", "Firebird", "H2", "SQLite", "ClickHouse",
    "Druid", "Pinot", "QuestDB", "RisingWave", "Materialize", "YugabyteDB",
    "Vitess", "TiDB", "OceanBase", "PolarDB", "GaussDB", "GoldenDB",
]

WAREHOUSES = [
    "BigQuery", "Redshift", "Databricks", "Synapse Analytics", "Snowflake",
    "Firebolt", "Dremio", "Presto", "Trino", "Apache Hive", "Apache Impala",
    "Google BigLake", "Azure Synapse Dedicated", "Azure Synapse Serverless",
    "Amazon Athena", "Amazon EMR", "Cloudera Data Platform", "IBM Db2 Warehouse",
    "Oracle Autonomous Warehouse", "SAP BW/4HANA", "Yellowbrick", "Actian Avalanche",
]

SAAS = [
    "Salesforce", "HubSpot", "Marketo", "Pardot", "Zendesk", "Intercom", "Freshdesk",
    "ServiceNow", "Jira", "Confluence", "Asana", "Monday.com", "Trello", "Notion",
    "Slack", "Microsoft Teams", "Zoom", "Google Analytics", "Google Ads", "Facebook Ads",
    "LinkedIn Ads", "Twitter Ads", "Stripe", "PayPal", "Square", "QuickBooks",
    "Xero", "NetSuite", "Workday", "BambooHR", "Greenhouse", "Lever", "ADP",
    "Shopify", "Magento", "WooCommerce", "BigCommerce", "Amazon Seller Central",
    "ShipStation", "FedEx", "UPS", "DHL", "Twilio", "SendGrid", "Mailchimp",
    "Klaviyo", "Segment", "Mixpanel", "Amplitude", "Pendo", "Looker", "Tableau",
    "Power BI", "Mode Analytics", "Datadog", "New Relic", "Splunk", "PagerDuty",
    "Okta", "Auth0", "OneLogin", "GitHub", "GitLab", "Bitbucket", "CircleCI",
    "Jenkins", "PagerDuty", "Statuspage", "Chargebee", "Recurly", "Zuora",
    "DocuSign", "Adobe Sign", "Box", "Dropbox", "Google Drive", "OneDrive",
    "SharePoint", "Airtable", "Smartsheet", "Typeform", "SurveyMonkey", "Qualtrics",
    "Braze", "Iterable", "Customer.io", "Delighted", "Gainsight", "Gong", "Outreach",
    "Salesloft", "Clari", "Copper CRM", "Pipedrive", "Zoho CRM", "Dynamics 365",
    "SAP SuccessFactors", "Oracle HCM", "UKG Pro", "Ceridian Dayforce", "Rippling",
    "Gusto", "Deel", "Remote.com", "Lattice", "Culture Amp", "15Five",
]

CLOUD_STORAGE = [
    "Amazon S3", "Google Cloud Storage", "Azure Blob Storage", "Azure Data Lake",
    "Wasabi", "Backblaze B2", "MinIO", "DigitalOcean Spaces", "IBM Cloud Object Storage",
    "Oracle Cloud Object Storage", "Alibaba OSS", "Cloudflare R2",
]

APIS = [
    "REST OpenAPI", "GraphQL", "SOAP", "OData", "gRPC", "Webhook", "SFTP", "FTP",
    "Kafka", "Amazon Kinesis", "Google Pub/Sub", "Azure Event Hubs", "RabbitMQ",
    "Apache Pulsar", "MQTT", "Apache NiFi",
]

MARKETING = [
    "Google Search Console", "Bing Webmaster", "Semrush", "Ahrefs", "Moz",
    "Sprout Social", "Hootsuite", "Buffer", "Canva", "Figma", "Adobe Analytics",
]

FINANCE = [
    "Bloomberg", "Refinitiv", "FactSet", "Morningstar", "Plaid", "Yodlee",
    "Finicity", "Adyen", "Braintree", "Worldpay", "Fiserv", "FIS", "Jack Henry",
]

HEALTHCARE = [
    "Epic", "Cerner", "Athenahealth", "Allscripts", "Meditech", "NextGen",
    "HL7 FHIR", "DICOM", "Redox", "Health Gorilla",
]

LOGISTICS = [
    "Flexport", "Project44", "FourKites", "Convoy", "Uber Freight", "C.H. Robinson",
    "Maersk", "FedEx Tracking", "UPS Quantum View", "XPO Logistics",
]


def _slug(name: str) -> str:
    return name.lower().replace(" ", "_").replace("/", "_").replace(".", "").replace("-", "_")


def _entry(name: str | tuple[str, str], category: str, status: str, description: str) -> dict:
    # A (id, name) pair keeps a published id when its label changes.
    cid, name = name if isinstance(name, tuple) else (_slug(name), name)
    return {
        "id": cid,
        "name": name,
        "category": category,
        "status": status,
        "description": description,
    }


def generate() -> list[dict]:
    items: list[dict] = []
    seen: set[str] = set()

    def add(entry: dict) -> None:
        if entry["id"] in seen:
            return  # one tile per id: a repeated name is the same connector
        seen.add(entry["id"])
        items.append(entry)

    for cid, name, cat, desc in LIVE:
        add(_entry(name, cat, "live", desc))

    for cid, name, cat, desc in BETA:
        add(_entry(name, cat, "beta", desc))

    for name in DATABASES:
        label = name[1] if isinstance(name, tuple) else name
        add(_entry(name, "database", "planned", f"{label} source and destination connector"))

    for name in WAREHOUSES:
        add(_entry(name, "warehouse", "planned", f"{name} analytics warehouse connector"))

    for name in SAAS:
        add(_entry(name, "saas", "planned", f"{name} SaaS application sync"))

    for name in CLOUD_STORAGE:
        add(_entry(name, "cloud_storage", "planned", f"{name} object storage ingest and export"))

    for name in APIS:
        add(_entry(name, "api", "ai" if "OpenAPI" in name or "GraphQL" in name else "planned", f"{name} integration adapter"))

    for name in MARKETING:
        add(_entry(name, "marketing", "planned", f"{name} marketing analytics connector"))

    for name in FINANCE:
        add(_entry(name, "finance", "planned", f"{name} financial data connector"))

    for name in HEALTHCARE:
        add(_entry(name, "healthcare", "planned", f"{name} healthcare data connector"))

    for name in LOGISTICS:
        add(_entry(name, "logistics", "planned", f"{name} logistics and shipment data connector"))

    # Roadmap breadth: regional / industry variants (Planned, not certified)
    regions = ["US", "EU", "APAC", "UK", "CA", "AU", "JP", "DE", "FR", "IN"]
    industries = ["Retail", "Manufacturing", "Energy", "Telecom", "Insurance", "Banking", "Government"]
    base_saas = SAAS[:40]
    # The (base, region, industry) cycle repeats after its least common
    # multiple; past that every name is a duplicate, so stop there.
    for idx in range(len(base_saas) * len(industries)):
        if len(items) >= 620:
            break
        region = regions[idx % len(regions)]
        industry = industries[idx % len(industries)]
        base = base_saas[idx % len(base_saas)]
        name = f"{base} ({region} {industry})"
        add(_entry(name, "saas", "planned", f"Regional {industry.lower()} deployment for {base}"))

    return items


# ``<id>_<6 hex>``: the random suffix the old generator gave a colliding id.
_COLLISION_ID = re.compile(r"^(?P<base>.+)_[0-9a-f]{6}$")


def merge_catalog(existing: list[dict], generated: list[dict]) -> list[dict]:
    """Curated ``existing`` entries win; collision copies go; new tiles are added.

    A collision copy is a suffixed id whose name an earlier tile already has.
    A generated tile is added when neither its id nor its name is in the
    catalog, right after the nearest generated tile before it. A generated
    ``(id, name)`` pair relabels that id.
    """
    labels = {g["id"]: g["name"] for g in generated}
    out: list[dict] = []
    ids: set[str] = set()
    names: set[str] = set()
    for row in existing:
        name = str(row.get("name") or "").casefold()
        if row["id"] in ids or (_COLLISION_ID.match(row["id"]) and name in names):
            continue
        row = dict(row)
        if row["id"] in labels and labels[row["id"]] != row.get("name"):
            relabel = next(g for g in generated if g["id"] == row["id"])
            if _slug(relabel["name"]) != row["id"]:  # explicit (id, name) pair
                row["name"] = relabel["name"]
                row["description"] = relabel["description"]
        out.append(row)
        ids.add(row["id"])
        names.add(str(row.get("name") or "").casefold())
    previous = ""
    for entry in generated:
        if entry["id"] not in ids and entry["name"].casefold() not in names:
            at = next((i + 1 for i, r in enumerate(out) if r["id"] == previous), len(out))
            out.insert(at, dict(entry))
            ids.add(entry["id"])
            names.add(entry["name"].casefold())
        previous = entry["id"] if entry["id"] in ids else previous
    return out


def main() -> None:
    catalog = generate()
    if OUTPUT.is_file():
        existing = json.loads(OUTPUT.read_text(encoding="utf-8")).get("connectors") or []
        catalog = merge_catalog(existing, catalog)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps({"version": 1, "total": len(catalog), "connectors": catalog}, indent=2, default=json_default), encoding="utf-8")
    print(f"Generated {len(catalog)} connectors -> {OUTPUT}")


if __name__ == "__main__":
    main()
