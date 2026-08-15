export interface ConnectorSetupGuide {
  title: string;
  steps: string[];
}

const GENERIC: ConnectorSetupGuide = {
  title: "Set up this connection",
  steps: [
    "Enter the host and credentials your admin issued.",
    "Leave optional fields blank unless you were given a specific value.",
    "Test before you save. Connected means the driver reached the system.",
  ],
};

const GUIDES: Record<string, ConnectorSetupGuide> = {
  snowflake: {
    title: "Set up Snowflake",
    steps: [
      "Pick the Snowflake tile. AWS, Azure, GCP, Standard, and Enterprise are the same login — not different products.",
      "In Snowsight, open the account menu and copy the account identifier (org-account, for example myorg-acctname).",
      "Paste that in Account host. A browser URL is the host, not a login. Locator-only hosts can return HTTP 404.",
      "Leave Role blank unless you know a role this user can assume.",
      "Test. A 250001 means the host was reached — check username and password. If Snowsight uses MFA, switch to Programmatic access token.",
    ],
  },
  excel: {
    title: "Set up Excel",
    steps: [
      "Pick Excel for a workbook file — not Snowflake.",
      "Provide a .xlsx path or object-store URI. Prefer .xlsx over legacy .xls.",
      "Test proves the file is readable. A missing path is not Connected.",
    ],
  },
  sftp: {
    title: "Set up SFTP",
    steps: [
      "Pick SFTP for a remote file host — not Snowflake.",
      "Enter host, user, and the remote path to the CSV, Excel, or Parquet file.",
      "Test before you save. Then use Transfer to write into Postgres or Snowflake.",
    ],
  },
};

export function getConnectorSetupGuide(type: string): ConnectorSetupGuide {
  const id = (type || "").toLowerCase().trim();
  return GUIDES[id] || GENERIC;
}
