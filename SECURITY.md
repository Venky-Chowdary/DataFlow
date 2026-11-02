# Security policy

## Reporting a vulnerability

Email **contact@datawrap.io** with the subject line `Security report`.
Please do not open a public GitHub issue for a suspected vulnerability.

Include what you can:

- the affected surface (web app, API endpoint, connector, MCP server, CLI)
- steps to reproduce, or a proof of concept
- the impact you observed, and the account or workspace you tested with

## What to expect

| Step | Target |
|---|---|
| Acknowledge your report | 3 business days |
| Initial assessment and severity | 10 business days |
| Status updates while we fix | at least every 14 days |
| Fix for critical / high findings | as fast as practical; we coordinate disclosure timing with you |

We will credit you in the release notes if you want, once the fix ships.

## Safe harbor

We will not pursue legal action for good-faith research that:

- tests only accounts and workspaces you own or are authorized to use
- does not access, change, or keep other customers' data — stop and report if you reach any
- does not degrade the service (no denial-of-service, spam, or volumetric testing)
- does not use social engineering or physical attacks against staff or offices
- gives us reasonable time to fix before any public disclosure

## Scope

In scope: the Datawrap web app, the `/api/v1` API, connectors, the MCP server,
and this repository's code.

Out of scope: third-party services we use (report those to the vendor),
findings that need a compromised device or browser, missing best-practice
headers without a demonstrated impact, and rate-limit or volumetric findings.

## Supported versions

Only the current production release receives security fixes.

Machine-readable contact details: [`/.well-known/security.txt`](apps/web/public/.well-known/security.txt) (RFC 9116).
