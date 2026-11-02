# Jira Cloud synthetic fixtures

These fixtures are synthetic, modelled on the Jira Cloud REST API documentation,
and not recorded from a live tenant:

- https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/
- https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-projects/

They cover issue search `nextPageToken` pagination, minute-granularity JQL
incremental filters, and project search `startAt`/`maxResults` pagination with
the documented `isLast` flag.
