# **GitHub API Documentation**

## **Authorization**

- **Chosen method**: Personal Access Token (PAT) for the GitHub REST API v3.
- **Base URL**: `https://api.github.com`
- **Auth placement**:
  - HTTP header: `Authorization: Bearer <personal_access_token>`
  - Recommended scopes for read-only analytics on repositories and issues:
    - Public repositories only: `public_repo`
    - Public + private repositories: `repo`
- **Other supported methods (not used by this connector)**:
  - OAuth apps and GitHub Apps are also supported by GitHub, but the connector will **not** perform interactive OAuth flows. Tokens must be provisioned out-of-band and stored in configuration.

Example authenticated request:

```bash
curl -X GET \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/<owner>/<repo>/issues?per_page=2"
```

Notes:
- `Accept: application/vnd.github+json` is recommended by GitHub for the REST API.
- Rate limiting for authenticated requests is typically **5,000 requests per hour per user/app**; unauthenticated requests are limited to **60/hour** (see official docs).


## **Object List**

For connector purposes, we treat specific GitHub REST resources as **objects/tables**.  
The object list is **static** (defined by the connector), not discovered dynamically from an API.

| Object Name | Description | Primary Endpoint | Ingestion Type (planned) |
|------------|-------------|------------------|--------------------------|
| `issues` | Issues (and PRs presented as issues) for a given repository | `GET /repos/{owner}/{repo}/issues` | `cdc` (upserts based on `updated_at`) |
| `repositories` | Metadata about a specific repository | `GET /repos/{owner}/{repo}` | `snapshot` |
| `pull_requests` | Pull requests for a given repository | `GET /repos/{owner}/{repo}/pulls` | `cdc` (planned; TBD details) |

**Connector scope for initial implementation**:
- Step 1 focuses on the `issues` object and documents it in detail.
- Other objects are listed for future extension and are marked `TBD` where details are not yet specified.


## **Object Schema**

### General notes

- GitHub provides a static JSON schema for resources via its REST API documentation and OpenAPI spec.
- For the connector, we define **tabular schemas** per object, derived from the JSON representation.
- Nested JSON objects (e.g., `user`, `labels`) are modeled as **nested structures/arrays** rather than being fully flattened.
- For now, we intentionally include a **subset of the GitHub issue fields** that are most relevant for analytics and incremental sync. Additional fields can be added later as needed.

### `issues` object (primary table)

**Source endpoint**:  
`GET /repos/{owner}/{repo}/issues`

**Key behavior**:
- Returns both issues and pull requests (PRs appear as issues with an additional `pull_request` object).
- Supports filtering and incremental reads via `state`, `since`, and pagination parameters.

**High-level schema (connector view)**:

Top-level fields (all from the GitHub REST API unless marked as “connector-derived”):

| Column Name | Type | Description |
|------------|------|-------------|
| `id` | integer (64-bit) | Unique identifier for the issue. Globally unique across GitHub. |
| `node_id` | string | GraphQL node ID for the issue. |
| `number` | integer | Issue number within the repository (unique per repo). |
| `repository_owner` | string (connector-derived) | Owner part of the `{owner}` path parameter for the repository whose issues are being read. |
| `repository_name` | string (connector-derived) | Repo name part of the `{repo}` path parameter for the repository whose issues are being read. |
| `title` | string | Issue title. |
| `body` | string or null | Markdown-formatted issue body. |
| `state` | string (enum) | Issue state: typically `open` or `closed`. |
| `locked` | boolean | Whether the issue is locked. |
| `comments` | integer | Number of comments on the issue. |
| `created_at` | string (ISO 8601 datetime) | Issue creation time. |
| `updated_at` | string (ISO 8601 datetime) | Last time the issue was updated. Used as incremental cursor. |
| `closed_at` | string (ISO 8601 datetime) or null | Time when the issue was closed (if closed). |
| `author_association` | string | How the author is associated with the repo (e.g., `OWNER`, `CONTRIBUTOR`). |
| `url` | string | API URL for the issue resource. |
| `html_url` | string | Web URL for the issue in the GitHub UI. |
| `labels_url` | string | Template URL for labels for this issue. |
| `comments_url` | string | API URL for comments for this issue. |
| `events_url` | string | API URL for events of this issue. |
| `timeline_url` | string | API URL for the timeline of this issue. |
| `state_reason` | string or null | Optional reason for the current state (e.g., `completed`, `not_planned`). |
| `user` | struct | User who created the issue (see nested schema). |
| `assignee` | struct or null | Single assignee, if any (see nested schema). |
| `assignees` | array\<struct\> | All assignees for the issue. |
| `labels` | array\<struct\> | Labels applied to the issue. |
| `milestone` | struct or null | Milestone associated with the issue, if any. |
| `pull_request` | struct or null | Present when the issue is a pull request. |
| `reactions` | struct or null | Aggregated reaction counts if requested with appropriate preview/accept header. |

**Nested `user` struct** (creator/owner of the issue):

| Field | Type | Description |
|-------|------|-------------|
| `login` | string | Username of the user. |
| `id` | integer (64-bit) | Unique user ID. |
| `node_id` | string | GraphQL node ID for the user. |
| `type` | string | Type of user (typically `User`, `Bot`, or `Organization`). |
| `site_admin` | boolean | Whether the user is a GitHub site admin. |

**Nested `label` struct** (elements of `labels` array):

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer (64-bit) | Unique label ID. |
| `node_id` | string | GraphQL node ID for the label. |
| `name` | string | Label name. |
| `color` | string | Hex color code for the label (without `#`). |
| `description` | string or null | Label description. |
| `default` | boolean | Whether this is a default label. |

**Nested `assignee` struct** (similar shape to `user`):

| Field | Type | Description |
|-------|------|-------------|
| `login` | string | Username of the assignee. |
| `id` | integer (64-bit) | Unique assignee ID. |
| `node_id` | string | GraphQL node ID. |
| `type` | string | Type of user. |
| `site_admin` | boolean | Whether the assignee is a site admin. |

**Nested `milestone` struct** (simplified subset):

| Field | Type | Description |
|-------|------|-------------|
| `id` | integer (64-bit) | Unique milestone ID. |
| `number` | integer | Milestone number. |
| `title` | string | Milestone title. |
| `description` | string or null | Milestone description. |
| `state` | string | Milestone state (`open` or `closed`). |
| `created_at` | string (ISO 8601 datetime) | Milestone creation time. |
| `updated_at` | string (ISO 8601 datetime) | Last update time. |
| `due_on` | string (ISO 8601 datetime) or null | Due date, if set. |

**Example request**:

```bash
curl -X GET \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/<owner>/<repo>/issues?state=all&per_page=2"
```

**Example response (truncated)**:

```json
[
  {
    "id": 1,
    "node_id": "MDU6SXNzdWUx",
    "number": 1347,
    "title": "Found a bug",
    "user": {
      "login": "octocat",
      "id": 1,
      "node_id": "MDQ6VXNlcjE=",
      "type": "User",
      "site_admin": false
    },
    "state": "open",
    "locked": false,
    "comments": 0,
    "created_at": "2011-04-22T13:33:48Z",
    "updated_at": "2011-04-22T13:33:48Z",
    "closed_at": null,
    "body": "I'm having a problem with this.",
    "labels": [],
    "assignee": null,
    "assignees": [],
    "milestone": null,
    "url": "https://api.github.com/repos/octocat/Hello-World/issues/1347",
    "html_url": "https://github.com/octocat/Hello-World/issues/1347",
    "comments_url": "https://api.github.com/repos/octocat/Hello-World/issues/1347/comments",
    "events_url": "https://api.github.com/repos/octocat/Hello-World/issues/1347/events",
    "timeline_url": "https://api.github.com/repos/octocat/Hello-World/issues/1347/timeline"
  }
]
```

> The connector’s table schema is intentionally a **subset** of the full GitHub issue object. Additional fields (e.g., `active_lock_reason`, full `reactions` details) can be added in later iterations as needed.


## **Get Object Primary Key**

There is no dedicated metadata endpoint to get the primary key for the `issues` object.  
Instead, the primary key is defined **statically** based on the resource schema.

- **Primary key for `issues`**: `id`
  - Type: 64-bit integer
  - Property: Unique across all issues on GitHub (not just within a repository).

The connector will:
- Read the `id` field from each issue record returned by `GET /repos/{owner}/{repo}/issues`.
- Use it as the immutable primary key for upserts when ingestion type is `cdc`.

Example showing primary key in response:

```json
{
  "id": 1,
  "number": 1347,
  "title": "Found a bug",
  "state": "open",
  "created_at": "2011-04-22T13:33:48Z",
  "updated_at": "2011-04-22T13:33:48Z"
}
```


## **Object's ingestion type**

Supported ingestion types (framework-level definitions):
- `cdc`: Change data capture; supports upserts and/or deletes incrementally.
- `snapshot`: Full replacement snapshot; no inherent incremental support.
- `append`: Incremental but append-only (no updates/deletes).

Planned ingestion types for GitHub objects:

| Object | Ingestion Type | Rationale |
|--------|----------------|-----------|
| `issues` | `cdc` | Issues have a stable primary key `id` and an `updated_at` field that can be used as a cursor for incremental syncs. Issues are not truly deleted in most workflows; updates and closures can be modeled as upserts. |
| `repositories` | `snapshot` | Repository metadata is relatively small and can be re-snapshotted. No dedicated incremental cursor is required for initial implementation. |
| `pull_requests` | `cdc` (TBD) | PRs expose similar fields to issues, including `updated_at`. Detailed ingestion strategy will mirror `issues` but is out of scope for this initial doc. |

For `issues`:
- **Primary key**: `id`
- **Cursor field**: `updated_at`
- **Sort order**: Ascending by `updated_at`, then `id` as tie-breaker (implementation detail).
- **Deletes**: GitHub issues are typically closed but not hard-deleted; the connector treats changes in `state` and `closed_at` as updates (upserts), not deletes.


## **Read API for Data Retrieval**

### Primary read endpoint for `issues`

- **HTTP method**: `GET`
- **Endpoint**: `/repos/{owner}/{repo}/issues`
- **Base URL**: `https://api.github.com`

**Path parameters**:
- `owner` (string, required): Repository owner (user or organization).
- `repo` (string, required): Repository name.

**Key query parameters** (subset relevant for ingestion):

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `state` | string | no | `open` | Filter by state: `open`, `closed`, or `all`. For full history, use `all`. |
| `since` | string (ISO 8601 datetime) | no | none | Return issues updated at or after this time. Used for incremental reads. |
| `per_page` | integer | no | 30 | Number of results per page (max 100). |
| `page` | integer | no | 1 | Page number of the results to fetch. |
| `sort` | string | no | `created` | The field to sort by (e.g., `created`, `updated`, `comments`). |
| `direction` | string | no | `desc` | Sort direction: `asc` or `desc`. |
| `labels` | string | no | none | Comma-separated list of label names. If provided, only issues with all these labels are returned. |

**Pagination strategy**:
- GitHub uses traditional page-based pagination with `per_page` and `page` parameters.
- Pagination links are provided via the `Link` response header (RFC 5988 style) with `rel="next"`, `rel="prev"`, `rel="first"`, and `rel="last"`.
- The connector should:
  - Request with `per_page=100` (maximum) for efficiency.
  - Follow the `Link` header `rel="next"` URL until it is no longer present.

Example incremental read using `since`:

```bash
SINCE_TS="2024-01-01T00:00:00Z"
curl -X GET \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/<owner>/<repo>/issues?state=all&since=${SINCE_TS}&per_page=100"
```

**Incremental strategy**:
- On the first run, the connector can:
  - Either perform a full historical backfill (no `since`), or
  - Use a configurable `start_date` as the initial `since` cursor.
- On subsequent runs:
  - Use the maximum `updated_at` value (plus a small lookback window, e.g., a few minutes) from the previous sync as the new `since` parameter.
  - Sort by `updated` (or rely on GitHub’s default) and reprocess overlapping records to handle late-arriving updates.

**Handling deletes / closed issues**:
- GitHub does not commonly hard-delete issues; instead, issues are closed.
- The connector should:
  - Treat changes in `state` (e.g., from `open` to `closed`) and `closed_at` as **updates**, not deletes.
  - If a downstream system needs to interpret “closed” as a type of soft delete, this can be derived from the `state` and `closed_at` fields.

**Alternative APIs**:
- For more complex analytics, related endpoints (e.g., comments, events, timeline) can be used:
  - `GET /repos/{owner}/{repo}/issues/{issue_number}/comments`
  - `GET /repos/{owner}/{repo}/issues/{issue_number}/events`
  - `GET /repos/{owner}/{repo}/issues/{issue_number}/timeline`
- Those are out of scope for the initial connector’s primary `issues` table.


## **Field Type Mapping**

### General mapping (GitHub JSON → connector logical types)

| GitHub JSON Type | Example Fields | Connector Logical Type | Notes |
|------------------|----------------|------------------------|-------|
| integer (32/64-bit) | `id`, `number`, `comments` | `long` / integer | For Spark-based connectors, prefer 64-bit integer (`LongType`). |
| string | `title`, `body`, `state`, `author_association` | string | UTF-8 text. Long markdown bodies should be supported. |
| boolean | `locked`, `site_admin`, `default` | boolean | Standard true/false. |
| string (ISO 8601 datetime) | `created_at`, `updated_at`, `closed_at`, `due_on` | timestamp with timezone | Stored as UTC timestamps; parsing must respect timezone `Z`. |
| object | `user`, `assignee`, `milestone`, `pull_request` | struct | Represented as nested records instead of flattened columns. |
| array | `labels`, `assignees` | array\<struct\> | Arrays of nested objects. |
| nullable fields | `body`, `closed_at`, `milestone`, `assignee` | corresponding type + null | When fields are absent or null, the connector should surface `null`, not `{}`. |

### Special behaviors and constraints

- `id` and other numeric identifiers should be stored as **64-bit integers** to avoid overflow.
- `state` and `author_association` are effectively enums, but are represented as strings; connector may choose to preserve as strings for flexibility.
- Timestamp fields use ISO 8601 in UTC (e.g., `"2011-04-22T13:33:48Z"`); parsing must be robust to this format.
- Nested structs (`user`, `labels`, etc.) should not be flattened in the connector implementation; instead, they should be represented as nested types.


## **Write API**

The initial connector implementation is primarily **read-only**. However, for completeness, the GitHub REST API supports write operations relevant to the `issues` object.

### Create an issue

- **HTTP method**: `POST`
- **Endpoint**: `/repos/{owner}/{repo}/issues`

**Required scope**:
- For public repositories: `public_repo`
- For private repositories: `repo`

**Request body (JSON)**:

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `title` | string | yes | Title of the issue. |
| `body` | string | no | Body content of the issue. |
| `assignees` | array\<string\> | no | Usernames to assign to the issue. |
| `labels` | array\<string\> | no | Label names to apply. |
| `milestone` | integer | no | Milestone number to associate. |

Example request:

```bash
curl -X POST \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  -H "Accept: application/vnd.github+json" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "Found a bug",
    "body": "I am having a problem with this.",
    "assignees": ["octocat"],
    "labels": ["bug"]
  }' \
  "https://api.github.com/repos/<owner>/<repo>/issues"
```

### Update an issue

- **HTTP method**: `PATCH`
- **Endpoint**: `/repos/{owner}/{repo}/issues/{issue_number}`

**Commonly updated fields**:
- `title`
- `body`
- `state` (`open` or `closed`)
- `assignees`
- `labels`

Example request to close an issue:

```bash
curl -X PATCH \
  -H "Authorization: Bearer <PERSONAL_ACCESS_TOKEN>" \
  -H "Accept: application/vnd.github+json" \
  -H "Content-Type: application/json" \
  -d '{"state": "closed"}' \
  "https://api.github.com/repos/<owner>/<repo>/issues/1347"
```

**Validation / read-after-write**:
- The connector (or user) can validate writes by:
  - Reading back the updated issue via `GET /repos/{owner}/{repo}/issues/{issue_number}`, or
  - Letting the next incremental read (using `since` and `updated_at`) pick up the change.


## **Known Quirks & Edge Cases**

- **Issues vs pull requests**:
  - The `issues` endpoint returns both issues and pull requests; pull requests are distinguished by a non-null `pull_request` object.
  - Depending on downstream modeling, PRs may be filtered out or kept in the same table with an indicator.
- **Missing or partial fields**:
  - Some fields (e.g., `reactions`) may require specific preview headers or may be absent for older data.
  - The connector should treat missing nested objects as `null`, not empty dicts, to align with downstream schema expectations.
- **Authentication methods**:
  - Although GitHub supports OAuth apps and GitHub Apps, the connector will **only** support PAT-based auth to keep configuration simple and avoid managing OAuth flows.


## **Research Log**

| Source Type | URL | Accessed (UTC) | Confidence | What it confirmed |
|------------|-----|----------------|------------|-------------------|
| Official Docs | https://docs.github.com/en/rest/issues/issues | 2025-11-14 | High | `GET /repos/{owner}/{repo}/issues` endpoint behavior, parameters, example response, and issue fields. |
| Official Docs | https://docs.github.com/en/rest/using-the-rest-api/authenticating-with-github-api | 2025-11-14 | High | Personal access token authentication method, header format, and rate limit behavior. |
| Official Docs | https://docs.github.com/en/rest/overview/resources-in-the-rest-api#rate-limiting | 2025-11-14 | High | Authenticated vs unauthenticated rate limits. |
| Official Docs | https://docs.github.com/en/rest/repos/repos#get-a-repository | 2025-11-14 | High | Repository object and justification for treating `repositories` as snapshot. |
| Official Docs | https://docs.github.com/en/rest/issues/issues#create-an-issue | 2025-11-14 | High | Write API for creating issues, required fields, and scopes. |
| Official Docs | https://docs.github.com/en/rest/issues/issues#update-an-issue | 2025-11-14 | High | Write API for updating issues and common mutable fields. |
| OSS Connector Docs | https://docs.airbyte.com/integrations/sources/github | 2025-11-14 | High | Airbyte GitHub source connector behavior, use of `updated_at` as cursor, and supported streams. |
| OSS Connector Code | https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-github | 2025-11-14 | High | Reference implementation details for incremental sync, pagination handling via `Link` headers, and object list. |


## **Sources and References**

- **Official GitHub REST API documentation** (highest confidence)
  - `https://docs.github.com/en/rest`
  - `https://docs.github.com/en/rest/issues/issues`
  - `https://docs.github.com/en/rest/using-the-rest-api/authenticating-with-github-api`
  - `https://docs.github.com/en/rest/overview/resources-in-the-rest-api#rate-limiting`
  - `https://docs.github.com/en/rest/issues/issues#create-an-issue`
  - `https://docs.github.com/en/rest/issues/issues#update-an-issue`
- **Airbyte GitHub source connector documentation and implementation** (high confidence)
  - `https://docs.airbyte.com/integrations/sources/github`
  - `https://github.com/airbytehq/airbyte/tree/master/airbyte-integrations/connectors/source-github`

When conflicts arise, **official GitHub documentation** is treated as the source of truth, with the Airbyte connector used to validate practical details like pagination and cursor fields.


