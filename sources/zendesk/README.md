# Lakeflow Zendesk Connector

The Lakeflow Zendesk Connector allows you to extract data from your Zendesk instance and load it into your data lake or warehouse. This connector supports both incremental and full refresh synchronization patterns for various Zendesk objects.

## Set up

### Prerequisites
- Access to a Zendesk instance with API permissions
- Zendesk API token with appropriate read permissions
- Admin or agent role in Zendesk (depending on the data you want to access)

### Required Parameters

To configure the Zendesk connector, you'll need to provide the following parameters in your connector options:

| Parameter | Type | Required | Description | Example |
|-----------|------|----------|-------------|---------|
| `subdomain` | string | Yes | Your Zendesk subdomain (the part before .zendesk.com) | `mycompany` |
| `email` | string | Yes | Email address of the Zendesk user with API access | `admin@mycompany.com` |
| `api_token` | string | Yes | Zendesk API token for authentication | `abc123def456ghi789` |

### Getting Your API Token

1. Log in to your Zendesk instance as an admin
2. Navigate to Admin Center → Apps and integrations → APIs → Zendesk API
3. Enable token access if not already enabled
4. Click the "+" button to create a new API token
5. Copy the generated token (you won't be able to see it again)

### Create a UC connection or Secret
UC connection is currently not supported yet.

Create a secret that stores the parameters needed, using Databricks CLI tool

```
databricks secrets create-scope <connection_name>
databricks secrets put-secret <connection_name> subdomain
databricks secrets put-secret <connection_name> email
databricks secrets put-secret <connection_name> api_token 
```


## Objects Supported

The Zendesk connector supports the following objects with their respective schemas, primary keys, and incremental strategies:

### Tickets
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: Complete ticket information including subject, description, status, priority, assignee, requester, custom fields, and metadata
- **Key Fields**: `id`, `subject`, `description`, `status`, `priority`, `requester_id`, `assignee_id`, `organization_id`, `created_at`, `updated_at`

### Organizations  
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: Organization details including name, domain names, group associations, and custom fields
- **Key Fields**: `id`, `name`, `domain_names`, `group_id`, `created_at`, `updated_at`

### Users
- **Primary Key**: `id` 
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: User profile information including contact details, roles, permissions, and custom fields
- **Key Fields**: `id`, `email`, `name`, `role`, `organization_id`, `active`, `created_at`, `updated_at`

### Ticket Comments
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: Comments and internal notes on tickets with author and timestamp information
- **Key Fields**: `id`, `ticket_id`, `type`, `body`, `author_id`, `created_at`, `updated_at`

### Articles (Help Center)
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: Knowledge base articles including content, metadata, and publishing information
- **Key Fields**: `id`, `title`, `body`, `section_id`, `author_id`, `created_at`, `updated_at`

### Groups
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: Agent groups for ticket assignment and organization
- **Key Fields**: `id`, `name`, `description`, `default`, `created_at`, `updated_at`

### Brands
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`  
- **Schema**: Brand configuration for multi-brand Zendesk instances
- **Key Fields**: `id`, `name`, `brand_url`, `subdomain`, `active`, `created_at`, `updated_at`

### Topics (Community)
- **Primary Key**: `id`
- **Incremental Strategy**: Cursor-based on `updated_at`
- **Schema**: Community forum topics and discussions
- **Key Fields**: `id`, `name`, `description`, `community_id`, `created_at`, `updated_at`

## How to Run

### Step 1: Clone/Copy the Source Connector Code
Copy the code from git master and paste in the connector template notebook.

### Step 2: Configure Your Pipeline
1. Update the connection_name in the notebook
2. Update the objects to ingest in the notebook
3.  (Optional) customize the source connector code if needed for special use case.


### Step 3: Run and Schedule the LDP Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a subset of objects to test your pipeline
- **Monitor API Limits**: Zendesk has rate limits (200 requests per minute for most endpoints)
- **Use Incremental Sync**: Reduces API calls and improves performance
- **Set Appropriate Schedules**: Balance data freshness needs with API usage
- **Test Thoroughly**: Validate data accuracy and completeness after initial setup

#### Troubleshooting

**Common Issues:**
- **Authentication Errors**: Verify API token and email are correct
- **Rate Limiting**: Reduce sync frequency or implement exponential backoff
- **Missing Data**: Check user permissions and API access levels
- **Schema Changes**: Update connector code if Zendesk adds new fields

**Error Handling:**
The connector includes built-in error handling for common scenarios like rate limiting, network issues, and API errors. Check the pipeline logs for detailed error information and recommended actions.