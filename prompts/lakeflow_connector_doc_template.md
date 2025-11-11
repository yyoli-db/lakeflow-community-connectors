Use this template to generate public documentation for the {{ source name }} connector.
The output should consist only of Markdown, as shown below.

# Lakeflow {{ source name }} Connector

This documentation provides setup instructions and reference information for the {{ source name }} source connector.

## Prerequisites
<Please list the prerequisites to use the connector>
<Example: An account with admin permissions, read permissions, etc.>
<Example: API token, or username/password for the account>

## Setup

### Required Parameters for Connection

To configure the connector, you'll need to provide the following parameters in your connector options:
<list the parameters with name, type, required or not, description and example in a table>

### <Add section to describe how to obtain the required parameters>

### Create a UC Connection 

Run the Databricks CLI tool to create a connection.

```
databricks secrets create-scope <connection_name>
databricks secrets put-secret <connection_name> subdomain
databricks secrets put-secret <connection_name> email
databricks secrets put-secret <connection_name> api_token 
```


## Objects Supported
<Describe what objects are supported.>
<If only a static list of objects is supported, please list all of them. Otherwise, describe the supported objects in categories or as a whole>
<Describe the primary key of the object if available>
<Describe the incremental strategy for ingesting the object, and cursor if it can be incrementally read>
<No need to describe the full schema, but list columns that are special and need to be highlighted if any.>


## Data Type Mapping
<Describe the type mapping between the source system and ingested table in Databricks in a table.>


## How to Run

### Step 1: Clone/Copy the Source Connector Code
#### Import the notebook directly
- Import the `lakeflow_connect_notebook.py` notebook.
- In the first cell, add your specific source connector implementation.
- In the second cell, specify the connection name and the list of objects you want to ingest.

#### Alternatively, copy the latest files individually
Copy the code from the latest version in git and paste it into the connector template notebook as follows:
- 1st cell: the generated connector code
- 2nd cell: `ingestion.py` (be sure to update your connection name and object list)
- 3rd cell: `utils.py`
- 4th cell: `lakeflow_python_source.py`
- 5th cell: `ingestion_pipeline.py`

### Step 2: Configure Your Pipeline
1. Update the connection_name in the 2nd cell of the notebook
2. Update the objects to ingest in the 2nd cell of the notebook
3. (Optional) Customize the source connector code if needed for special use cases.


### Step 3: Run and Schedule the LDP Pipeline

#### Best Practices

- **Start Small**: Begin by syncing a subset of objects to test your pipeline
- **Use Incremental Sync**: Reduces API calls and improves performance
- **Set Appropriate Schedules**: Balance data freshness needs with API usage
- <Add other considerations like API limits when they apply to the source>

#### Troubleshooting

**Common Issues:**
<list common issues if any. Examples: authentication error, rate limit, missing data>


## Reference
<list documentation references>