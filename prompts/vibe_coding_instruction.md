# Connector Vibe-Coding Prompts

## Step 1: Understanding Source Document

### Prompts
Create a markdown document to summarize the source API based on the provided template.

**REQUIRED RESEARCH STEPS (must be completed BEFORE generating documentation):**
1. **Check for user-provided documentation** - If the user provides API documentation, use that as the highest confidence source
2. **Search official API documentation** - Use WebSearch/WebFetch to find and review the official API documentation
3. **Find existing implementations** - Search for existing connector implementations (Airbyte OSS(high confidence), Singer, dlthub, etc.) using WebSearch
4. **Verify current API endpoints and schemas** - Cross-reference multiple sources to ensure accuracy
5. **Document sources used** - Include a "Sources" section at the end listing all URLs and references used

**Documentation Requirements:**
- Focus on endpoints, authentication parameters, object schemas, Python API for reading data, and incremental read strategy
- Please include all fields in the schema. DO NOT hide any fields using links
- All information must be backed by official sources or verified or reliable implementations
- If conflicting information is found, prioritize official API documentation

### Notes
- **NEVER generate API documentation from memory alone** - always research first
- Analyze existing implementations (e.g., Airbyte OSS) to fill in the source API details
- Use the `source_api_doc_template.md`
- If multiple authentication methods are available, select the preferred method and remove the others from the generated documentation.
- For OAuth, the connector implementation should not handle the entire OAuth flow. Instead, store credentials such as client_id, client_secret, and refresh_token, so the connector code can use them to obtain a temporary token for accessing the source system.

### Recommendations
- Try to get the connector working end-to-end while just ingesting a single table to begin with. This will help identify gaps in the authentication and any other setup issues.
- Once the single table ingestion is successful, proceed to implement the full connector incrementally while keeping on testing the new features(eg:- incremental sync, deletes support) implemented.

## Step 2: Create a UC Connection

**Note:** This is a manual step

### Option 1: Use Secret Service
Use a secret scope to mimic a UC connection

```bash
databricks secrets create-scope <connection_name>
databricks secrets put-secret <connection_name> <param_name>
databricks secrets put-secret <connection_name> <param_name>
```

### Option 2: Create UC Connection via UI or API
> **Status:** Not ready yet

## Step 3: Generate the Connector Code in Python

### Prompts
Implement the interface defined in `@lakeflow_connect.py` based on the source API documentation <link the doc summary>

**Requirements:**
- Implement all functions
- When returning the schema in the get_table_details function, prefer using StructType over MapType to enforce explicit typing of sub-columns. - - Avoid flattening nested fields when parsing JSON data.
- Prefer using `LongType` over `IntegerType`
- In logic of processing records, if a StructType field is absent in the response, assign None as the default value instead of an empty dictionary {}.
- Please avoid creating mock objects in the implementation.
- Please do not add an extra main function - only implement the defined functions within the LakeflowConnect class. 
- You can refer to `example/example.py` or other connectors under `connector_sources` as examples

## Step 4: Run Test and Fix

### Prompts 
#### If using IDE like cursor
- Run the test defined in `test/test_suite.py`
- Please use this option <fill in the parameters needed to connect to the testing instance> to initialize for testing
- Generate code to write to the source system based on the source API
- Run the test to validate

#### If using chatbot and need to run notebook
- Import `test/run_test_notebook.py` and update the second cell with the `connection_name` you created in step 2.
- Run the notebook.
- If you encounter any errors, you can provide them to the chatbot to help debug and fix the generated code.


### Notes
This step is more interactive. Based on testing results, we need to make various adjustments

## Step 5: Create a Public Connector Documentation

### Prompts
Create public documentation for the users of the connector based on the template.

**Requirements:**
- Please use the code implementation as the source of truth
- Use the source API documentation to cover anything missing
- Always include a section about how to configure the parameters needed to connect to the source system

### Notes
Use `lakeflow_connector_doc_template.md`

### Step 6: Test pipeline end to end

**This step is manual**
- Create a new notebook and import `lakeflow_connect_notebook.py`.
- In the first cell, add the implementation for your specific source connector.
- In the second cell, set the connection_name and specify the list of objects you want to ingest.
- Create and run an LDP pipeline using this notebook.
- You can follow the detailed steps provided in the public connector documentation generated in step 5.
