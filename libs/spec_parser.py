from typing import List, Dict, Any, Optional


class SpecParser:
    """
    Parser for ingestion pipeline specifications.
    
    Parses a JSON specification containing connection information and table configurations.
    """
    
    def __init__(self, spec: Dict[str, Any]):
        """
        Initialize the spec parser with a pipeline specification.
        
        Args:
            spec: A dictionary containing the pipeline specification with the following structure:
                - connection_name: The name of the connection
                - objects: A list of table objects to be ingested
        
        Example:
            spec = {
                "connection_name": "my_connection",
                "objects": [
                    {
                        "table": {
                            "source_table": "t_1",
                        }
                    }
                ]
            }
        """
        if not isinstance(spec, dict):
            raise ValueError("Spec must be a dictionary")
        
        self._spec = spec
        self._validate_spec()
    
    def _validate_spec(self):
        """
        Validate the spec structure to ensure required fields are present.
        """
        if "connection_name" not in self._spec:
            raise ValueError("Spec must contain 'connection_name' field")
        
        if "objects" not in self._spec:
            raise ValueError("Spec must contain 'objects' field")
        
        if not isinstance(self._spec["objects"], list):
            raise ValueError("'objects' field must be a list")
    
    def connection_name(self) -> str:
        """
        Return the connection name from the specification.
        
        Returns:
            The connection name as a string
        """
        return self._spec["connection_name"]
    
    def get_tables(self) -> List[Dict[str, Any]]:
        """
        Return a list of tables from the specification.
        
        Each table in the list contains:
            - source_table: The source table name
            - table_config: Optional configuration for the table (e.g., SCD type)
        
        Returns:
            A list of table dictionaries
        """
        tables = []
        
        for obj in self._spec["objects"]:
            if "table" not in obj:
                raise ValueError("Each object must contain a 'table' field")
            
            table_info = obj["table"]
            
            # Extract required fields
            if "source_table" not in table_info:
                raise ValueError("Table must contain 'source_table' field")
            
            # Build the table entry
            table_entry = {
                "source_table": table_info["source_table"],
            }
            
            # Add optional table_config if present
            if "table_config" in table_info:
                table_entry["table_config"] = table_info["table_config"]
            
            tables.append(table_entry)
        
        return tables

