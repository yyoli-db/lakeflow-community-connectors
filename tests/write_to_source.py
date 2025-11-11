from abc import ABC, abstractmethod
from typing import Tuple, List, Dict


class WriteToSource(ABC):
    """
    Trait that provides functionality for generating rows and writing to a data source.
    Classes implementing this trait must override the generate_rows_and_write and
    list_insertable_tables methods.
    """

    @abstractmethod
    def generate_rows_and_write(
        self, table_name: str, number_of_rows: int
    ) -> Tuple[bool, List[Dict], Dict[str, str]]:
        """
        Generate specified number of rows and write them to the given table.
        DISCLAIMER: Currently the function only supports testing of inserts of rows, not deletes or updates.

        Args:
            table_name: Name of the table to write to
            number_of_rows: Number of rows to generate and write

        Returns:
            Tuple containing:
            - Boolean indicating success of the operation
            - List of rows as dictionaries (the written data)
            - Dictionary mapping written column names to returned column names
        """
        pass

    @abstractmethod
    def list_insertable_tables(self) -> List[str]:
        """
        List all tables that support insert/write-back functionality.

        Returns:
            List of table names that support inserting new data
        """
        pass
