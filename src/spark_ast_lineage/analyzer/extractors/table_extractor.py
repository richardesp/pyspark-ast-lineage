import ast


class TableExtractor:
    """Extractor for `spark.read.table("table_name")` calls"""

    def extract(self, node):
        """
        Extracts table names from `spark.read.table("table_name")` calls.

        Args:
            node (ast.Call): The AST node representing a function call.

        Returns:
            set: A set of extracted table names.
        """
        table_name = (
            node.args[0].value if isinstance(node.args[0], ast.Constant) else None
        )
        return {table_name} if table_name else set()
