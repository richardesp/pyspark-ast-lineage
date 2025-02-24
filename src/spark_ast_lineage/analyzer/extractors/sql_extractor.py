import ast
import sqlparse


class SQLExtractor:
    """Extractor for `spark.sql()` calls"""

    def extract(self, node):
        """
        Extracts table names from SQL queries.

        Args:
            node (ast.Call): The AST node representing a function call.

        Returns:
            set: A set of extracted table names.
        """
        sql_query = (
            node.args[0].value if isinstance(node.args[0], ast.Constant) else None
        )
        return self._extract_tables_from_sql(sql_query) if sql_query else set()

    @staticmethod
    def _extract_tables_from_sql(sql_query):
        """Uses sqlparse to extract table names from SQL queries"""
        tables = set()
        parsed = sqlparse.parse(sql_query)
        for stmt in parsed:
            for token in stmt.tokens:
                if isinstance(token, sqlparse.sql.IdentifierList):
                    for identifier in token.get_identifiers():
                        tables.add(identifier.get_real_name())
                elif isinstance(token, sqlparse.sql.Identifier):
                    tables.add(token.get_real_name())
        return tables
